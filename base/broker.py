import os
import sys
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

# pylint: disable=import-error, wrong-import-position

from protocolo import notificar_monitor

from constantes import (
    TipoMensagem, EstadoDrone, StatusRequisicao, Criticidade,
    HEARTBEAT_INTERVALO_S, HEARTBEAT_MAX_FALHAS
)
from protocolo import (
    criar_servidor_tcp, criar_servidor_udp,
    tcp_receber_completo, tcp_broadcast, tcp_enviar, BUFFER_UDP,
)
from lamport import LamportClock

from fila_replicada import FilaReplicada, EntradaFila
from prioridade import GerenciadorPrioridade

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("base")

# ── Configuração via ambiente ─────────────────────────────────────────────────
BASE_ID         = os.environ.get("BASE_ID", "NORTE")
MINHA_PORTA     = int(os.environ.get("MINHA_PORTA", "6001"))
MINHA_PORTA_UDP = int(os.environ.get("MINHA_PORTA_UDP", "6101"))
IP_PC_DRONES    = os.environ.get("IP_PC_DRONES", "127.0.0.1")

TODAS_AS_BASES = {
    "NORTE": (os.environ.get("IP_BASE_NORTE", "127.0.0.1"), int(os.environ.get("PORTA_BASE_NORTE", "6001"))),
    "SUL":   (os.environ.get("IP_BASE_SUL",   "127.0.0.1"), int(os.environ.get("PORTA_BASE_SUL",   "6002"))),
    "LESTE": (os.environ.get("IP_BASE_LESTE", "127.0.0.1"), int(os.environ.get("PORTA_BASE_LESTE", "6003"))),
    "OESTE": (os.environ.get("IP_BASE_OESTE", "127.0.0.1"), int(os.environ.get("PORTA_BASE_OESTE", "6004"))),
}
OUTRAS_BASES = [(host, porta) for base_id, (host, porta) in TODAS_AS_BASES.items() if base_id != BASE_ID]

HEARTBEAT_TIMEOUT_S = float(os.environ.get("HEARTBEAT_TIMEOUT", str(HEARTBEAT_INTERVALO_S * (HEARTBEAT_MAX_FALHAS + 1))))

# ── Instâncias Globais Modulares ──────────────────────────────────────────────
clock = LamportClock()
estado = FilaReplicada()
prioridade = GerenciadorPrioridade(BASE_ID)

_timers: dict[str, threading.Timer] = {}
_timers_lock = threading.Lock()


# ── Lógica Principal ──────────────────────────────────────────────────────────

def _tentar_aceitar(id_requisicao: str):
    with _timers_lock:
        _timers.pop(id_requisicao, None)

    status = estado.status_requisicao(id_requisicao)
    if status != StatusRequisicao.PENDENTE.value:
        logger.debug("[%s] Req %s já está '%s'.", BASE_ID, id_requisicao[:8], status)
        return

    drone = estado.drone_livre()
    if drone is None:
        logger.info("[%s] Sem drone livre para req %s — silêncio.", BASE_ID, id_requisicao[:8])
        return

    if not estado.marcar_aceita(id_requisicao):
        return

    estado.ocupar_drone(drone.drone_id, id_requisicao)
    logger.info("[%s] ✔ Aceitando req %s → drone %s", BASE_ID, id_requisicao[:8], drone.drone_id)

    aceite = {
        "tipo": TipoMensagem.ACEITE.value,
        "id_requisicao": id_requisicao,
        "base_id": BASE_ID,
        "drone_id": drone.drone_id,
        "timestamp_logico": clock.incrementar(),
    }
    tcp_broadcast(OUTRAS_BASES, aceite)
    _despachar_drone(drone, id_requisicao)

def _despachar_drone(drone, id_requisicao: str):
    entrada = estado.obter_entrada(id_requisicao)
    if not entrada: return

    missao = {
        "tipo": "MISSAO",
        "id_requisicao": entrada.id_requisicao,
        "setor_id": entrada.id_setor,
        "timestamp_logico": entrada.timestamp_logico,
        "criticidade": entrada.criticidade,
        "tipo_ocorrencia": entrada.tipo_ocorrencia,
        "base_origem": BASE_ID,
    }

    if tcp_enviar(IP_PC_DRONES, drone.porta_tcp, missao):
        logger.info("[%s] Drone %s despachado para req %s", BASE_ID, drone.drone_id, id_requisicao[:8])
    else:
        logger.warning("[%s] Falha ao contatar drone %s.", BASE_ID, drone.drone_id)
        _tratar_drone_perdido(drone.drone_id)

    notificar_monitor({
    "tipo": "DRONE_DESPACHADO",
    "base": BASE_ID,
    "drone": drone.drone_id,
    "setor": entrada.id_setor
    })
    

def _processar_requisicao(msg: dict):
    id_req, id_setor, ts = msg.get("id_requisicao", ""), msg.get("id_setor", ""), msg.get("timestamp_logico", 0)

    if not estado.verificar_e_registrar_vista(id_req):
        return

    clock.atualizar(ts)
    entrada = EntradaFila(
        id_requisicao=id_req, id_setor=id_setor, timestamp_logico=ts,
        criticidade=msg.get("criticidade", Criticidade.BAIXA.value),
        tipo_ocorrencia=msg.get("tipo_ocorrencia", "")
    )
    estado.inserir_na_fila(entrada)

    timeout_s = prioridade.timeout_para_setor(id_setor)
    logger.info("[%s] Req %s recebida | setor %s | Lamport=%d | timeout=%.0fms", BASE_ID, id_req[:8], id_setor, ts, timeout_s * 1000)

    timer = threading.Timer(timeout_s, _tentar_aceitar, args=(id_req,))
    with _timers_lock:
        _timers[id_req] = timer
    timer.start()

def _processar_aceite(msg: dict):
    id_req, ts = msg.get("id_requisicao", ""), msg.get("timestamp_logico", 0)
    clock.atualizar(ts)
    estado.marcar_aceita(id_req)

    with _timers_lock:
        timer = _timers.pop(id_req, None)
    if timer: timer.cancel()

def _processar_registro(msg: dict):
    drone_id, porta_tcp = msg.get("drone_id", ""), int(msg.get("porta", 7001))
    estado.registrar_drone(drone_id, porta_tcp)
    logger.info("[%s] Drone %s registrado (porta TCP %d).", BASE_ID, drone_id, porta_tcp)
    threading.Thread(target=_processar_fila_pendente, daemon=True).start()

def _processar_heartbeat_tcp(msg: dict):
    drone_id = msg.get("drone_id", "")
    estado_drone = msg.get("estado", EstadoDrone.LIVRE.value)
    missao_concluida = msg.get("missao_concluida")

    estado.atualizar_estado_drone(drone_id, estado_drone)

    if missao_concluida:
        estado.marcar_concluida(missao_concluida)
        logger.info("[%s] Missão %s concluída pelo drone %s.", BASE_ID, missao_concluida[:8], drone_id)
        threading.Thread(target=_processar_fila_pendente, daemon=True).start()

        notificar_monitor({
        "tipo": "MISSAO_CONCLUIDA",
        "base": BASE_ID,
        "drone": drone_id,
        "setor_concluido": missao_concluida
    })

def _processar_reemissao(msg: dict):
    id_req, id_setor = msg.get("id_requisicao", ""), msg.get("id_setor", "")
    clock.atualizar(msg.get("timestamp_logico_base", 0))
    estado.remover_vista(id_req)

    entrada = estado.obter_entrada(id_req)
    if entrada:
        with estado.fila_lock:
            entrada.status = StatusRequisicao.PENDENTE.value
    else:
        nova_entrada = EntradaFila(
            id_requisicao=id_req, id_setor=id_setor, timestamp_logico=msg.get("timestamp_logico", 0),
            criticidade=msg.get("criticidade", Criticidade.BAIXA.value), tipo_ocorrencia=msg.get("tipo_ocorrencia", "")
        )
        estado.inserir_na_fila(nova_entrada)

    timeout_s = prioridade.timeout_para_setor(id_setor)
    timer = threading.Timer(timeout_s, _tentar_aceitar, args=(id_req,))
    with _timers_lock:
        _timers[id_req] = timer
    timer.start()

def _processar_fila_pendente():
    pendentes = estado.obter_pendentes()
    for entrada in pendentes:
        with _timers_lock:
            if entrada.id_requisicao in _timers: continue
        _tentar_aceitar(entrada.id_requisicao)

def _tratar_drone_perdido(drone_id: str):
    with estado.drones_lock:
        info = estado.drones.get(drone_id)
        if not info or info.estado == EstadoDrone.PERDIDO.value: return
        id_req_em_curso = info.id_requisicao_atual
        info.estado = EstadoDrone.PERDIDO.value
        info.id_requisicao_atual = None

    logger.warning("[%s] Drone %s marcado como PERDIDO.", BASE_ID, drone_id)
    if not id_req_em_curso: return

    entrada = estado.obter_entrada(id_req_em_curso)
    if entrada:
        with estado.fila_lock:
            entrada.status = StatusRequisicao.PENDENTE.value

        reemissao = {
            "tipo": TipoMensagem.REEMISSAO.value,
            "id_requisicao": entrada.id_requisicao,
            "id_setor": entrada.id_setor,
            "timestamp_logico": entrada.timestamp_logico,
            "criticidade": entrada.criticidade,
            "tipo_ocorrencia": entrada.tipo_ocorrencia,
            "timestamp_logico_base": clock.incrementar(),
        }
        tcp_broadcast(OUTRAS_BASES, reemissao)
        
        timeout_s = prioridade.timeout_para_setor(entrada.id_setor)
        timer = threading.Timer(timeout_s, _tentar_aceitar, args=(entrada.id_requisicao,))
        with _timers_lock:
            _timers[entrada.id_requisicao] = timer
        timer.start()

def _monitor_heartbeat():
    while True:
        time.sleep(HEARTBEAT_INTERVALO_S)
        agora = time.time()
        with estado.drones_lock:
            snapshot = list(estado.drones.values())

        for info in snapshot:
            if info.estado == EstadoDrone.PERDIDO.value: continue
            if (agora - info.ultimo_heartbeat) > HEARTBEAT_TIMEOUT_S:
                _tratar_drone_perdido(info.drone_id)

def _loop_udp():
    servidor = criar_servidor_udp(MINHA_PORTA_UDP)
    while True:
        try:
            dados, addr = servidor.recvfrom(BUFFER_UDP)
            import json
            try: msg = json.loads(dados.decode("utf-8"))
            except: continue

            drone_id = msg.get("drone_id", "")
            estado_drone = msg.get("estado", EstadoDrone.LIVRE.value)
            porta = int(msg.get("porta", 7001))
            
            with estado.drones_lock:
                info = estado.drones.get(drone_id)
                if info:
                    info.estado = estado_drone
                    info.ultimo_heartbeat = time.time()
                    info.falhas_heartbeat = 0
                else:
                    estado.registrar_drone(drone_id, porta, estado_drone)
        except Exception as e:
            logger.error("[%s] Erro UDP: %s", BASE_ID, e)

def _despachar_mensagem(msg: dict):
    rotas = {
        TipoMensagem.REQUISICAO.value: _processar_requisicao,
        TipoMensagem.ACEITE.value: _processar_aceite,
        TipoMensagem.REGISTRO.value: _processar_registro,
        TipoMensagem.HEARTBEAT.value: _processar_heartbeat_tcp,
        TipoMensagem.REEMISSAO.value: _processar_reemissao,
    }
    handler = rotas.get(msg.get("tipo", ""))
    if handler: handler(msg)
    else: logger.warning("[%s] Msg desconhecida.", BASE_ID)

def _tratar_conexao(conn, addr):
    try:
        msg = tcp_receber_completo(conn)
        conn.close()
        if msg: _despachar_mensagem(msg)
    except Exception as e:
        logger.error("[%s] Erro conexão %s: %s", BASE_ID, addr, e)

def main():
    logger.info("[%s] Broker base iniciado. TCP=%d UDP=%d", BASE_ID, MINHA_PORTA, MINHA_PORTA_UDP)
    threading.Thread(target=_loop_udp, daemon=True, name="udp-hb").start()
    threading.Thread(target=_monitor_heartbeat, daemon=True, name="mon-hb").start()

    servidor = criar_servidor_tcp(MINHA_PORTA)
    with ThreadPoolExecutor(max_workers=16, thread_name_prefix="base") as pool:
        while True:
            try:
                conn, addr = servidor.accept()
                pool.submit(_tratar_conexao, conn, addr)
            except Exception as e:
                logger.error("Erro accept: %s", e)

if __name__ == "__main__":
    main()