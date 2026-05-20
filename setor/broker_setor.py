import os
import sys
import time
import logging
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

# pylint: disable=import-error, wrong-import-position
from protocolo import notificar_monitor, criar_servidor_tcp, tcp_receber_completo, tcp_broadcast
from constantes import TipoMensagem
from mensagens import MensagemRequisicao
from lamport import LamportClock

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("broker_setor")

# ── Configuração via ambiente ─────────────────────────────────────────────────

SETOR_ID     = os.environ.get("SETOR_ID", "S1")
SETOR_NOME   = os.environ.get("SETOR_NOME", "Norte")
MINHA_PORTA  = int(os.environ.get("MINHA_PORTA", "5001"))

IP_BASE_NORTE = os.environ.get("IP_BASE_NORTE", "127.0.0.1")
IP_BASE_SUL   = os.environ.get("IP_BASE_SUL",   "127.0.0.1")
IP_BASE_LESTE = os.environ.get("IP_BASE_LESTE", "127.0.0.1")
IP_BASE_OESTE = os.environ.get("IP_BASE_OESTE", "127.0.0.1")

PORTA_BASE_NORTE = int(os.environ.get("PORTA_BASE_NORTE", "6001"))
PORTA_BASE_SUL   = int(os.environ.get("PORTA_BASE_SUL",   "6002"))
PORTA_BASE_LESTE = int(os.environ.get("PORTA_BASE_LESTE", "6003"))
PORTA_BASE_OESTE = int(os.environ.get("PORTA_BASE_OESTE", "6004"))

PRIORIDADE = os.environ.get("PRIORIDADE", "NORTE,SUL,LESTE,OESTE")

# Quantas vezes tentar reenviar para bases que não responderam
BROADCAST_MAX_TENTATIVAS = int(os.environ.get("BROADCAST_MAX_TENTATIVAS", "3"))
BROADCAST_RETRY_DELAY_S  = float(os.environ.get("BROADCAST_RETRY_DELAY_S", "1.0"))

# ── Destinos de broadcast (todas as 4 bases) ──────────────────────────────────

BASES: list[tuple[str, int]] = [
    (IP_BASE_NORTE, PORTA_BASE_NORTE),
    (IP_BASE_SUL,   PORTA_BASE_SUL),
    (IP_BASE_LESTE, PORTA_BASE_LESTE),
    (IP_BASE_OESTE, PORTA_BASE_OESTE),
]

# ── Relógio de Lamport (compartilhado entre threads) ─────────────────────────

clock = LamportClock()


# ── Broadcast com retry ───────────────────────────────────────────────────────

def broadcast_com_retry(payload: dict) -> dict[str, bool]:
    """
    Tenta entregar o payload para todas as bases.

    Protocolo de tolerância a falhas de comunicação:
    - Primeira tentativa: broadcast simultâneo para todas as 4 bases.
    - Tentativas seguintes: apenas para as bases que falharam na tentativa anterior.
    - Espera BROADCAST_RETRY_DELAY_S entre tentativas.
    - Após BROADCAST_MAX_TENTATIVAS sem sucesso numa base, registra falha e segue.

    Justificativa: uma base temporariamente offline não pode bloquear o despacho
    para as demais. O sistema continua operando com 3 das 4 bases — o drone menos
    prioritário simplesmente não tem a oportunidade de aceitar naquela rodada.
    """
    resultados_finais: dict[str, bool] = {}
    pendentes = list(BASES)

    for tentativa in range(1, BROADCAST_MAX_TENTATIVAS + 1):
        if not pendentes:
            break

        parcial = tcp_broadcast(pendentes, payload)
        resultados_finais.update(parcial)

        falhas = [(h, p) for (h, p) in pendentes if not parcial.get(f"{h}:{p}", False)]

        enviados = len(pendentes) - len(falhas)
        logger.info(
            "[%s] Broadcast tentativa %d/%d — %d/%d bases alcançadas%s",
            SETOR_ID, tentativa, BROADCAST_MAX_TENTATIVAS,
            enviados, len(pendentes),
            f" | {len(falhas)} offline, aguardando {BROADCAST_RETRY_DELAY_S}s para retry" if falhas else "",
        )

        if not falhas:
            break

        pendentes = falhas
        if tentativa < BROADCAST_MAX_TENTATIVAS:
            time.sleep(BROADCAST_RETRY_DELAY_S)

    if pendentes:
        logger.warning(
            "[%s] Bases não alcançadas após %d tentativas: %s",
            SETOR_ID, BROADCAST_MAX_TENTATIVAS,
            [f"{h}:{p}" for h, p in pendentes],
        )

    return resultados_finais


# ── Processamento de alertas ──────────────────────────────────────────────────

def processar_alerta(msg: dict):
    """
    Recebe um alerta do sensor, cria uma requisição com timestamp de Lamport
    e faz broadcast com retry para todas as bases.
    """
    tipo = msg.get("tipo")

    if tipo != TipoMensagem.ALERTA.value:
        logger.warning("[%s] Mensagem ignorada — tipo inesperado: %s", SETOR_ID, tipo)
        return

    ts = clock.incrementar()

    requisicao = MensagemRequisicao(
        id_setor         = SETOR_ID,
        timestamp_logico = ts,
        criticidade      = msg.get("criticidade", "BAIXA"),
        tipo_ocorrencia  = msg.get("tipo_ocorrencia", "anomalia_menor"),
    )

    payload = asdict(requisicao)

    logger.info(
        "[%s] Alerta recebido → req %s | %s [%s] | Lamport=%d",
        SETOR_ID,
        requisicao.id_requisicao[:8],
        requisicao.tipo_ocorrencia,
        requisicao.criticidade,
        ts,
    )

    broadcast_com_retry(payload)

    notificar_monitor({
        "tipo": "ALERTA_GERADO",
        "setor": SETOR_ID,
        "criticidade": requisicao.criticidade,
    })


# ── Loop servidor TCP ─────────────────────────────────────────────────────────

def loop_servidor():
    servidor = criar_servidor_tcp(MINHA_PORTA)
    logger.info(
        "[%s — %s] Broker iniciado na porta %d | prioridade: %s",
        SETOR_ID, SETOR_NOME, MINHA_PORTA, PRIORIDADE,
    )

    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="alerta") as pool:
        while True:
            try:
                conn, addr = servidor.accept()
                pool.submit(_tratar_conexao, conn, addr)
            except Exception as e:
                logger.error("[%s] Erro no accept: %s", SETOR_ID, e, exc_info=True)


def _tratar_conexao(conn, addr):
    try:
        msg = tcp_receber_completo(conn)
        conn.close()
        if msg:
            processar_alerta(msg)
        else:
            logger.warning("[%s] Conexão vazia de %s", SETOR_ID, addr)
    except Exception as e:
        logger.error("[%s] Erro ao tratar conexão de %s: %s", SETOR_ID, addr, e, exc_info=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info(
        "[%s] Inicializando broker | Bases: Norte=%s:%d Sul=%s:%d Leste=%s:%d Oeste=%s:%d | retry=%dx @ %.1fs",
        SETOR_ID,
        IP_BASE_NORTE, PORTA_BASE_NORTE,
        IP_BASE_SUL,   PORTA_BASE_SUL,
        IP_BASE_LESTE, PORTA_BASE_LESTE,
        IP_BASE_OESTE, PORTA_BASE_OESTE,
        BROADCAST_MAX_TENTATIVAS,
        BROADCAST_RETRY_DELAY_S,
    )
    loop_servidor()


if __name__ == "__main__":
    main()