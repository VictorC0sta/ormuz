import os
import sys
import logging
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

# pylint: disable=import-error, wrong-import-position
from protocolo import notificar_monitor
from constantes import TipoMensagem
from mensagens import MensagemRequisicao
from protocolo import criar_servidor_tcp, tcp_receber_completo, tcp_broadcast
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

# "LESTE,NORTE,SUL,OESTE" → usado só para log, a ordem real fica nas bases
PRIORIDADE = os.environ.get("PRIORIDADE", "NORTE,SUL,LESTE,OESTE")

# ── Destinos de broadcast (todas as 4 bases) ──────────────────────────────────

BASES: list[tuple[str, int]] = [
    (IP_BASE_NORTE, PORTA_BASE_NORTE),
    (IP_BASE_SUL,   PORTA_BASE_SUL),
    (IP_BASE_LESTE, PORTA_BASE_LESTE),
    (IP_BASE_OESTE, PORTA_BASE_OESTE),
]

# ── Relógio de Lamport (compartilhado entre threads) ─────────────────────────

clock = LamportClock()


# ── Processamento de alertas ──────────────────────────────────────────────────

def processar_alerta(msg: dict):
    """
    Recebe um alerta do sensor, cria uma requisição com timestamp de Lamport
    e faz broadcast para todas as bases simultaneamente.
    """
    tipo = msg.get("tipo")

    if tipo != TipoMensagem.ALERTA.value:
        logger.warning("[%s] Mensagem ignorada — tipo inesperado: %s", SETOR_ID, tipo)
        return

    # Incrementa Lamport antes de criar a requisição
    ts = clock.incrementar()

    requisicao = MensagemRequisicao(
        id_setor        = SETOR_ID,
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

    # Broadcast simultâneo para as 4 bases
    resultados = tcp_broadcast(BASES, payload)

    enviados  = sum(1 for ok in resultados.values() if ok)
    falhas    = len(resultados) - enviados

    logger.info(
        "[%s] Broadcast concluído — %d/4 bases alcançadas%s",
        SETOR_ID,
        enviados,
        f" ({falhas} offline)" if falhas else "",
    )
    
    notificar_monitor({
    "tipo": "ALERTA_GERADO", 
    "setor": SETOR_ID, 
    "criticidade": requisicao.criticidade
})

# ── Loop servidor TCP ─────────────────────────────────────────────────────────

def loop_servidor():
    """
    Aceita conexões TCP dos sensores.
    Cada alerta é processado em thread separada via ThreadPoolExecutor
    para não bloquear o accept() durante o broadcast.
    """
    servidor = criar_servidor_tcp(MINHA_PORTA)
    logger.info(
        "[%s — %s] Broker iniciado na porta %d | prioridade: %s",
        SETOR_ID, SETOR_NOME, MINHA_PORTA, PRIORIDADE,
    )

    # Pool com 4 workers: suficiente para processar alertas simultâneos
    # sem criar threads ilimitadas
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="alerta") as pool:
        while True:
            try:
                conn, addr = servidor.accept()

                # Leitura e processamento em thread separada
                pool.submit(_tratar_conexao, conn, addr)

            except Exception as e:
                logger.error("[%s] Erro no accept: %s", SETOR_ID, e, exc_info=True)


def _tratar_conexao(conn, addr):
    """Lê a mensagem de uma conexão aceita e despacha para processamento."""
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
        "[%s] Inicializando broker de setor | Bases: Norte=%s:%d Sul=%s:%d Leste=%s:%d Oeste=%s:%d",
        SETOR_ID,
        IP_BASE_NORTE, PORTA_BASE_NORTE,
        IP_BASE_SUL,   PORTA_BASE_SUL,
        IP_BASE_LESTE, PORTA_BASE_LESTE,
        IP_BASE_OESTE, PORTA_BASE_OESTE,
    )
    loop_servidor()


if __name__ == "__main__":
    main()