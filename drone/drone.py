import os
import random
import logging
from dataclasses import asdict
import sys
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

# pylint: disable=import-error, wrong-import-position
from constantes import EstadoDrone, TipoMensagem, HEARTBEAT_INTERVALO_S
from mensagens import MensagemRegistro, MensagemHeartbeat
from protocolo import (
    tcp_enviar,
    tcp_receber_completo,
    criar_servidor_tcp,
    udp_enviar
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger("drone")

# Configurações via Ambiente
DRONE_ID = os.environ.get("DRONE_ID", "DRONE-NORTE-1")
BASE_ORIGEM = os.environ.get("BASE_ORIGEM", "NORTE")
IP_BASE = os.environ.get("IP_BASE", "127.0.0.1")

PORTA_BASE_TCP = int(os.environ.get("PORTA_BASE_TCP", "6001"))
PORTA_BASE_UDP = int(os.environ.get("PORTA_BASE_UDP", "6101"))
PORTA_HEARTBEAT = int(os.environ.get("PORTA_HEARTBEAT", "7001"))
HEARTBEAT_INTERVALO = float(
    os.environ.get("HEARTBEAT_INTERVALO", str(HEARTBEAT_INTERVALO_S))
)

MISSAO_DURACAO_MIN = float(os.environ.get("MISSAO_DURACAO_MIN", "8"))
MISSAO_DURACAO_MAX = float(os.environ.get("MISSAO_DURACAO_MAX", "20"))
MAX_TENTATIVAS = int(os.environ.get("MAX_TENTATIVAS", "5"))


class Drone:
    def __init__(self):
        self.estado = EstadoDrone.LIVRE
        self.id_requisicao_atual = None
        self._lock = threading.Lock()

    def ocupar(self, id_requisicao):
        with self._lock:
            self.estado = EstadoDrone.OCUPADO
            self.id_requisicao_atual = id_requisicao

    def liberar(self):
        with self._lock:
            self.estado = EstadoDrone.LIVRE
            self.id_requisicao_atual = None

    @property
    def livre(self):
        with self._lock:
            return self.estado == EstadoDrone.LIVRE


drone = Drone()

def registrar_na_base():
    msg = MensagemRegistro(
        drone_id=DRONE_ID,
        base_id=BASE_ORIGEM,
        porta=PORTA_HEARTBEAT
    )

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        # CORREÇÃO LAZY LOGGING: Passando argumentos como parâmetros do logger
        logger.info("[%s] Tentando registrar (tentativa %d/%d)", 
                    DRONE_ID, tentativa, MAX_TENTATIVAS)

        if tcp_enviar(IP_BASE, PORTA_BASE_TCP, asdict(msg)):
            logger.info("[%s] Registro realizado com sucesso", DRONE_ID)
            return

        espera = min(2 ** tentativa, 30)
        logger.warning("[%s] Falha. Nova tentativa em %ds", DRONE_ID, espera)
        time.sleep(espera)

    logger.error("[%s] Falha após %d tentativas", DRONE_ID, MAX_TENTATIVAS)
    sys.exit(1)


def enviar_heartbeat():
    logger.info("[%s] Heartbeat iniciado", DRONE_ID)

    while True:
        msg = MensagemHeartbeat(
            drone_id=DRONE_ID,
            base_id=BASE_ORIGEM,
            estado=drone.estado.value,
            id_requisicao=drone.id_requisicao_atual
        )

        udp_enviar(IP_BASE, PORTA_BASE_UDP, asdict(msg))
        time.sleep(HEARTBEAT_INTERVALO)


def executar_missao(dados_missao):
    id_req = dados_missao.get("id_requisicao", "?")
    setor = dados_missao.get("setor_id", "?")
    tipo = dados_missao.get("tipo_ocorrencia", "?")
    criticidade = dados_missao.get("criticidade", "?")

    logger.info("[%s] Iniciando missão %s | Setor:%s | Tipo:%s [%s]", 
                DRONE_ID, id_req, setor, tipo, criticidade)

    drone.ocupar(id_req)
    duracao = random.uniform(MISSAO_DURACAO_MIN, MISSAO_DURACAO_MAX)

    logger.info("[%s] Missão em andamento (%.1fs)", DRONE_ID, duracao)
    time.sleep(duracao)

    drone.liberar()
    logger.info("[%s] Missão concluída", DRONE_ID)

    conclusao = {
        "tipo": TipoMensagem.HEARTBEAT.value,
        "drone_id": DRONE_ID,
        "base_id": BASE_ORIGEM,
        "estado": drone.estado.value,
        "id_requisicao_atual": None,
        "missao_concluida": id_req
    }

    tcp_enviar(IP_BASE, PORTA_BASE_TCP, conclusao)


def loop_servidor_tcp():
    servidor = criar_servidor_tcp(PORTA_HEARTBEAT)
    logger.info("[%s] Servidor TCP iniciado na porta %d", DRONE_ID, PORTA_HEARTBEAT)

    while True:
        try:
            conn, addr = servidor.accept()
            msg = tcp_receber_completo(conn)
            conn.close()

            if not msg:
                logger.warning("[%s] Sem dados de %s", DRONE_ID, addr)
                continue

            tipo = msg.get("tipo")

            if tipo == "MISSAO":
                if drone.livre:
                    threading.Thread(
                        target=executar_missao,
                        args=(msg,),
                        daemon=True
                    ).start()
                else:
                    logger.warning("[%s] Drone ocupado, ignorando missão %s", 
                                   DRONE_ID, msg.get("id_requisicao"))
            else:
                logger.warning("[%s] Tipo de mensagem desconhecido: %s", DRONE_ID, tipo)

        except Exception as e:
            logger.error("[%s] Erro no loop TCP: %s", DRONE_ID, e, exc_info=True)


def main():
    logger.info("[%s] Inicializando...", DRONE_ID)

    registrar_na_base()

    t_heartbeat = threading.Thread(
        target=enviar_heartbeat,
        daemon=True
    )
    t_heartbeat.start()

    loop_servidor_tcp()


if __name__ == "__main__":
    main()