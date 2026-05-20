import os 
import time
import random
import logging
from dataclasses import asdict
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

# pylint: disable=import-error, wrong-import-position
from constantes import TipoOcorrencia, CRITICIDADE_POR_TIPO
from mensagens import MensagemAlerta
from protocolo import tcp_enviar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("Sensor")

# Configurações via Ambiente
SENSOR_ID = os.environ.get("SENSOR_ID", "SENSOR-S1")
SETOR_ID  = os.environ.get("SETOR_ID", "S1")
IP_BROKER_SETOR = os.environ.get("IP_BROKER_SETOR", "127.0.0.1")
PORTA_BROKER_SETOR = int(os.environ.get("PORTA_BROKER_SETOR", 5050))
INTERVALO_MIN = float(os.environ.get("INTERVALO_MIN", 5))
INTERVALO_MAX = float(os.environ.get("INTERVALO_MAX", 15))

TIPOS_E_PESOS = [
    (TipoOcorrencia.EMBARCACAO_PERIGO,       3),
    (TipoOcorrencia.BLOQUEIO_ROTA,           7),
    (TipoOcorrencia.OBJETO_NAO_IDENTIFICADO, 10),
    (TipoOcorrencia.EMBARCACAO_DERIVA,       20),
    (TipoOcorrencia.FALHA_SINALIZACAO,       25),
    (TipoOcorrencia.ANOMALIA_MENOR,          35),
]

_tipos = [tipo for tipo, peso in TIPOS_E_PESOS]
_pesos = [peso for tipo, peso in TIPOS_E_PESOS]

def gerar_ocorrencia() -> MensagemAlerta:
    tipo = random.choices(_tipos, weights=_pesos, k=1)[0]
    criticidade = CRITICIDADE_POR_TIPO[tipo]

    return MensagemAlerta(
        setor_id=SETOR_ID,
        tipo_ocorrencia=tipo.value,
        criticidade=criticidade,
    )

def enviar_alerta(alerta: MensagemAlerta) -> bool:
    """Serializa e envia o alerta ao broker do setor via TCP."""
    payload = asdict(alerta)
    sucesso = tcp_enviar(IP_BROKER_SETOR, PORTA_BROKER_SETOR, payload)
 
    if sucesso:
        # CORREÇÃO LAZY LOGGING: Passando valores como argumentos
        logger.info("[%s] Alerta enviado → %s | %s [%s] (id: %s...)",
                    SENSOR_ID, SETOR_ID, alerta.tipo_ocorrencia, 
                    alerta.criticidade, alerta.id_alerta[:8])
    else:
        logger.error("[%s] Falha ao enviar alerta para broker %s:%d — "
                     "broker offline? Tentará novamente no próximo ciclo.",
                     SENSOR_ID, IP_BROKER_SETOR, PORTA_BROKER_SETOR)
    return sucesso

def main():
    logger.info("[%s] Iniciando — setor %s | broker em %s:%d | intervalo %.1f–%.1fs",
                SENSOR_ID, SETOR_ID, IP_BROKER_SETOR, PORTA_BROKER_SETOR, 
                INTERVALO_MIN, INTERVALO_MAX)
 
    tempo_inicial = random.uniform(2, 8)
    logger.info("[%s] Aguardando %.1fs antes do primeiro alerta...", SENSOR_ID, tempo_inicial)
    time.sleep(tempo_inicial)
 
    while True:
        alerta = gerar_ocorrencia()
        enviar_alerta(alerta)
 
        intervalo = random.uniform(INTERVALO_MIN, INTERVALO_MAX)
        # Log de debug também deve ser lazy
        logger.debug("[%s] Próxima ocorrência em %.1fs", SENSOR_ID, intervalo)
        time.sleep(intervalo)
 
if __name__ == "__main__":
    main()