"""
constantes.py — Enums e valores fixos compartilhados por todos os módulos.
Importar daqui garante que sensor, broker_setor, broker_base e drone
falem a mesma língua.
"""

from enum import Enum


# ── Criticidade das ocorrências ─────────────────────────────────────────────

class Criticidade(str, Enum):
    CRITICA = "CRITICA"   # Embarcação em perigo imediato, explosivo
    ALTA    = "ALTA"      # Bloqueio de rota, objeto não identificado
    BAIXA   = "BAIXA"    

    def peso(self) -> int:
        """Retorna peso numérico para ordenação (maior = mais urgente)."""
        pesos = {
            Criticidade.CRITICA: 3,
            Criticidade.ALTA:    2,
            Criticidade.BAIXA:   1,
        }
        return pesos[self]


# ── Tipos de ocorrência ──────────────────────────────────────────────────────

class TipoOcorrencia(str, Enum):
    EMBARCACAO_DERIVA        = "embarcacao_deriva"
    BLOQUEIO_ROTA            = "bloqueio_rota"
    FALHA_SINALIZACAO        = "falha_sinalizacao"
    OBJETO_NAO_IDENTIFICADO  = "objeto_nao_identificado"
    EMBARCACAO_PERIGO        = "embarcacao_perigo"
    ANOMALIA_MENOR           = "anomalia_menor"


# ── Mapeamento tipo → criticidade padrão ────────────────────────────────────
# Usado pelo sensor para montar a requisição com criticidade coerente.

CRITICIDADE_POR_TIPO: dict[TipoOcorrencia, Criticidade] = {
    TipoOcorrencia.EMBARCACAO_PERIGO:       Criticidade.CRITICA,
    TipoOcorrencia.BLOQUEIO_ROTA:           Criticidade.ALTA,
    TipoOcorrencia.OBJETO_NAO_IDENTIFICADO: Criticidade.ALTA,
    TipoOcorrencia.EMBARCACAO_DERIVA:       Criticidade.BAIXA,
    TipoOcorrencia.FALHA_SINALIZACAO:       Criticidade.BAIXA,
    TipoOcorrencia.ANOMALIA_MENOR:          Criticidade.BAIXA,
}


# ── Estado dos drones ────────────────────────────────────────────────────────

class EstadoDrone(str, Enum):
    LIVRE    = "LIVRE"
    OCUPADO  = "OCUPADO"
    PERDIDO  = "PERDIDO"


# ── Status de uma requisição na fila ────────────────────────────────────────

class StatusRequisicao(str, Enum):
    PENDENTE = "pendente"
    ACEITA   = "aceita"
    CONCLUIDA = "concluida"


# ── Tipos de mensagem trocadas entre entidades ───────────────────────────────

class TipoMensagem(str, Enum):
    ALERTA       = "ALERTA"        # Sensor → Broker de setor
    REQUISICAO   = "REQUISICAO"    # Broker de setor → Bases (broadcast)
    ACEITE       = "ACEITE"        # Base → outras Bases (broadcast)
    HEARTBEAT    = "HEARTBEAT"     # Drone → Base (UDP)
    REGISTRO     = "REGISTRO"      # Drone → Base (ao iniciar)
    REEMISSAO    = "REEMISSAO"     # Base → Bases (drone perdido)


# ── Timeouts e intervalos (em ms / s) ───────────────────────────────────────

TIMEOUT_PRIORIDADE_MS = {
    1: 0,     # 1ª prioridade: tenta imediatamente
    2: 200,
    3: 400,
    4: 600,
}

HEARTBEAT_INTERVALO_S  = 3   # Drone envia heartbeat a cada N segundos
HEARTBEAT_MAX_FALHAS   = 3   # Após N falhas consecutivas → drone PERDIDO