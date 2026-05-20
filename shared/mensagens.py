import uuid
from dataclasses import dataclass, field
from typing import Optional
from constantes import TipoMensagem

# =====================================================================
#  Classes de Mensagens (Dataclasses para facilitar conversão em JSON)
# =====================================================================

@dataclass
class MensagemAlerta:
    """Enviada pelo Sensor para o Broker do Setor"""
    setor_id: str
    criticidade: str
    tipo_ocorrencia: str
    # Gera um ID único para o alerta automaticamente
    id_alerta: str = field(default_factory=lambda: str(uuid.uuid4()))
    tipo: str = TipoMensagem.ALERTA.value

@dataclass
class MensagemRequisicao:
    """Gerada pelo Broker do Setor e enviada via broadcast para as Bases"""
    id_setor: str
    timestamp_logico: int
    criticidade: str
    tipo_ocorrencia: str
    # Gera um UUID único automaticamente se não for passado
    id_requisicao: str = field(default_factory=lambda: str(uuid.uuid4()))
    tipo: str = TipoMensagem.REQUISICAO.value

@dataclass
class MensagemRegistro:
    """Enviada pelo Drone para a Base ao iniciar (TCP)"""
    drone_id: str
    base_id: str
    porta: int
    tipo: str = TipoMensagem.REGISTRO.value

@dataclass
class MensagemHeartbeat:
    """Enviada periodicamente (UDP) ou no fim de uma missão (TCP) pelo Drone"""
    drone_id: str
    base_id: str
    estado: str
    id_requisicao: Optional[str] = None
    missao_concluida: Optional[str] = None
    tipo: str = TipoMensagem.HEARTBEAT.value