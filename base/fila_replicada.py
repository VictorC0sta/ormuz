import threading
import time
from dataclasses import dataclass, field
from typing import Optional
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
# pylint: disable=import-error, wrong-import-position
from constantes import EstadoDrone, StatusRequisicao, Criticidade

@dataclass(order=False)
class EntradaFila:
    id_requisicao: str
    id_setor: str
    timestamp_logico: int
    criticidade: str
    tipo_ocorrencia: str
    status: str = StatusRequisicao.PENDENTE.value

    def chave_ordenacao(self) -> tuple:
        try:
            peso = Criticidade(self.criticidade).peso()
        except ValueError:
            peso = 0
        return (-peso, self.timestamp_logico, self.id_setor)

@dataclass
class InfoDrone:
    drone_id: str
    porta_tcp: int
    estado: str = EstadoDrone.LIVRE.value
    id_requisicao_atual: Optional[str] = None
    ultimo_heartbeat: float = field(default_factory=time.time)
    falhas_heartbeat: int = 0

class FilaReplicada:
    def __init__(self):
        self.fila: list[EntradaFila] = []
        self.fila_lock = threading.Lock()
        
        self.requisicoes_vistas: set[str] = set()
        self.vistas_lock = threading.Lock()
        
        self.drones: dict[str, InfoDrone] = {}
        self.drones_lock = threading.Lock()

    # --- Controle de Fila ---
    def inserir_na_fila(self, entrada: EntradaFila):
        with self.fila_lock:
            self.fila.append(entrada)
            self.fila.sort(key=lambda e: e.chave_ordenacao())

    def verificar_e_registrar_vista(self, id_requisicao: str) -> bool:
        """Retorna True se é nova, False se já foi vista."""
        with self.vistas_lock:
            if id_requisicao in self.requisicoes_vistas:
                return False
            self.requisicoes_vistas.add(id_requisicao)
            return True

    def remover_vista(self, id_requisicao: str):
        with self.vistas_lock:
            self.requisicoes_vistas.discard(id_requisicao)

    def marcar_aceita(self, id_requisicao: str) -> bool:
        with self.fila_lock:
            for entrada in self.fila:
                if entrada.id_requisicao == id_requisicao:
                    if entrada.status == StatusRequisicao.PENDENTE.value:
                        entrada.status = StatusRequisicao.ACEITA.value
                        return True
                    return False
        return False

    def marcar_concluida(self, id_requisicao: str):
        with self.fila_lock:
            for entrada in self.fila:
                if entrada.id_requisicao == id_requisicao:
                    entrada.status = StatusRequisicao.CONCLUIDA.value
                    return

    def status_requisicao(self, id_requisicao: str) -> Optional[str]:
        with self.fila_lock:
            for entrada in self.fila:
                if entrada.id_requisicao == id_requisicao:
                    return entrada.status
        return None
        
    def obter_entrada(self, id_requisicao: str) -> Optional[EntradaFila]:
        with self.fila_lock:
            return next((e for e in self.fila if e.id_requisicao == id_requisicao), None)

    def obter_pendentes(self) -> list[EntradaFila]:
        with self.fila_lock:
            return [e for e in self.fila if e.status == StatusRequisicao.PENDENTE.value]

    # --- Controle de Drones ---
    def drone_livre(self) -> Optional[InfoDrone]:
        with self.drones_lock:
            for info in self.drones.values():
                if info.estado == EstadoDrone.LIVRE.value:
                    return info
        return None

    def ocupar_drone(self, drone_id: str, id_requisicao: str):
        with self.drones_lock:
            if drone_id in self.drones:
                self.drones[drone_id].estado = EstadoDrone.OCUPADO.value
                self.drones[drone_id].id_requisicao_atual = id_requisicao

    def atualizar_estado_drone(self, drone_id: str, estado: str):
        with self.drones_lock:
            if drone_id in self.drones:
                self.drones[drone_id].estado = estado
                self.drones[drone_id].ultimo_heartbeat = time.time()
                self.drones[drone_id].falhas_heartbeat = 0
                if estado == EstadoDrone.LIVRE.value:
                    self.drones[drone_id].id_requisicao_atual = None

    def registrar_drone(self, drone_id: str, porta_tcp: int, estado: str = EstadoDrone.LIVRE.value):
        with self.drones_lock:
            if drone_id not in self.drones:
                self.drones[drone_id] = InfoDrone(drone_id=drone_id, porta_tcp=porta_tcp, estado=estado)
            else:
                self.drones[drone_id].porta_tcp = porta_tcp
                self.drones[drone_id].estado = estado