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
    """Guarda os dados de uma ocorrência/missão que precisa ser atendida."""
    id_requisicao: str
    id_setor: str
    timestamp_logico: int
    criticidade: str
    tipo_ocorrencia: str
    status: str = StatusRequisicao.PENDENTE.value

    def chave_ordenacao(self) -> tuple:
        """
        Regra para organizar a fila:
        1º Mais graves primeiro (-peso)
        2º Mais antigos primeiro (relógio de Lamport menor)
        3º Desempate pelo nome do setor
        """
        try:
            peso = Criticidade(self.criticidade).peso()
        except ValueError:
            peso = 0
        return (-peso, self.timestamp_logico, self.id_setor)

@dataclass
class InfoDrone:
    """A 'ficha' do drone: guarda quem ele é, porta de rede e se está trabalhando."""
    drone_id: str
    porta_tcp: int
    estado: str = EstadoDrone.LIVRE.value
    id_requisicao_atual: Optional[str] = None
    ultimo_heartbeat: float = field(default_factory=time.time)
    falhas_heartbeat: int = 0

class FilaReplicada:
    """
    A 'memória' principal da Base. 
    Guarda a fila de missões e a lista de drones locais.
    Usa 'Locks' (travas) em tudo para o sistema não bugar quando várias 
    mensagens chegam na base no mesmo milissegundo.
    """
    def __init__(self):
        # A fila de missões em si
        self.fila: list[EntradaFila] = []
        self.fila_lock = threading.Lock()
        
        # Histórico do que já chegou para não processar a mesma mensagem duas vezes
        self.requisicoes_vistas: set[str] = set()
        self.vistas_lock = threading.Lock()
        
        # Os drones que estão conectados nesta base
        self.drones: dict[str, InfoDrone] = {}
        self.drones_lock = threading.Lock()

    # --- Controle de Fila ---
    
    def inserir_na_fila(self, entrada: EntradaFila):
        """Coloca a missão nova na fila e já arruma tudo por ordem de prioridade."""
        with self.fila_lock:
            self.fila.append(entrada)
            self.fila.sort(key=lambda e: e.chave_ordenacao())

    def verificar_e_registrar_vista(self, id_requisicao: str) -> bool:
        """Checa se essa missão é repetida. Se for novidade, anota que já viu."""
        with self.vistas_lock:
            if id_requisicao in self.requisicoes_vistas:
                return False
            self.requisicoes_vistas.add(id_requisicao)
            return True

    def remover_vista(self, id_requisicao: str):
        """Apaga a missão do histórico. Útil quando um drone cai e a missão precisa 'voltar no tempo'."""
        with self.vistas_lock:
            self.requisicoes_vistas.discard(id_requisicao)

    def marcar_aceita(self, id_requisicao: str) -> bool:
        """
        Tenta pegar a missão para esta base. 
        Retorna True se conseguiu pegar. Retorna False se outra base já pegou antes.
        """
        with self.fila_lock:
            for entrada in self.fila:
                if entrada.id_requisicao == id_requisicao:
                    if entrada.status == StatusRequisicao.PENDENTE.value:
                        entrada.status = StatusRequisicao.ACEITA.value
                        return True
                    return False
        return False

    def marcar_concluida(self, id_requisicao: str):
        """Avisa que o drone terminou e muda a missão para concluída."""
        with self.fila_lock:
            for entrada in self.fila:
                if entrada.id_requisicao == id_requisicao:
                    entrada.status = StatusRequisicao.CONCLUIDA.value
                    return

    def status_requisicao(self, id_requisicao: str) -> Optional[str]:
        """Pergunta como está a missão agora (PENDENTE, ACEITA ou CONCLUIDA)."""
        with self.fila_lock:
            for entrada in self.fila:
                if entrada.id_requisicao == id_requisicao:
                    return entrada.status
        return None
        
    def obter_entrada(self, id_requisicao: str) -> Optional[EntradaFila]:
        """Pega todos os detalhes de uma missão pelo ID dela."""
        with self.fila_lock:
            return next((e for e in self.fila if e.id_requisicao == id_requisicao), None)

    def obter_pendentes(self) -> list[EntradaFila]:
        """Lista todas as missões que estão mofando na fila esperando um drone."""
        with self.fila_lock:
            return [e for e in self.fila if e.status == StatusRequisicao.PENDENTE.value]

    # --- Controle de Drones ---
    
    def drone_livre(self) -> Optional[InfoDrone]:
        """Procura e devolve o primeiro drone que estiver parado (LIVRE)."""
        with self.drones_lock:
            for info in self.drones.values():
                if info.estado == EstadoDrone.LIVRE.value:
                    return info
        return None

    def ocupar_drone(self, drone_id: str, id_requisicao: str):
        """Avisa que o drone não está mais livre e amarra o nome dele na missão."""
        with self.drones_lock:
            if drone_id in self.drones:
                self.drones[drone_id].estado = EstadoDrone.OCUPADO.value
                self.drones[drone_id].id_requisicao_atual = id_requisicao

    def atualizar_estado_drone(self, drone_id: str, estado: str):
        """Atualiza a 'ficha' do drone quando ele manda um sinal de vida (heartbeat)."""
        with self.drones_lock:
            if drone_id in self.drones:
                self.drones[drone_id].estado = estado
                self.drones[drone_id].ultimo_heartbeat = time.time()
                self.drones[drone_id].falhas_heartbeat = 0
                
                # Se o drone avisar que ficou livre, desvincula ele da missão antiga
                if estado == EstadoDrone.LIVRE.value:
                    self.drones[drone_id].id_requisicao_atual = None

    def registrar_drone(self, drone_id: str, porta_tcp: int, estado: str = EstadoDrone.LIVRE.value):
        """Cadastra um drone novato ou atualiza os dados de um drone que reiniciou."""
        with self.drones_lock:
            if drone_id not in self.drones:
                self.drones[drone_id] = InfoDrone(drone_id=drone_id, porta_tcp=porta_tcp, estado=estado)
            else:
                self.drones[drone_id].porta_tcp = porta_tcp
                self.drones[drone_id].estado = estado