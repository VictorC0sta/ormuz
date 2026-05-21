import os
import json
import sys

# Adiciona a pasta "shared" no caminho para conseguir importar os módulos comuns
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

# pylint: disable=import-error, wrong-import-position
from constantes import TIMEOUT_PRIORIDADE_MS

class GerenciadorPrioridade:
    """
    Gerencia o tempo de espera (timeout) da base atual.
    A base que tem maior prioridade num setor não espera nada (0 ms), 
    as outras esperam um tempo para não tentar pegar a missão ao mesmo tempo.
    """
    def __init__(self, base_id: str):
        self.base_id = base_id
        
        # Carrega as regras de quem manda em qual setor
        self._carregar_tabela()
        
        # Lê os tempos de espera do arquivo .env ou usa os valores padrão
        _t2 = int(os.environ.get("TIMEOUT_BASE_2", str(TIMEOUT_PRIORIDADE_MS[2])))
        _t3 = int(os.environ.get("TIMEOUT_BASE_3", str(TIMEOUT_PRIORIDADE_MS[3])))
        _t4 = int(os.environ.get("TIMEOUT_BASE_4", str(TIMEOUT_PRIORIDADE_MS[4])))
        
        # Posição 1 sempre espera 0. O resto espera o tempo configurado.
        self.timeout_ms_por_posicao = {1: 0, 2: _t2, 3: _t3, 4: _t4}

    def _carregar_tabela(self):
        """Lê o arquivo JSON que diz a ordem de prioridade de cada setor."""
        caminho_json = os.path.join(os.path.dirname(__file__), "..", "config", "prioridade_tabela.json")
        try:
            with open(caminho_json, "r", encoding="utf-8") as f:
                self.tabela_setores = json.load(f)
        except Exception as e:
            # Se der erro ao ler o arquivo, usa uma tabela vazia para não quebrar o sistema
            print(f"Erro ao carregar {caminho_json}: {e}. Usando fallback vazio.")
            self.tabela_setores = {}

    def timeout_para_setor(self, id_setor: str) -> float:
        """
        Descobre a posição da base atual para o setor pedido e 
        retorna o tempo de espera convertido em SEGUNDOS.
        """
        # Pega a ordem de prioridade do setor (ex: ["NORTE", "SUL", "LESTE", "OESTE"])
        ordem = self.tabela_setores.get(id_setor, ["NORTE", "SUL", "LESTE", "OESTE"])
        
        try:
            # Posição na lista (1º, 2º, 3º ou 4º lugar)
            posicao = ordem.index(self.base_id) + 1  
        except ValueError:
            # Se a base não estiver na lista do setor, joga para a última prioridade
            posicao = 4  

        # Pega o tempo em milissegundos e divide por 1000 para virar segundos (o timer exige em segundos)
        return self.timeout_ms_por_posicao.get(posicao, self.timeout_ms_por_posicao[4]) / 1000.0