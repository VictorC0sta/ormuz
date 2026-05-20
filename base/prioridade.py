import os
import json
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
# pylint: disable=import-error, wrong-import-position
from constantes import TIMEOUT_PRIORIDADE_MS

class GerenciadorPrioridade:
    def __init__(self, base_id: str):
        self.base_id = base_id
        self._carregar_tabela()
        
        # Lê os timeouts do ambiente (ou usa os padrões do shared/constantes.py)
        _t2 = int(os.environ.get("TIMEOUT_BASE_2", str(TIMEOUT_PRIORIDADE_MS[2])))
        _t3 = int(os.environ.get("TIMEOUT_BASE_3", str(TIMEOUT_PRIORIDADE_MS[3])))
        _t4 = int(os.environ.get("TIMEOUT_BASE_4", str(TIMEOUT_PRIORIDADE_MS[4])))
        
        self.timeout_ms_por_posicao = {1: 0, 2: _t2, 3: _t3, 4: _t4}

    def _carregar_tabela(self):
        caminho_json = os.path.join(os.path.dirname(__file__), "..", "config", "prioridade_tabela.json")
        try:
            with open(caminho_json, "r", encoding="utf-8") as f:
                self.tabela_setores = json.load(f)
        except Exception as e:
            print(f"Erro ao carregar {caminho_json}: {e}. Usando fallback vazio.")
            self.tabela_setores = {}

    def timeout_para_setor(self, id_setor: str) -> float:
        """Retorna o timeout em segundos que esta base deve esperar."""
        ordem = self.tabela_setores.get(id_setor, ["NORTE", "SUL", "LESTE", "OESTE"])
        try:
            posicao = ordem.index(self.base_id) + 1  # 1-based
        except ValueError:
            posicao = 4  # Não está na tabela → última prioridade

        return self.timeout_ms_por_posicao.get(posicao, self.timeout_ms_por_posicao[4]) / 1000.0