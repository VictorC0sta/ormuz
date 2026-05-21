"""
Testes unitários do Drone — sem dependências externas de rede.
Execute a partir da raiz do projeto: python -m pytest tests/test_drone.py -v
"""

import os
import sys
import threading
import unittest
from unittest.mock import patch, MagicMock

# 1. Pega o caminho absoluto da pasta onde este arquivo de teste está (ex: ormuz/tests/)
PASTA_ATUAL = os.path.dirname(os.path.abspath(__file__))
# 2. Volta uma pasta para achar a raiz do projeto (ex: ormuz/)
RAIZ_PROJETO = os.path.abspath(os.path.join(PASTA_ATUAL, ".."))

# 3. Adiciona a raiz do projeto no caminho de importação do Python
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)
    
# 4. Adiciona a pasta shared no caminho
PASTA_SHARED = os.path.join(RAIZ_PROJETO, "shared")
if PASTA_SHARED not in sys.path:
    sys.path.insert(0, PASTA_SHARED)
# -----------------------------------------

# Injeta variáveis de ambiente antes de importar os módulos
os.environ.setdefault("DRONE_ID", "drone-test-01")
os.environ.setdefault("BASE_ORIGEM", "NORTE")
os.environ.setdefault("IP_BASE", "127.0.0.1")

# pylint: disable=import-error, wrong-import-position
from constantes import EstadoDrone, TipoMensagem
from drone import drone  # <--- IMPORT CORRIGIDO: importa o módulo drone da pasta drone/


# ---------------------------------------------------------------------------
# Testes de Controle de Estado e Concorrência
# ---------------------------------------------------------------------------
class TestEstadoDrone(unittest.TestCase):

    def test_estado_inicial_livre(self):
        d = drone.Drone()
        self.assertTrue(d.livre)
        self.assertEqual(d.estado, EstadoDrone.LIVRE)
        self.assertIsNone(d.id_requisicao_atual)

    def test_ocupar_e_liberar(self):
        d = drone.Drone()
        
        d.ocupar("req-teste-123")
        self.assertFalse(d.livre)
        self.assertEqual(d.estado, EstadoDrone.OCUPADO)
        self.assertEqual(d.id_requisicao_atual, "req-teste-123")
        
        d.liberar()
        self.assertTrue(d.livre)
        self.assertEqual(d.estado, EstadoDrone.LIVRE)
        self.assertIsNone(d.id_requisicao_atual)

    def test_thread_safety_estado(self):
        """Múltiplas threads modificando o estado não devem causar inconsistência."""
        d = drone.Drone()
        erros = []

        def alterna():
            try:
                for i in range(100):
                    d.ocupar(f"req-{i}")
                    d.liberar()
            except Exception as e:
                erros.append(e)

        threads = [threading.Thread(target=alterna) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(erros, [])


# ---------------------------------------------------------------------------
# Testes de Lógica de Negócio (Mockando a Rede)
# ---------------------------------------------------------------------------
class TestDroneFuncoes(unittest.TestCase):

    def setUp(self):
        # Garante que o drone global comece livre antes de cada teste
        drone.drone.liberar()

    # O patch agora aponta corretamente para o módulo "drone.drone" (pasta.arquivo)
    @patch("drone.drone.tcp_enviar")
    def test_registrar_na_base_sucesso(self, mock_tcp_enviar):
        # Configura o mock para simular um envio bem-sucedido de primeira
        mock_tcp_enviar.return_value = True
        
        drone.registrar_na_base()
        
        mock_tcp_enviar.assert_called_once()
        args, _ = mock_tcp_enviar.call_args
        msg_enviada = args[2]  # O payload é o 3º argumento da tcp_enviar
        
        self.assertEqual(msg_enviada["drone_id"], "drone-test-01")
        self.assertEqual(msg_enviada["base_id"], "NORTE")

    @patch("drone.drone.time.sleep")
    @patch("drone.drone.tcp_enviar")
    def test_executar_missao(self, mock_tcp_enviar, mock_sleep):
        dados_missao = {
            "id_requisicao": "req-999",
            "setor_id": "S1",
            "tipo_ocorrencia": "embarcacao_deriva",
            "criticidade": "ALTA"
        }
        
        # Executa a função
        drone.executar_missao(dados_missao)
        
        # Verifica se o time.sleep foi chamado (simulando a duração da missão)
        mock_sleep.assert_called_once()
        
        # Verifica se o drone foi liberado ao final
        self.assertTrue(drone.drone.livre)
        
        # Verifica se a mensagem de conclusão (Heartbeat via TCP) foi enviada
        mock_tcp_enviar.assert_called_once()
        args, _ = mock_tcp_enviar.call_args
        msg_conclusao = args[2]
        
        self.assertEqual(msg_conclusao["tipo"], TipoMensagem.HEARTBEAT.value)
        self.assertEqual(msg_conclusao["missao_concluida"], "req-999")
        self.assertEqual(msg_conclusao["estado"], EstadoDrone.LIVRE.value)


if __name__ == "__main__":
    unittest.main(verbosity=2)