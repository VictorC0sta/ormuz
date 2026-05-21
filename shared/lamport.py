import threading

class LamportClock:
    """
    Contador lógico usado para saber a ordem em que os eventos aconteceram.
    Como os relógios dos computadores nunca são exatamente iguais, 
    o sistema usa esse número para desempatar quem pediu o drone primeiro.
    """
    def __init__(self):
        self._clock = 0  # O tempo lógico sempre começa no zero
        self._lock  = threading.Lock()  # Trava de segurança para não embolar se duas threads mexerem juntas

    def incrementar(self) -> int:
        """Soma 1 no relógio. Usado toda vez que esta máquina cria e envia uma mensagem nova."""
        with self._lock:
            self._clock += 1
            return self._clock

    def atualizar(self, recebido: int) -> int:
        """
        Atualiza o relógio ao receber uma mensagem de fora. 
        Pega o maior tempo (o local ou o da mensagem que chegou) e soma 1.
        """
        with self._lock:
            self._clock = max(self._clock, recebido) + 1
            return self._clock

    @property
    def valor(self) -> int:
        """Apenas lê o número atual do relógio com segurança."""
        with self._lock:
            return self._clock