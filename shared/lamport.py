import threading

class LamportClock:
    def __init__(self):
        self._clock = 0
        self._lock  = threading.Lock()

    def incrementar(self) -> int:
        with self._lock:
            self._clock += 1
            return self._clock

    def atualizar(self, recebido: int) -> int:
        with self._lock:
            self._clock = max(self._clock, recebido) + 1
            return self._clock

    @property
    def valor(self) -> int:
        with self._lock:
            return self._clock