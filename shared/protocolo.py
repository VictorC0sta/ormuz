import socket
import struct
import json
import logging
from typing import Optional
import os

logger = logging.getLogger(__name__)

# ── Constantes de rede ───────────────────────────────────────────────────────
BUFFER_UDP  = 4096      
TIMEOUT_TCP = 5.0       

def tcp_enviar(host: str, porta: int, payload: dict) -> bool:
    try:
        dados = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        header = struct.pack(">I", len(dados))

        with socket.create_connection((host, porta), timeout=TIMEOUT_TCP) as s:
            s.sendall(header + dados)

        # Lazy formatting: use %s e passe as variáveis como argumentos
        logger.debug("[TCP] Enviado para %s:%s — %d bytes", host, porta, len(dados))
        return True

    except (ConnectionRefusedError, TimeoutError, OSError) as e:
        logger.warning("[TCP] Falha ao enviar para %s:%s — %s", host, porta, e)
        return False


def tcp_broadcast(destinos: list[tuple[str, int]], payload: dict) -> dict[str, bool]:
    resultados = {}
    for host, porta in destinos:
        chave = f"{host}:{porta}"
        resultados[chave] = tcp_enviar(host, porta, payload)
    return resultados


# ── TCP — Recebimento ────────────────────────────────────────────────────────

def tcp_receber_completo(conn: socket.socket) -> Optional[dict]:
    try:
        header = _receber_exato(conn, 4)
        if not header:
            return None

        tamanho = struct.unpack(">I", header)[0]

        dados = _receber_exato(conn, tamanho)
        if not dados:
            return None

        return json.loads(dados.decode("utf-8"))

    except (json.JSONDecodeError, struct.error, OSError) as e:
        logger.warning("[TCP] Erro ao receber mensagem — %s", e)
        return None


def _receber_exato(conn: socket.socket, n: int) -> Optional[bytes]:
    buffer = b""
    while len(buffer) < n:
        chunk = conn.recv(n - len(buffer))
        if not chunk:
            return None
        buffer += chunk
    return buffer


# ── TCP — Servidor simples ───────────────────────────────────────────────────

def criar_servidor_tcp(porta: int, backlog: int = 10) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", porta))
    s.listen(backlog)
    logger.info("[TCP] Servidor ouvindo na porta %d", porta)
    return s


# ── UDP — Envio ──────────────────────────────────────────────────────────────

def udp_enviar(host: str, porta: int, payload: dict) -> bool:
    try:
        dados = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.sendto(dados, (host, porta))
        return True
    except OSError as e:
        logger.warning("[UDP] Falha ao enviar para %s:%s — %s", host, porta, e)
        return False


# ── UDP — Servidor simples ────────────────────────────────────────────────────

def criar_servidor_udp(porta: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", porta))
    logger.info("[UDP] Servidor ouvindo na porta %d", porta)
    return s

def notificar_monitor(evento: dict):
    """Envia um evento para a interface gráfica (Pygame) via UDP."""
    # O IP do computador que estará rodando a janela do Pygame
    ip_monitor = os.environ.get("IP_MONITOR", "127.0.0.1") 
    porta_monitor = 8000
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(evento).encode("utf-8"), (ip_monitor, porta_monitor))
        sock.close()
    except Exception:
        pass # Se o monitor não estiver rodando, ignora silenciosamente