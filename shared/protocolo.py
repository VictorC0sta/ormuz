"""
protocolo.py — Camada de rede (Ormuz Command Center).

Implementa comunicação TCP (garantida) usando framing length-prefix (4 bytes),
e UDP (telemetria/heartbeats) em modo fire-and-forget.
"""

import socket
import struct
import json
import logging
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time

logger = logging.getLogger(__name__)

# ── Constantes de rede ───────────────────────────────────────────────────────
BUFFER_UDP  = 4096
TIMEOUT_TCP = 2.0  # Timeout curto para evitar bloqueios no broadcast


# ── TCP — Envio ──────────────────────────────────────────────────────────────

def tcp_enviar(host: str, porta: int, payload: dict, max_tentativas: int = 3) -> bool:
    """
    Envia payload JSON via TCP prefixado com seu tamanho (4 bytes).
    Implementa lógica de RETRANSMISSÃO (Retry) em caso de falha de rede.
    """
    dados = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    header = struct.pack(">I", len(dados))

    for tentativa in range(1, max_tentativas + 1):
        try:
            with socket.create_connection((host, porta), timeout=TIMEOUT_TCP) as s:
                s.sendall(header + dados)
            
            logger.debug("[TCP] Enviado para %s:%s — %d bytes (Tentativa %d)", host, porta, len(dados), tentativa)
            return True

        except (ConnectionRefusedError, TimeoutError, OSError) as e:
            if tentativa < max_tentativas:
                logger.warning("[TCP] Falha ao enviar para %s:%s. Retentando (%d/%d)...", host, porta, tentativa, max_tentativas)
                time.sleep(0.5) # Pausa meio segundo antes de tentar de novo
            else:
                logger.error("[TCP] Falha definitiva ao enviar para %s:%s após %d tentativas — %s", host, porta, max_tentativas, e)
                return False


def tcp_broadcast(destinos: list[tuple[str, int]], payload: dict) -> dict[str, bool]:
    """Envia payload em paralelo para vários destinos TCP."""
    resultados: dict[str, bool] = {}

    with ThreadPoolExecutor(max_workers=len(destinos), thread_name_prefix="broadcast") as pool:
        futuros = {
            pool.submit(tcp_enviar, host, porta, payload): f"{host}:{porta}"
            for host, porta in destinos
        }
        for futuro in as_completed(futuros, timeout=TIMEOUT_TCP + 1):
            chave = futuros[futuro]
            try:
                resultados[chave] = futuro.result()
            except Exception as e:
                logger.warning("[TCP broadcast] Exceção para %s: %s", chave, e)
                resultados[chave] = False

    # Preenche como False os destinos que não responderam a tempo
    for host, porta in destinos:
        chave = f"{host}:{porta}"
        if chave not in resultados:
            resultados[chave] = False

    return resultados


# ── TCP — Recebimento ────────────────────────────────────────────────────────

def tcp_receber_completo(conn: socket.socket) -> Optional[dict]:
    """Lê prefixo de 4 bytes e retorna o payload JSON desserializado."""
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
    """Lê exatamente n bytes do socket."""
    buffer = b""
    while len(buffer) < n:
        chunk = conn.recv(n - len(buffer))
        if not chunk:
            return None
        buffer += chunk
    return buffer


# ── TCP — Servidor ───────────────────────────────────────────────────────────

def criar_servidor_tcp(porta: int, backlog: int = 10) -> socket.socket:
    """Cria e retorna um socket TCP passivo configurado para reuso imediato."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", porta))
    s.listen(backlog)
    logger.info("[TCP] Servidor ouvindo na porta %d", porta)
    return s


# ── UDP — Envio ──────────────────────────────────────────────────────────────

def udp_enviar(host: str, porta: int, payload: dict) -> bool:
    """Envia payload JSON em um único datagrama UDP."""
    try:
        dados = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.sendto(dados, (host, porta))
        return True
    except OSError as e:
        logger.warning("[UDP] Falha ao enviar para %s:%s — %s", host, porta, e)
        return False


# ── UDP — Servidor ────────────────────────────────────────────────────────────

def criar_servidor_udp(porta: int) -> socket.socket:
    """Cria e retorna um socket UDP passivo configurado para reuso imediato."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", porta))
    logger.info("[UDP] Servidor ouvindo na porta %d", porta)
    return s


# ── Monitor ───────────────────────────────────────────────────────────────────

def notificar_monitor(evento: dict) -> None:
    """Envia evento ao painel web via UDP ignorando silenciosamente as falhas."""
    ip_monitor = os.environ.get("IP_MONITOR", "127.0.0.1")
    porta_monitor = 8000
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(evento).encode("utf-8"), (ip_monitor, porta_monitor))
        sock.close()
    except Exception:
        pass  # Evita que o sistema trave caso o monitor esteja offline