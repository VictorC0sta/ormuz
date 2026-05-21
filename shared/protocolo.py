"""
protocolo.py — Camada de rede (Ormuz Command Center).

Aqui fica toda a comunicação entre as máquinas.
- Usamos TCP para mensagens que não podem ser perdidas de jeito nenhum.
- Usamos UDP para mensagens rápidas (como o heartbeat) onde perder uma ou outra não tem problema.
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
TIMEOUT_TCP = 2.0  # Tempo máximo esperando uma resposta antes de desistir


# ── TCP — Envio ──────────────────────────────────────────────────────────────

def tcp_enviar(host: str, porta: int, payload: dict, max_tentativas: int = 3) -> bool:
    """
    Envia uma mensagem segura via TCP.
    Se a rede piscar ou falhar, ele tenta enviar de novo automaticamente.
    """
    dados = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    # Coloca o tamanho da mensagem no começo (4 bytes) para quem receber saber o tamanho exato
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
    """Envia a mesma mensagem para várias máquinas ao mesmo tempo (em paralelo)."""
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

    # Se alguma máquina não respondeu, marca como False (falhou)
    for host, porta in destinos:
        chave = f"{host}:{porta}"
        if chave not in resultados:
            resultados[chave] = False

    return resultados


# ── TCP — Recebimento ────────────────────────────────────────────────────────

def tcp_receber_completo(conn: socket.socket) -> Optional[dict]:
    """Lê a mensagem que chegou da rede e converte de volta para um dicionário Python."""
    try:
        # Lê os 4 primeiros bytes para descobrir o tamanho da mensagem
        header = _receber_exato(conn, 4)
        if not header:
            return None

        tamanho = struct.unpack(">I", header)[0]
        # Sabendo o tamanho, lê o resto da mensagem exatamente
        dados = _receber_exato(conn, tamanho)
        if not dados:
            return None

        return json.loads(dados.decode("utf-8"))

    except (json.JSONDecodeError, struct.error, OSError) as e:
        logger.warning("[TCP] Erro ao receber mensagem — %s", e)
        return None


def _receber_exato(conn: socket.socket, n: int) -> Optional[bytes]:
    """Garante que leu a quantidade exata de pedacinhos (bytes) do pacote de rede."""
    buffer = b""
    while len(buffer) < n:
        chunk = conn.recv(n - len(buffer))
        if not chunk:
            return None
        buffer += chunk
    return buffer


# ── TCP — Servidor ───────────────────────────────────────────────────────────

def criar_servidor_tcp(porta: int, backlog: int = 10) -> socket.socket:
    """Abre uma 'porta' no computador para ficar escutando conexões TCP chegando."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Permite reiniciar rápido sem prender a porta
    s.bind(("0.0.0.0", porta))
    s.listen(backlog)
    logger.info("[TCP] Servidor ouvindo na porta %d", porta)
    return s


# ── UDP — Envio ──────────────────────────────────────────────────────────────

def udp_enviar(host: str, porta: int, payload: dict) -> bool:
    """Envia uma mensagem rápida (sem garantia de entrega) num pacote único (UDP)."""
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
    """Abre uma 'porta' no computador para ficar escutando pacotes UDP chegando."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", porta))
    logger.info("[UDP] Servidor ouvindo na porta %d", porta)
    return s


# ── Monitor ───────────────────────────────────────────────────────────────────

def notificar_monitor(evento: dict) -> None:
    """
    Avisa o painel web (a tela) sobre o que está acontecendo.
    Se o painel estiver fechado ou com problema, ele ignora silenciosamente para não travar o sistema inteiro.
    """
    ip_monitor = os.environ.get("IP_MONITOR", "127.0.0.1")
    porta_monitor = 8000
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(evento).encode("utf-8"), (ip_monitor, porta_monitor))
        sock.close()
    except Exception:
        pass  # Evita que o sistema trave caso o monitor não esteja rodando