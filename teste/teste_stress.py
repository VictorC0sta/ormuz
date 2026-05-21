import socket
import json
import struct
import threading
import time
import os

# Aponta para o IP do PC que roda os setores (ajuste se necessario)
HOST = os.environ.get("IP_PC_SETORES", "127.0.0.1")
PORTA_SETOR = 5051  # Porta do Setor 1
TOTAL_REQUISICOES = 150

def enviar_alerta_tcp(id_teste):
    payload = {
        "sensor_id": f"SENSOR-STRESS-{id_teste}",
        "setor_id": "S1",
        "tipo_ocorrencia": "anomalia_menor",
        "criticidade": "BAIXA"
    }
    try:
        dados = json.dumps(payload).encode("utf-8")
        header = struct.pack(">I", len(dados))
        with socket.create_connection((HOST, PORTA_SETOR), timeout=2) as s:
            s.sendall(header + dados)
        return True
    except Exception:
        return False

def executar_teste_carga():
    print(f"Iniciando bombardeamento TCP: {TOTAL_REQUISICOES} requisicoes simultaneas para {HOST}:{PORTA_SETOR}...")
    inicio = time.time()
    
    sucessos = 0
    falhas = 0
    lock_contadores = threading.Lock()
    
    def worker(id_teste):
        nonlocal sucessos, falhas
        resultado = enviar_alerta_tcp(id_teste)
        with lock_contadores:
            if resultado:
                sucessos += 1
            else:
                falhas += 1

    threads = []
    for i in range(TOTAL_REQUISICOES):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)

    for t in threads:
        t.start()
        
    for t in threads:
        t.join()

    duracao = time.time() - inicio
    print("-" * 40)
    print("RESULTADOS DO TESTE DE CARGA (DESEMPENHO)")
    print("-" * 40)
    print(f"Tempo total: {duracao:.2f} segundos")
    print(f"Enviados com sucesso (TCP): {sucessos}")
    print(f"Falhas de conexao/Timeout: {falhas}")
    print(f"Taxa de transferencia: {TOTAL_REQUISICOES / duracao:.2f} requisicoes/segundo")
    print("Verifique os logs das Bases para confirmar o processamento sem duplicatas.")

if __name__ == "__main__":
    executar_teste_carga()