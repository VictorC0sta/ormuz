import asyncio
import websockets
import socket
import json

async def main():
    clients = set()
    
    # Configura o socket UDP para escutar os alertas (porta 8000 igual ao pygame antigo)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 8000))
    sock.setblocking(False)

    # Função que gerencia os navegadores conectados
    async def ws_handler(ws):
        clients.add(ws)
        try:
            async for _ in ws: pass
        finally: 
            clients.discard(ws)

    # Loop infinito que pega pacotes UDP e repassa para o HTML
    async def udp_loop():
        loop = asyncio.get_event_loop()
        while True:
            data = await loop.sock_recv(sock, 4096)
            try:
                # Opcional: printar no terminal só pra você ver que chegou algo
                print(f"Pacote recebido: {data.decode('utf-8')}") 
                evt = json.loads(data)
                for ws in set(clients):
                    try: 
                        await ws.send(json.dumps(evt))
                    except: 
                        clients.discard(ws)
            except Exception as e:
                print("Erro ao processar pacote:", e)

    # Sobe o servidor WebSocket na porta 8001
    srv = await websockets.serve(ws_handler, "0.0.0.0", 8001)
    print("Bridge Ativa: Escutando UDP 8000 -> Repassando para WebSocket 8001")
    
    await asyncio.gather(srv.wait_closed(), udp_loop())

if __name__ == "__main__":
    asyncio.run(main())