# Ormuz Command Center
### Infraestrutura Distribuída para Coordenação de Drones Autônomos de Monitoramento Marítimo
*Disciplina TEC502 — Sistemas Distribuídos · UEFS*

---

## Visão Geral

O sistema monitora o Estreito de Ormuz dividindo a área operacional em **8 setores marítimos**, cada um com um sensor e um broker próprio. Uma frota de drones autônomos é compartilhada entre **4 bases** (Norte, Sul, Leste, Oeste), que competem de forma coordenada para atender as ocorrências reportadas pelos sensores.

O objetivo central é garantir que nenhum drone seja despachado duas vezes para a mesma ocorrência, que requisições críticas sejam priorizadas e que o sistema continue operando mesmo quando componentes falham.

---

## Arquitetura

O sistema segue o estilo **broker distribuído sem ponto central de falha**. Não existe um coordenador único — cada base possui sua própria cópia da fila e toma decisões de forma autônoma usando um mecanismo de exclusão mútua por timeout diferenciado.

```
[Sensor S1..S8]
      │ TCP (alerta)
      ▼
[Broker de Setor S1..S8]
      │ TCP broadcast simultâneo
      ▼
[Base NORTE] [Base SUL] [Base LESTE] [Base OESTE]
      │              │
      │ TCP broadcast de ACEITE (cancela as outras)
      │
      ▼
[Drone despachado via TCP]
      │ UDP heartbeat periódico
      ▼
[Base monitora e detecta falha]
```

Cada camada roda isolada em contêineres Docker, em computadores distintos no laboratório.

---

## Componentes

### Sensor (`sensor/sensor.py`)
Simula um sensor naval que gera ocorrências aleatórias em intervalos configuráveis. Sorteia o tipo de ocorrência com pesos (eventos críticos são raros, anomalias menores são frequentes) e envia um alerta via TCP para o broker do seu setor.

Não possui interface gráfica — opera de forma autônoma para simular carga real no sistema.

### Broker de Setor (`setor/broker_setor.py`)
Recebe alertas dos sensores, incrementa o relógio de Lamport e faz **broadcast simultâneo** para as 4 bases usando `ThreadPoolExecutor`. Cada setor tem sua própria ordem de prioridade de bases, definida em `config/prioridade_tabela.json`.

### Broker de Base (`base/broker.py`)
É o componente mais complexo do sistema. Ao receber uma requisição:
1. Registra na fila replicada local com o timestamp de Lamport.
2. Aguarda um timeout proporcional à sua posição de prioridade para aquele setor.
3. Se ainda pendente após o timeout, verifica se tem drone livre e tenta aceitar.
4. Ao aceitar, faz broadcast de ACEITE para as outras bases cancelarem seus timers.
5. Despacha o drone via TCP com os dados da missão.

Também monitora os heartbeats dos drones e detecta falhas por ausência de sinal.

### Drone (`drone/drone.py`)
Ao iniciar, registra-se na sua base de origem via TCP. Mantém um loop de heartbeat UDP periódico informando seu estado. Ao receber uma missão, simula a duração com `time.sleep` e ao concluir envia uma mensagem TCP de volta à base liberando-se para novas missões.

### Monitor (`monitor/index.html` + `monitor/monitor_bridge.py`)
Painel web que visualiza o estado do sistema em tempo real. O `monitor_bridge.py` recebe eventos UDP na porta 8000 (enviados pelas bases via `notificar_monitor()`) e os repassa via WebSocket para o navegador.

---

## Protocolos de Comunicação

### Por que TCP para mensagens críticas?
Alertas, requisições, aceites e missões usam TCP porque a entrega garantida e a detecção de falha na conexão são essenciais — perder uma requisição significa uma ocorrência não atendida. O protocolo usa **length-prefixing** (4 bytes big-endian seguidos do payload JSON) para delimitar mensagens no stream TCP e evitar o problema clássico de framing.

### Por que UDP para heartbeats?
Os heartbeats dos drones são enviados a cada 3 segundos. A perda ocasional de um pacote é tolerável — o sistema só marca o drone como perdido após `HEARTBEAT_MAX_FALHAS=3` falhas consecutivas. UDP elimina o overhead de conexão para mensagens de alta frequência e baixa criticidade.

### Tabela de APIs

| Fluxo | Protocolo | Mensagem | Campos principais |
|---|---|---|---|
| Sensor → Broker Setor | TCP | `MensagemAlerta` | setor_id, tipo_ocorrencia, criticidade |
| Broker Setor → Bases | TCP broadcast | `MensagemRequisicao` | id_requisicao, id_setor, timestamp_logico, criticidade |
| Base → Outras Bases | TCP broadcast | ACEITE | id_requisicao, base_id, drone_id, timestamp_logico |
| Base → Drone | TCP | MISSAO | id_requisicao, setor_id, criticidade, tipo_ocorrencia |
| Drone → Base (periódico) | UDP | `MensagemHeartbeat` | drone_id, estado, id_requisicao_atual |
| Drone → Base (conclusão) | TCP | HEARTBEAT + missao_concluida | drone_id, estado=LIVRE, missao_concluida |
| Base/Setor → Monitor | UDP | evento | tipo, base, drone, setor |

---

## Exclusão Mútua Distribuída

O sistema usa um mecanismo de **timeout diferenciado por prioridade**, inspirado no conceito de janelas de tempo do protocolo TDMA. Cada base possui uma posição de prioridade para cada setor (definida em `prioridade_tabela.json`):

- Posição 1 (prioridade máxima): timeout = 0ms — tenta aceitar imediatamente
- Posição 2: timeout = 200ms
- Posição 3: timeout = 400ms
- Posição 4: timeout = 600ms

Quando a base de maior prioridade aceita e faz broadcast do ACEITE, as outras bases cancelam seus timers e descartam a requisição. O status `PENDENTE → ACEITA` é marcado atomicamente com lock, e o ID da requisição é registrado em um set de "vistos" para evitar processamento duplicado.

---

## Priorização de Requisições

A fila de cada base é ordenada por três critérios em cascata:

1. **Criticidade** — CRITICA (peso 3) > ALTA (peso 2) > BAIXA (peso 1)
2. **Timestamp de Lamport** — menor valor = chegou primeiro logicamente
3. **ID do setor** — desempate lexicográfico

O relógio de Lamport (`shared/lamport.py`) é atualizado a cada mensagem recebida com `max(local, recebido) + 1`, garantindo ordenação causal entre eventos distribuídos.

---

## Tolerância a Falhas

### Falha de drone
O `_monitor_heartbeat()` roda em thread dedicada e verifica a cada ciclo se algum drone ultrapassou `HEARTBEAT_TIMEOUT_S` sem sinal. Ao detectar:
1. Drone é marcado como `PERDIDO`.
2. A requisição em curso volta ao status `PENDENTE` na fila local.
3. Um broadcast de `REEMISSAO` é enviado para as outras bases recolocarem a requisição em suas filas.
4. Timers são reiniciados para que a requisição seja reassociada.

### Falha de broker de setor
Se um broker de setor cair, os outros 7 setores continuam operando normalmente — não há dependência entre eles. O sensor daquele setor tentará no próximo ciclo e falhará silenciosamente até o broker voltar (`restart: unless-stopped` no compose).

### Falha de base
As outras 3 bases continuam operando. A base que falhou não cancela timers nas demais — as outras assumem normalmente após seus timeouts.

---

## Como Executar

### Pré-requisitos
- Docker e Docker Compose instalados em todos os PCs
- Arquivo `.env` copiado para todos os PCs (mesmo conteúdo)
- IPs dos PCs preenchidos no `.env`

### Configurar o `.env`
```env
IP_PC_SETORES=<IP do PC 1>
IP_PC_BASES=<IP do PC 2>
IP_PC_DRONES=<IP do PC 3>
IP_PC_SENSORES=<IP do PC 3>
IP_MONITOR=<IP do PC que exibirá o monitor>
```

### PC 1 — Brokers de Setor
```bash
docker compose -f docker-compose.setores.yml up --build
```
Sobe 8 brokers (S1 a S8) nas portas 5051–5058.

### PC 2 — Brokers de Base
```bash
docker compose -f docker-compose.bases.yml up --build
```
Sobe 4 bases (Norte, Sul, Leste, Oeste) nas portas TCP 6001–6004 e UDP 6101–6104.

### PC 3 — Drones e Sensores
```bash
docker compose -f docker-compose.drones.yml up --build
docker compose -f docker-compose.sensores.yml up --build
```

### Monitor (qualquer PC)
```bash
pip install websockets
python monitor/monitor_bridge.py
# Abra monitor/index.html no navegador
```

---

## Teste de Resiliência

### Teste 1 — Falha de broker de setor
```bash
# Derruba o broker do setor S3
docker stop broker_s3

# Os setores S1, S2, S4..S8 continuam gerando e enviando alertas normalmente
# Verificar nos logs das bases que requisições de outros setores seguem sendo atendidas
docker logs base_norte --follow
```
**Resultado esperado:** nenhuma interrupção nos demais setores. S3 volta automaticamente ao reiniciar o container.

### Teste 2 — Falha de drone em missão
```bash
# Identifique o drone em missão nos logs
docker logs base_norte | grep "despachado"

# Derruba o container do drone
docker stop drone_norte   # ou drone_sul, drone_leste, drone_oeste

# Aguarde HEARTBEAT_TIMEOUT segundos (padrão: 12s)
# A base detecta a perda e faz broadcast de REEMISSAO
docker logs base_norte | grep "PERDIDO"
docker logs base_sul | grep "REEMISSAO"
```
**Resultado esperado:** a requisição volta à fila e outro drone livre a assume.

### Teste 3 — Carga simultânea
```bash
# No monitor web (index.html), aba "Stress Test"
# Configure: 50 alertas, taxa 10/s, distribuição mista
# Clique "Iniciar Bombardeamento"
# Observe zero duplicatas na fila e priorização correta
```

---

## Estrutura de Pastas

```
.
├── base/
│   ├── fila_replicada.py
│   └── prioridade.py
├── config/
│   └── prioridade_tabela.json
├── docker/
│   ├── .env
│   ├── docker-compose.bases.yml
│   ├── docker-compose.drones.yml
│   ├── docker-compose.sensores.yml
│   └── docker-compose.setores.yml
├── drone/
│   ├── dockerfile
│   ├── drone.py
│   └── test_drone.py
├── monitor/
│   ├── index.html
│   └── monitor_bridge.py
├── sensor/
│   ├── dockerfile
│   └── sensor.py
├── setor/
│   ├── broker_setor.py
│   └── dockerfile
└── shared/
    ├── constantes.py
    ├── lamport.py
    ├── mensagens.py
    └── protocolo.py
```

---

## Variáveis de Ambiente Relevantes

| Variável | Padrão | Descrição |
|---|---|---|
| `HEARTBEAT_INTERVALO` | 3s | Frequência do heartbeat UDP do drone |
| `HEARTBEAT_MAX_FALHAS` | 3 | Falhas consecutivas para marcar drone como perdido |
| `HEARTBEAT_TIMEOUT` | 12s | Tempo máximo sem heartbeat antes de declarar perda |
| `MISSAO_DURACAO_MIN` | 8s | Duração mínima de uma missão simulada |
| `MISSAO_DURACAO_MAX` | 20s | Duração máxima de uma missão simulada |
| `TIMEOUT_BASE_2` | 200ms | Timeout da base em 2ª prioridade |
| `TIMEOUT_BASE_3` | 400ms | Timeout da base em 3ª prioridade |
| `TIMEOUT_BASE_4` | 600ms | Timeout da base em 4ª prioridade |
