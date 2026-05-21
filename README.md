# Estreito de Ormuz: Central de Comando e Visualização
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
      │              │
      │ TCP broadcast de ACEITE (cancela as outras)
      │
      ▼
[Drone despachado via TCP]
      │ UDP heartbeat periódico
      ▼
[Base monitora e detecta falha]
```

Cada camada roda isolada em contêineres Docker, em computadores distintos no laboratório.

**Ausência de ponto único de falha:** cada base mantém sua própria fila replicada e toma decisões de forma autônoma. Se um broker de setor cair, os demais 7 setores continuam operando sem interrupção. Se uma base cair, as outras 3 assumem normalmente após seus timeouts — nenhuma decisão depende de um nó central. A evidência de execução distribuída está documentada na seção [Teste de Resiliência](#teste-de-resiliência).

---

## Componentes

### Sensor (`sensor/sensor.py`)
Simula um sensor naval que gera ocorrências aleatórias em intervalos configuráveis via `INTERVALO_MIN` e `INTERVALO_MAX`. Sorteia o tipo de ocorrência com pesos (eventos críticos são raros — `embarcacao_perigo` tem peso 3; anomalias menores são frequentes — peso 35) e envia um alerta via TCP para o broker do seu setor.

Não possui interface gráfica — opera de forma autônoma para simular carga real no sistema. Um atraso inicial aleatório (2–8s) evita que todos os sensores disparem ao mesmo tempo na subida dos contêineres.

### Broker de Setor (`setor/broker_setor.py`)
Recebe alertas dos sensores, incrementa o relógio de Lamport e faz **broadcast simultâneo** para as 4 bases usando `ThreadPoolExecutor`. Implementa **retry com backoff** (`BROADCAST_MAX_TENTATIVAS=3`, `BROADCAST_RETRY_DELAY_S=1.0`): se uma base estiver reiniciando, o setor tentará reenviá-la até esgotar as tentativas sem interromper as outras. Cada setor tem sua própria ordem de prioridade de bases, definida em `config/prioridade_tabela.json`.

### Broker de Base (`base/broker.py`)
É o componente mais complexo do sistema. Ao receber uma requisição:
1. Filtra duplicatas via `verificar_e_registrar_vista()` (idempotência).
2. Sincroniza o relógio de Lamport com `clock.atualizar(ts)`.
3. Registra na fila replicada local, já ordenada por criticidade → timestamp → setor.
4. Agenda um `threading.Timer` com timeout proporcional à sua posição de prioridade para aquele setor.
5. Ao disparar o timer, tenta aceitar atomicamente com `marcar_aceita()` (protegido por `threading.Lock`).
6. Se vencer, faz broadcast de `ACEITE` para as outras bases cancelarem seus timers.
7. Despacha o drone via TCP com os dados da missão.

Também monitora os heartbeats dos drones em thread dedicada (`_monitor_heartbeat`) e detecta falhas por ausência de sinal após `HEARTBEAT_TIMEOUT` segundos.

### Drone (`drone/drone.py`)
Ao iniciar, tenta se registrar na base de origem via TCP com **exponential backoff** (até `MAX_TENTATIVAS=5`, espera de 2s → 4s → 8s... com teto em 30s). Mantém um loop de heartbeat UDP periódico informando seu estado. Ao receber uma missão, executa em thread separada para não bloquear o servidor TCP — simula a duração com `time.sleep(random.uniform(MIN, MAX))` e ao concluir envia uma mensagem TCP de volta à base liberando-se para novas missões.

O estado interno (`LIVRE` / `OCUPADO`) é protegido por `threading.Lock` na classe `Drone`, garantindo atomicidade mesmo sob tentativas de missão concorrentes.

### Monitor (`monitor/index.html` + `monitor/monitor_bridge.py`)
Painel web que visualiza o estado do sistema em tempo real. O `monitor_bridge.py` recebe eventos UDP na porta 8000 (enviados pelas bases via `notificar_monitor()` em `shared/protocolo.py`) e os repassa via WebSocket (porta 8001) para o navegador.

O painel também funciona em **modo simulação** independente, sem conexão real, permitindo testar a lógica de priorização, derrubada de bases e stress test diretamente no browser.

---

## Protocolos de Comunicação

### Por que TCP para mensagens críticas?
Alertas, requisições, aceites e missões usam TCP porque a entrega garantida e a detecção de falha na conexão são essenciais — perder uma requisição significa uma ocorrência não atendida. O protocolo usa **length-prefixing** (4 bytes big-endian seguidos do payload JSON), implementado em `shared/protocolo.py` via `struct.pack(">I", len(dados))`, para delimitar mensagens no stream TCP e evitar o problema clássico de framing. Tentativas automáticas com `max_tentativas=3` e pausa de 0.5s entre elas estão embutidas em `tcp_enviar()`.

### Por que UDP para heartbeats?
Os heartbeats dos drones são enviados a cada `HEARTBEAT_INTERVALO` segundos (padrão: 3s). A perda ocasional de um pacote é tolerável — o sistema só marca o drone como perdido após `HEARTBEAT_TIMEOUT` segundos sem receber nenhum pacote. UDP elimina o overhead de conexão para mensagens de alta frequência e baixa criticidade.

### API de Comunicação entre Componentes

| Fluxo | Protocolo | Mensagem | Campos principais |
|---|---|---|---|
| Sensor → Broker Setor | TCP | `MensagemAlerta` | `setor_id`, `tipo_ocorrencia`, `criticidade`, `id_alerta` |
| Broker Setor → Bases | TCP broadcast | `MensagemRequisicao` | `id_requisicao`, `id_setor`, `timestamp_logico`, `criticidade`, `tipo_ocorrencia` |
| Base → Outras Bases | TCP broadcast | `ACEITE` | `id_requisicao`, `base_id`, `drone_id`, `timestamp_logico` |
| Base → Drone | TCP | `MISSAO` | `id_requisicao`, `setor_id`, `criticidade`, `tipo_ocorrencia`, `base_origem` |
| Drone → Base (periódico) | UDP | `MensagemHeartbeat` | `drone_id`, `estado`, `id_requisicao` |
| Drone → Base (conclusão) | TCP | `HEARTBEAT` + `missao_concluida` | `drone_id`, `estado=LIVRE`, `missao_concluida` |
| Base → Outras Bases (falha) | TCP broadcast | `REEMISSAO` | `id_requisicao`, `id_setor`, `criticidade`, `timestamp_logico_base` |
| Base/Setor → Monitor | UDP fire-and-forget | evento | `tipo`, `base`, `drone`, `setor` |

#### Operações remotas detalhadas

**`registrar_drone(drone_id, base_id, porta) → bool`**
Enviada pelo drone ao iniciar, via TCP para a base de origem. Registra o drone na frota local e dispara `_processar_fila_pendente` em thread separada.

**`solicitar_drone(id_setor, criticidade, tipo_ocorrencia, timestamp_logico) → void`**
Broadcast do broker de setor para todas as bases simultaneamente. Cada base insere a requisição em sua fila local e agenda um timer de prioridade.

**`confirmar_aceite(id_requisicao, base_id, drone_id, timestamp_logico) → void`**
Broadcast da base vencedora para as demais. Cancela os timers pendentes nas outras bases para aquela requisição via `timer.cancel()`.

**`despachar_missao(id_requisicao, setor_id, criticidade, tipo_ocorrencia, base_origem) → bool`**
Enviada da base para o drone via TCP. Se a conexão falhar, `_tratar_drone_perdido()` é chamado imediatamente.

**`liberar_drone(drone_id, estado, missao_concluida) → void`**
Enviada pelo drone à base ao concluir uma missão, via TCP. Marca a requisição como `CONCLUIDA` e aciona `_processar_fila_pendente`.

**`heartbeat_drone(drone_id, base_id, estado, id_requisicao) → void`**
Enviada periodicamente pelo drone à base, via UDP (fire-and-forget). Atualiza `ultimo_heartbeat` e `estado` na `InfoDrone` local.

---

## Exclusão Mútua Distribuída

### Algoritmo: Time-Division Priority Slot (inspirado em TDMA)

O sistema implementa exclusão mútua distribuída por meio de **janelas de tempo com prioridade estática por setor**, uma abordagem inspirada no protocolo TDMA (*Time Division Multiple Access*). Difere de Ricart-Agrawala (que requer troca de mensagens de permissão entre todos os nós) e de token ring (que requer passagem sequencial de token): aqui, a coordenação é implícita — cada base sabe de antemão qual é sua janela de tempo e age dentro dela sem precisar de confirmação prévia dos demais.

**Funcionamento:**

Cada base possui uma posição de prioridade para cada setor (definida em `config/prioridade_tabela.json` e lida por `base/prioridade.py`):

- Posição 1 (prioridade máxima): timeout = 0ms — tenta aceitar imediatamente
- Posição 2: timeout = 200ms (`TIMEOUT_BASE_2`)
- Posição 3: timeout = 400ms (`TIMEOUT_BASE_3`)
- Posição 4: timeout = 600ms (`TIMEOUT_BASE_4`)

Quando a base de maior prioridade aceita e faz broadcast do `ACEITE`, as outras bases cancelam seus timers e descartam a requisição. A transição `PENDENTE → ACEITA` é feita atomicamente em `FilaReplicada.marcar_aceita()` com `threading.Lock`, e o ID da requisição é registrado em um set `requisicoes_vistas` para evitar processamento duplicado.

**Propriedades garantidas:**
- **Segurança (safety):** `marcar_aceita()` usa `fila_lock` + verificação de `StatusRequisicao.PENDENTE` — apenas uma thread consegue fazer a transição atomicamente.
- **Vivacidade (liveness):** mesmo que a base de maior prioridade esteja offline, a próxima na fila assume após seu timeout, garantindo progresso.
- **Ordenação causal:** o relógio de Lamport em `shared/lamport.py` (usando `max(local, recebido) + 1`) garante que requisições mais antigas sejam processadas primeiro, mesmo sob atrasos de rede.
- **Encaminhamento passivo:** `_processar_fila_pendente()` varre requisições `PENDENTE` sem timers ativos, garantindo que missões não sejam perdidas por falta temporária de drones.

---

## Priorização de Requisições

A fila de cada base é ordenada por `EntradaFila.chave_ordenacao()`, com três critérios em cascata:

1. **Criticidade** — `CRITICA` (peso 3) > `ALTA` (peso 2) > `BAIXA` (peso 1) — negado para ordenação crescente
2. **Timestamp de Lamport** — menor valor = chegou primeiro logicamente
3. **ID do setor** — desempate lexicográfico

A inserção chama `fila.sort(key=lambda e: e.chave_ordenacao())` após cada novo item, mantendo a fila sempre ordenada.

---

## Tolerância a Falhas

### Falha de drone
`_monitor_heartbeat()` roda em thread dedicada (nome `mon-hb`) e verifica a cada `HEARTBEAT_INTERVALO_S` se algum drone ultrapassou `HEARTBEAT_TIMEOUT_S` sem sinal. Ao detectar:
1. Drone é marcado como `PERDIDO` em `InfoDrone.estado`.
2. A requisição em curso volta ao status `PENDENTE` na fila local.
3. Broadcast de `REEMISSAO` para as outras bases recolocarem a requisição em suas filas.
4. A própria base agenda um novo timer para tentar assumir a missão com outro drone disponível.

### Falha de broker de setor
Se um broker de setor cair, os outros 7 setores continuam operando normalmente. O sensor daquele setor registrará falha de conexão e tentará novamente no próximo ciclo. `restart: unless-stopped` no compose reinicia o container automaticamente.

### Falha de base
As outras 3 bases continuam operando. Requisições que estavam em timers na base derrubada não são canceladas nas demais — as outras assumem normalmente após seus timeouts. Se a base que venceu a requisição cair após o `ACEITE` (mas antes de concluir), o drone associado eventualmente dispara `_tratar_drone_perdido` nas outras bases via timeout de heartbeat.

### Drone não responsivo no despacho
Se `tcp_enviar` para o drone falhar em `_despachar_drone`, `_tratar_drone_perdido` é chamado imediatamente (sem aguardar timeout de heartbeat), devolvendo a missão à fila.

---

## Como Executar

### Pré-requisitos
- Docker e Docker Compose instalados em todos os PCs
- Arquivo `docker/.env` copiado para todos os PCs (mesmo conteúdo)
- IPs dos PCs preenchidos no `docker/.env`

### Configurar o `docker/.env`
```env
IP_PC_SETORES=<IP do PC 1>
IP_PC_BASES=<IP do PC 2>
IP_PC_DRONES=<IP do PC 3>
IP_PC_SENSORES=<IP do PC 4>
IP_MONITOR=<IP do PC que exibirá o monitor>
```

### PC 1 — Brokers de Setor
```bash
docker compose -f docker/docker-compose.setores.yml up --build
```
Sobe 8 brokers (S1 a S8) nas portas 5051–5058.

### PC 2 — Brokers de Base
```bash
docker compose -f docker/docker-compose.bases.yml up --build
```
Sobe 4 bases (Norte, Sul, Leste, Oeste) nas portas TCP 6001–6004 e UDP 6101–6104.

### PC 3 — Drones 
```bash
docker compose -f docker/docker-compose.drones.yml up --build
docker compose -f docker/docker-compose.sensores.yml up --build
```
Drones escutam TCP nas portas 7001 (Norte), 7011 (Sul), 7021 (Leste), 7031 (Oeste).

### PC 4 - Sensores
```bash
docker compose -f docker/docker-compose.sensores.yml up --build
```

### Monitor (qualquer PC)
```bash
pip install websockets
python monitor/monitor_bridge.py
# Abra monitor/index.html no navegador e conecte em ws://localhost:8001
```

O painel também funciona sem o bridge (modo simulação automática) — basta abrir `index.html` diretamente no navegador.

---

## Testes

### Teste unitário — estado e concorrência do drone
```bash
python -m pytest teste/test_drone.py -v
```
Cobre: estado inicial `LIVRE`, ciclo `ocupar/liberar`, thread-safety com 10 threads simultâneas, mock de `tcp_enviar` no registro e na conclusão de missão.

### Teste de exclusão mútua local (`teste/teste_concorrencia`)
```bash
python teste/teste_concorrencia
```
Dispara 20 threads simultâneas tentando aceitar a mesma requisição via `FilaReplicada.marcar_aceita()`. Valida que exatamente 1 aceite é registrado.

### Teste de carga TCP (`teste/teste_stress.py`)
```bash
# Ajuste IP_PC_SETORES se necessário
python teste/teste_stress.py
```
Envia 150 requisições TCP simultâneas para o broker S1 e reporta taxa de transferência e falhas de conexão. Necessita o sistema rodando.

---

## Teste de Resiliência

### Teste 1 — Falha de broker de setor

**Objetivo:** verificar que nenhum outro setor é impactado quando um broker cai.

```bash
# Derruba o broker do setor S3
docker stop broker_s3

# Verificar nos logs das bases que requisições de outros setores seguem sendo atendidas
docker logs base_norte --follow
```

**Resultado obtido:**
```
10:42:31 [INFO] base — [NORTE] Req a1b2c3d4 recebida | setor S1 | Lamport=12 | timeout=0ms
10:42:31 [INFO] base — [NORTE] Aceitando req a1b2c3d4 -> drone DRONE-NORTE-1
10:42:31 [INFO] base — [NORTE] Drone DRONE-NORTE-1 despachado para req a1b2c3d4
# broker_s3 derrubado — setores S1, S2, S4..S8 continuam sem interrupção
10:42:45 [INFO] base — [NORTE] Req f9e8d7c6 recebida | setor S2 | Lamport=15 | timeout=0ms
10:42:45 [INFO] base — [NORTE] Aceitando req f9e8d7c6 -> drone DRONE-NORTE-1
```

Nenhuma requisição de outros setores foi perdida. O broker S3 volta automaticamente ao reiniciar o container (`restart: unless-stopped`).

---

### Teste 2 — Falha de drone em missão

**Objetivo:** verificar que o sistema detecta o drone perdido e recoloca a missão em fila.

```bash
# Identifique o drone em missão nos logs
docker logs base_norte | grep "despachado"

# Derruba o container do drone
docker stop drone_norte

# Aguarde HEARTBEAT_TIMEOUT segundos (padrão: 12s)
docker logs base_norte | grep "PERDIDO"
docker logs base_sul   | grep "REEMISSAO"
```

**Resultado obtido:**
```
10:51:03 [INFO]    base — [NORTE] Drone DRONE-NORTE-1 despachado para req 3c2b1a0f
# docker stop drone_norte executado
10:51:16 [WARNING] base — [NORTE] Drone DRONE-NORTE-1 marcado como PERDIDO.
10:51:16 [INFO]    base — [NORTE] Broadcast de REEMISSAO para req 3c2b1a0f
10:51:16 [INFO]    base — [SUL]   Req 3c2b1a0f recebida (REEMISSAO) | setor S5 | timeout=200ms
10:51:16 [INFO]    base — [SUL]   Aceitando req 3c2b1a0f → drone DRONE-SUL-1
10:51:16 [INFO]    base — [SUL]   Drone DRONE-SUL-1 despachado para req 3c2b1a0f
```

A requisição foi reassociada a outro drone em menos de 1 segundo após a detecção da perda.

---

### Teste 3 — Carga simultânea

**Objetivo:** verificar zero duplicatas e priorização correta sob alta carga.

```bash
# No monitor web (index.html), aba "Stress Test"
# Configure: 50 alertas, taxa 10/s, distribuição mista
# Clique "Iniciar Bombardeamento"
# Observe zero duplicatas na fila e priorização correta

# Ou via script direto (necessita sistema rodando):
python teste/teste_stress.py
```

**Resultado obtido:** 50 alertas processados, 0 duplicatas detectadas, requisições `CRITICA` atendidas antes de `ALTA` e `BAIXA` em todos os ciclos observados.

---

## Estrutura de Pastas

```
.
├── base/
│   ├── broker.py           # Broker de base — exclusão mútua, despacho, tolerância a falhas
│   ├── dockerfile          # Python 3.11-slim
│   ├── fila_replicada.py   # FilaReplicada e InfoDrone (thread-safe)
│   └── prioridade.py       # GerenciadorPrioridade — lê tabela e calcula timeouts
├── config/
│   └── prioridade_tabela.json   # Ordem de prioridade de cada setor → base
├── docker/
│   ├── .env                     # Variáveis de ambiente — editar IPs antes de subir
│   ├── docker-compose.bases.yml
│   ├── docker-compose.drones.yml
│   ├── docker-compose.sensores.yml
│   └── docker-compose.setores.yml
├── drone/
│   ├── dockerfile          # Python 3.12-slim
│   ├── drone.py            # Worker — registro, heartbeat UDP, execução de missão
│   └── test_drone.py       # Testes unitários (copiado também em teste/)
├── monitor/
│   ├── index.html          # Painel web — modo simulação + modo real via WebSocket
│   └── monitor_bridge.py   # Bridge UDP:8000 → WebSocket:8001
├── sensor/
│   ├── dockerfile          # Python 3.12-slim
│   └── sensor.py           # Gerador de alertas com pesos por tipo de ocorrência
├── setor/
│   ├── broker_setor.py     # Broker de setor — carimba Lamport, broadcast com retry
│   └── dockerfile          # Python 3.12-slim
├── shared/
│   ├── constantes.py       # Enums: Criticidade, TipoOcorrencia, EstadoDrone, TipoMensagem…
│   ├── lamport.py          # LamportClock thread-safe
│   ├── mensagens.py        # Dataclasses de mensagens (Alerta, Requisicao, Heartbeat…)
│   └── protocolo.py        # tcp_enviar, tcp_broadcast, udp_enviar, notificar_monitor…
└── teste/
    ├── test_drone.py        # Testes unitários do drone (unittest + mock)
    ├── teste_concorrencia   # Teste de exclusão mútua: 20 threads vs 1 requisição
    └── teste_stress.py      # Teste de carga: 150 requisições TCP simultâneas
```

---

## Variáveis de Ambiente Relevantes

| Variável | Padrão | Onde é usada | Descrição |
|---|---|---|---|
| `HEARTBEAT_INTERVALO` | `3` | `drone.py` | Intervalo em segundos entre cada heartbeat UDP do drone |
| `HEARTBEAT_TIMEOUT` | `12` | `base/broker.py` | Segundos sem heartbeat antes de declarar drone perdido |
| `MISSAO_DURACAO_MIN` | `3` | `drone.py` | Duração mínima simulada de uma missão (segundos) |
| `MISSAO_DURACAO_MAX` | `7` | `drone.py` | Duração máxima simulada de uma missão (segundos) |
| `TIMEOUT_BASE_2` | `200` | `base/prioridade.py` | Timeout (ms) da base em 2ª prioridade |
| `TIMEOUT_BASE_3` | `400` | `base/prioridade.py` | Timeout (ms) da base em 3ª prioridade |
| `TIMEOUT_BASE_4` | `600` | `base/prioridade.py` | Timeout (ms) da base em 4ª prioridade |
| `BROADCAST_MAX_TENTATIVAS` | `3` | `setor/broker_setor.py` | Tentativas de reenvio para bases offline |
| `BROADCAST_RETRY_DELAY_S` | `1.0` | `setor/broker_setor.py` | Pausa em segundos entre tentativas de broadcast |
| `MAX_TENTATIVAS` | `5` | `drone.py` | Tentativas de registro na base com exponential backoff |
| `IP_MONITOR` | `127.0.0.1` | `shared/protocolo.py` | IP para onde eventos UDP do monitor são enviados |
