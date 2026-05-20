# Ormuz Command Center
**Infraestrutura Distribuída para Coordenação de Drones Autônomos de Monitoramento Marítimo**  
*Disciplina TEC502 — Sistemas Distribuídos · Universidade Estadual de Feira de Santana (UEFS)*

---

## Visão Geral

O sistema monitora o Estreito de Ormuz dividindo a área operacional em **8 setores marítimos**, cada um com um sensor e um broker próprio. Uma frota de drones autônomos é compartilhada entre **4 bases** (Norte, Sul, Leste, Oeste), que competem de forma coordenada para atender as ocorrências reportadas pelos sensores.

O objetivo central do projeto é garantir que **nenhum drone seja despachado duas vezes para a mesma ocorrência (não-duplicidade)**, que requisições críticas sejam priorizadas e que o sistema possua resiliência para continuar operando mesmo sob falha de componentes.

---

## Arquitetura e Estilo

O sistema adota o estilo de **Brokers Distribuídos (P2P entre bases)**, garantindo a **ausência de um ponto único de falha**. Não existe um coordenador central dando as ordens — cada base possui sua própria cópia da Fila Replicada e toma decisões de despacho de forma totalmente autônoma.

```text
[Sensor S1..S8]
      │ TCP (Alerta de anomalia)
      ▼
[Broker de Setor S1..S8]
      │ TCP (Broadcast de Requisição)
      ▼
[Base NORTE] <─── TCP (Broadcast de ACEITE - Exclusão Mútua) ───> [Base SUL]
      │
      │ TCP (Despacho de Missão)
      ▼
[Drone Autônomo] ─── UDP (Heartbeat de Status) ───> [Sua Base de Origem]
```

Todos os serviços foram contêinerizados com Docker e isolados, permitindo a execução distribuída em máquinas distintas nas redes do LARSID/LADICA.

---

## Componentes do Sistema

**Sensor** (`sensor/sensor.py`): Atua como publicador de dados brutos. Sorteia ocorrências com base em pesos (eventos críticos são raros) e envia alertas TCP para o seu broker de setor.

**Broker de Setor** (`setor/broker_setor.py`): Recebe o alerta, estampa o Relógio de Lamport e faz broadcast da requisição simultaneamente para todas as 4 bases.

**Broker de Base** (`base/broker.py`): O núcleo da inteligência. Mantém a Fila Replicada, monitora a integridade dos drones e executa o algoritmo de exclusão mútua por timeout diferenciado para decidir quem atende o chamado.

**Drone** (`drone/drone.py`): Componente passivo. Registra-se na base, recebe a missão (simulada via sleep) e emite heartbeats contínuos.

**Monitor Bridge** (`monitor/`): Interface tática via navegador. O `monitor_bridge.py` consome os pacotes UDP da rede e repassa para a interface HTML via WebSocket para renderização em tempo real.

---

## Mecanismos Distribuídos

### 1. Exclusão Mútua (Timers e Broadcast)

Para evitar que duas bases despachem drones para a mesma missão, o sistema utiliza **Timeouts Diferenciados por Prioridade** (inspirado em slots TDMA):

- A base com prioridade 1 para um setor atua em **0ms**
- Bases secundárias aguardam **200ms**, **400ms** ou **600ms**

Quando uma base aceita a missão, ela faz um broadcast de ACEITE. As demais bases recebem a mensagem, cancelam suas concorrências e atualizam o status local da requisição para `ACEITA`. O uso de Locks (mutex) garante a atomicidade da leitura e gravação no estado da fila local.

### 2. Priorização e Relógios de Lamport

A Fila Distribuída é rigorosamente ordenada em três níveis:

1. **Criticidade** — CRITICA > ALTA > BAIXA
2. **Timestamp de Lamport** — ordenação causal temporal dos eventos
3. **ID do Setor** — desempate lexicográfico

### 3. Encaminhamento Passivo e Replanejamento

Se uma base recebe uma requisição mas não possui drones livres, a requisição aguarda na fila global. Quando qualquer drone finaliza uma missão, a base varre a fila e puxa a requisição pendente de maior prioridade automaticamente.

### 4. APIs e Tratamento de Falhas de Comunicação

**Mensagens Críticas (TCP):** Alertas, requisições, missões e aceites trafegam via TCP com length-prefixing (4 bytes de cabeçalho) garantindo a entrega e o framing correto.

**Telemetria (UDP):** O heartbeat dos drones usa UDP por ser de alta frequência. A perda de pacotes avulsos é tolerada pelo sistema.

---

## Evidências de Consistência e Testes

Para comprovar a não-duplicidade de cobertura e a consistência sob carga, o projeto inclui testes isolados e mecanismos de stress.

### A. Teste Automatizado de Concorrência

O script `test_duplicidade.py` simula 10 threads concorrentes disparando milissegundos umas das outras para tentar aceitar a mesma requisição exata na memória.

```bash
python tests/test_duplicidade.py
# Saída esperada: OK — zero duplicatas sob concorrência
```

### B. Stress Test e Tolerância a Falhas

A interface web foi utilizada para submeter a malha a uma alta injeção de alertas simultâneos. As imagens abaixo comprovam o funcionamento da exclusão mútua sob carga e a recuperação de falhas de hardware (drone abatido).

> **Figura 1:** Fila distribuída processando alertas massivos sem duplicação de atendimento.

> **Figura 2:** Sistema detectando a queda de um drone via timeout de heartbeat e devolvendo a missão para o topo da fila.

---

## Como Executar no Laboratório (LARSID/LADICA)

O sistema foi desenhado para rodar distribuído. Siga os passos abaixo configurando o arquivo `.env` com os IPs locais de cada máquina.

### 1. Configuração do `.env` (em todas as máquinas)

```env
IP_PC_SETORES=192.168.x.x
IP_PC_BASES=192.168.x.x
IP_MONITOR=192.168.x.x
```

### 2. Máquina 1 — Brokers de Base

```bash
docker compose -f docker-compose.bases.yml up --build
```

### 3. Máquina 2 — Setores e Sensores

```bash
docker compose -f docker-compose.setores.yml up --build
# Em outro terminal da mesma máquina (ou máquina separada)
docker compose -f docker-compose.sensores.yml up --build
```

### 4. Máquina 3 — Frota de Drones

```bash
docker compose -f docker-compose.drones.yml up --build
```

### 5. Command Center (Interface Gráfica)

No terminal do PC configurado para exibir o mapa:

```bash
pip install websockets
python monitor/monitor_bridge.py
```

Em seguida, abra o arquivo `monitor/index.html` em qualquer navegador, acesse a aba **"Bridge"** e clique em **"Conectar ao Sistema Real"**.
