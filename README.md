# rabbit_mq — conversão de imagens em tons de cinza com fila de mensagens

Módulo distribuído de conversão de imagens para tons de cinza, baseado em fila de mensagens
(RabbitMQ). Vários clientes publicam imagens, vários servidores de conversão consomem a mesma
fila, e **todos** os servidores de armazenamento guardam **todas** as imagens convertidas, com o
mesmo nome do arquivo original.

Trabalho da disciplina de Sistemas Distribuídos (COMP0470, UFS) — Atividade 01 U1.

> Status: **código implementado (`cargo test`: 3 testes passando); falta o empacotamento**
> (`Dockerfile` + `docker-compose.yml`). Sem ele, não houve execução ponta a ponta com RabbitMQ
> real — a evidência hoje é o teste de fluxo completo sobre um broker em memória.

---

## 1. Requisitos que a solução precisa cumprir

| # | Requisito |
|---|---|
| R1 | Vários clientes (produtores) enviam imagens para uma fila de imagens |
| R2 | Vários servidores de conversão (consumidores) retiram imagens da mesma fila e convertem |
| R3 | A imagem convertida segue para armazenamento em uma etapa posterior |
| R4 | Todos os servidores de armazenamento guardam todas as imagens (redundância total) |
| R5 | A imagem convertida é gravada com o mesmo nome do arquivo original |
| R6 | O programa é minimamente testável: instâncias executáveis e verificáveis |
| R7 | Execução automatizada com Docker (Dockerfile e/ou Docker Compose), comandos documentados |

## 2. Arquitetura

```mermaid
flowchart LR
    c1["cliente1"] --> fila["fila.originais<br/>durável, mensagem persistente"]
    c2["cliente2"] --> fila
    fila --> cv1["conversor1"]
    fila --> cv2["conversor2"]
    cv1 --> ex["exchange convertidas<br/>fanout"]
    cv2 --> ex
    ex --> qs1["fila storage.1"]
    ex --> qs2["fila storage.2"]
    qs1 --> st1["storage1"]
    qs2 --> st2["storage2"]
```

Duas decisões sustentam os requisitos R4 e R5:

- **Exchange do tipo `fanout`** no lugar de um processo "sender" que reenvia para outras filas.
  Um processo roteador entrega cada mensagem a **um** destino, então o storage que cai perde a
  parte dele do acervo. No fanout o broker copia a mensagem para **todas** as filas vinculadas,
  logo toda réplica tem tudo.
- **Payload com o nome original + bytes da imagem**, gravado pelo storage sem gerar UUID e sem
  renomear — é o que faz a imagem convertida sair com o nome da original.

Garantia de não perder arquivo: fila durável, mensagem persistente, `prefetch 1` e **ack manual
depois do trabalho**. Não perder arquivo em queda do broker/consumidor. Detalhe importante do
desenho: a cópia para as réplicas e a durabilidade são **configuração** (broker/compose), não
código de roteamento — a aplicação não tem uma linha decidindo para qual storage enviar.

Diagrama da arquitetura proposta: [`docs/arquitetura-proposta.png`](docs/arquitetura-proposta.png)

### 2.1 Estado atual do código (ler antes de começar)

```bash
cargo run -- cliente      # publica a pasta de entrada
cargo run -- conversor    # consome, converte e publica no fanout
cargo run -- storage      # consome a própria fila e grava
```

O `src/` foi implementado **em camadas**, não por padrão — desvio consciente do
[`docs/uml/06 mapa de modulos por padrao.puml`](docs/uml/06%20mapa%20de%20modulos%20por%20padrao.puml).
A árvore por padrão continua sendo a referência de leitura dos diagramas 6 a 11; a
troca por camadas foi para andar mais rápido. Ver a tabela de correspondência em
[`docs/patterns.md`](docs/patterns.md) §5.

```mermaid
flowchart LR
    main["main.rs<br/>args, Papel, Config"] --> ctrl["controladores/<br/>criar — Factory Method<br/>Invocador — Command"]
    ctrl --> mod["modelos/<br/>Cliente, Conversor, Storage — Strategy<br/>Topologia — Mediator<br/>Broker + PortaBroker — Facade<br/>io_imagem — Adapter<br/>utils — Template Method"]
```

Motivo: menos pastas com um arquivo só e menos `mod.rs` para declarar. Custo aceito: o nome do
padrão deixa de aparecer no caminho do arquivo, que é justamente o que o diagrama 06 queria
garantir.

| Já existe | Ainda não existe |
|---|---|
| `Cargo.toml` / `Cargo.lock` (edition 2024; `lapin`, `tokio`, `image`, `async-trait`, `futures-lite`) | `Dockerfile`, `docker-compose.yml` |
| `src/` completo: `modelos/` + `controladores/` (1.114 linhas, 15 arquivos) | `Observer` e `Decorator` (figura 11) |
| 3 testes passando (`cargo test`), incluindo o fluxo completo sobre broker em memória | prova ponta a ponta (`docker compose up`) |
| `docs/` com os diagramas e `patterns.md` | imagens de teste nas pastas |

Próximo passo: `Dockerfile` multi-stage → `docker-compose.yml` (rabbitmq + 6 serviços) → copiar
algumas imagens para `pastas-cliente/cliente1` → `docker compose up --build -d` → conferir
`md5sum` das duas pastas de storage.

## 3. Modelagem feita antes do código

Os diagramas foram feitos **antes** de escrever qualquer linha do programa, e não depois para
ilustrar o que já existia. As duas correções mais caras do projeto (sender → fanout, e a decisão
de manter o nome original no payload) apareceram no desenho, onde custam uma edição de arquivo, e
não no código, onde custariam refazer o fluxo de publicação, de consumo e de gravação.

| Figura | Diagrama | Fonte |
|---|---|---|
| 1 | Implantação — containers, rede do compose, pastas montadas, filas e exchange | [`docs/uml/01 implantacao containers.puml`](docs/uml/01%20implantacao%20containers.puml) |
| 2 | Componentes do binário único — subcomandos e módulos | [`docs/uml/02 componentes do binario.puml`](docs/uml/02%20componentes%20do%20binario.puml) |
| 3 | Sequência — fluxo de uma imagem, do cliente às duas réplicas | [`docs/uml/03 sequencia fanout.puml`](docs/uml/03%20sequencia%20fanout.puml) |
| 4 | Classes — especialização em cliente/conversor/storage | [`docs/uml/04 classes dependencias.puml`](docs/uml/04%20classes%20dependencias.puml) |
| 5 | Classes de **implementação** — campos e assinaturas prontos para virar código | [`docs/uml/05 classes implementacao.puml`](docs/uml/05%20classes%20implementacao.puml) |

Diagramas de implementação por padrão (as figuras 7 a 11 abaixo estão em
[`docs/patterns.md`](docs/patterns.md)):

| Figura | Diagrama | Arquivo |
|---|---|---|
| 6 | Mapa de módulos de `src/` — dependências entre os padrões | [`docs/uml/06 mapa de modulos por padrao.puml`](docs/uml/06%20mapa%20de%20modulos%20por%20padrao.puml) |
| 7 | Factory Method — quem constrói cada serviço | [`docs/uml/07 factory method.puml`](docs/uml/07%20factory%20method.puml) |
| 8 | Strategy + Template Method — o contrato dos três papéis | [`docs/uml/08 strategy e template method.puml`](docs/uml/08%20strategy%20e%20template%20method.puml) |
| 9 | Mediator + Command — topologia e despacho | [`docs/uml/09 mediator e command.puml`](docs/uml/09%20mediator%20e%20command.puml) |
| 10 | Facade + Adapter — a fronteira onde o `lapin` fica confinado | [`docs/uml/10 facade e adapter.puml`](docs/uml/10%20facade%20e%20adapter.puml) |
| 11 | Decorator + Observer — empilhar comportamento sem editar o conversor | [`docs/uml/11 decorator e observer.puml`](docs/uml/11%20decorator%20e%20observer.puml) |
| 12 | Estrutura entregue — as duas camadas e a correspondência arquivo ↔ padrão | [`docs/uml/12 codigo entregue.puml`](docs/uml/12%20codigo%20entregue.puml) |

PNGs renderizados em [`docs/uml/png/`](docs/uml/png/). Para re-renderizar (precisa de Java +
GraphViz `dot`):

```bash
java -jar plantuml.jar -tpng -charset UTF-8 -o png docs/uml/*.puml
```

## 4. Estrutura do repositório

```mermaid
graph TD
    raiz["rabbit_mq/"]
    raiz --> md["README.md"]
    raiz --> docs["docs/"]
    docs --> pat["patterns.md — os padrões GoF, com código e justificativa"]
    docs --> png["arquitetura-proposta.png"]
    docs --> uml["uml/ — fontes PlantUML + PNG das figuras"]
    raiz --> cargo["Cargo.toml — binário único rabbit_mq, edition 2024"]
    raiz --> dockerfile["Dockerfile — multi-stage: rust:slim build, debian slim runtime"]
    raiz --> compose["docker-compose.yml — rabbitmq + cliente1/2 + conversor1/2 + storage1/2"]
    raiz --> src["src/"]
    src --> main["main.rs — args, Papel, Config, criar, Invocador.executar"]
    src --> modelos["modelos/ — domínio + os padrões, desvio do diagrama 06, ver §2.1"]
    modelos --> cfg["Config.rs — Config.from_env"]
    modelos --> msg["Mensagem.rs — 4 bytes BE com o tamanho do nome + nome UTF-8 + bytes"]
    modelos --> topo["Topologia.rs — Mediator: filas, exchange, binds"]
    modelos --> serv["Cliente.rs, Conversor.rs, Storage.rs — Strategy"]
    modelos --> fac["Broker.rs + PortaBroker.rs — Facade e a porta"]
    modelos --> adap["io_imagem.rs — Adapter do crate image"]
    modelos --> utils["utils.rs — Papel, Res, relatorio, trait Servico + Template Method"]
    src --> ctrl["controladores/"]
    ctrl --> criar["criar.rs — Factory Method, FabricaReal e FabricaFalsa"]
    ctrl --> inv["invocador.rs — Command"]
    raiz --> pc["pastas-cliente/ — cliente1 e cliente2, bind mount ro"]
    raiz --> ps["pastas-storage/ — storage1 e storage2, bind mount rw"]
```

Correspondência: o código ficou **em camadas** (`modelos/` + `controladores/`), e não com
**nome da pasta = nome do padrão** como o diagrama 06 pede — o mapa padrão ↔ arquivo está em
[`docs/patterns.md`](docs/patterns.md) §5.1.

## 5. Como rodar

**Hoje** (sem Docker, com o binário local — precisa de um RabbitMQ em `AMQP_URL`):

```bash
cargo run -- cliente      # publica pastas-cliente/cliente1 na fila.originais
cargo run -- conversor    # consome, converte e publica no exchange convertidas
cargo run -- storage      # consome a própria fila e grava com o nome original
```

**Projetado** (ainda não escrito — `Dockerfile` e `docker-compose.yml` são o que falta):

```bash
docker compose up --build -d     # sobe broker, clientes, conversores e storages
docker compose logs -f           # acompanha o processamento
docker compose down              # derruba o ambiente
```

Configuração por variável de ambiente, um serviço por instância (a pasta de entrada/saída é um
bind mount por instância, por isso **não** se usa `--scale`):

| Variável | Papel |
|---|---|
| `AMQP_URL` | endereço do broker (ex.: `amqp://guest:***@rabbitmq:5672/%2f`) |
| `FILA_ORIGINAIS` | nome da fila de entrada (`fila.originais`) |
| `EXCHANGE_CONVERTIDAS` | exchange fanout (`convertidas`) |
| `FILA_STORAGE` | fila de storage daquela instância (`fila.storage.1`, `fila.storage.2`) |
| `PASTA_ENTRADA` | pasta lida pelo cliente |
| `PASTA_SAIDA` | pasta gravada pelo storage |

## 6. Verificação

Log de terminal é indício, não prova. O que roda hoje: `cargo test` — 3 testes, incluindo o fluxo
completo (cliente → conversor → 2 storages) sobre um broker em memória, com asserções de que as
duas réplicas têm conteúdo idêntico, que todo pixel de saída tem os três canais iguais e que nada
ficou sem ack.

Depois do empacotamento, a prova ponta a ponta é:

```bash
ls pastas-storage/storage1 pastas-storage/storage2   # os mesmos nomes enviados pelo cliente
md5sum pastas-storage/storage1/* pastas-storage/storage2/*   # hashes iguais = mesma imagem
```

Mais os contadores de cada execução (`relatorio`, função livre — não há Observer no código):
quantas imagens entraram na fila e quantas cada storage gravou — número, não linha bonita de log.
