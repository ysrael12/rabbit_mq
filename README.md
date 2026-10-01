# rabbit_mq — conversão de imagens em tons de cinza com fila de mensagens

Módulo distribuído de conversão de imagens para tons de cinza, baseado em fila de mensagens
(RabbitMQ). Vários clientes publicam imagens, vários servidores de conversão consomem a mesma
fila, e **todos** os servidores de armazenamento guardam **todas** as imagens convertidas, com o
mesmo nome do arquivo original.

Trabalho da disciplina de Sistemas Distribuídos (COMP0470, UFS) — Atividade 01 U1.

> Status: **modelagem concluída; implementação em andamento.** Este repositório nasce com os
> diagramas e com o contrato de classes fechado antes do código (figura 5 abaixo).

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

```
clientes ──▶ fila.originais ──▶ conversores ──▶ exchange convertidas (fanout)
                                                     │        │
                                              fila storage.1  fila storage.2
                                                     │        │
                                                 storage1   storage2
```

Duas decisões sustentam os requisitos R4 e R5:

- **Exchange do tipo `fanout`** no lugar de um processo "sender" que reenvia para outras filas.
  Um processo roteador entrega cada mensagem a **um** destino (round-robin), então o storage que
  cai perde a parte dele do acervo. No fanout o broker copia a mensagem para **todas** as filas
  vinculadas, logo toda réplica tem tudo. O número de réplicas deixa de ser um parâmetro do
  código e passa a ser a quantidade de serviços `storage` no compose.
- **Payload com o nome original + bytes da imagem**, gravado pelo storage sem gerar UUID e sem
  renomear — é o que faz a imagem convertida sair com o nome da original.

Garantia de não perder arquivo: fila durável, mensagem persistente, `basic_qos(prefetch=1)` e
**ack manual depois do trabalho**.

Diagrama da arquitetura proposta: [`docs/arquitetura-proposta.png`](docs/arquitetura-proposta.png)

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

PNGs renderizados em [`docs/uml/png/`](docs/uml/png/). Para re-renderizar (precisa de Java +
GraphViz `dot`):

```bash
java -jar plantuml.jar -tpng -charset UTF-8 -o png docs/uml/*.puml
```

## 4. Design patterns aplicados

Nenhum padrão entra aqui por catálogo. Cada linha abaixo resolve um problema concreto deste
projeto — e os que foram **evitados** estão listados com o motivo, que é a metade útil da
decisão.

### 4.1 Resumo

| Padrão | Origem | Onde entra | Resolve |
|---|---|---|---|
| **Publish-Subscribe Channel** | EIP (Hohpe/Woolf) | exchange `convertidas` tipo fanout | R4 — a mesma mensagem chega a N réplicas |
| **Competing Consumers** | EIP | N clientes na `fila.originais`, N conversores | R1, R2 — escala horizontal sem alterar o produtor |
| **Guaranteed Delivery / durável** | EIP | `queue_declare(durable)`, `delivery_mode=PERSISTENT` | não perder arquivo em queda do broker |
| **Idempotent Receiver** | EIP | gravação com o nome original, sobrescrevendo | repetir entrega não duplica o acervo |
| **Dead Letter Channel** | EIP | fila `morta` para mensagem inválida | não travar um consumidor (opcional, hoje fora do escopo) |
| **Strategy** | GoF | trait `Servico` + `Cliente`/`Conversor`/`Storage` | um binário, três comportamentos, um ponto de troca |
| **Command** | GoF | despacho por subcomando em `main` | encapsular "o que rodar" sem `if` espalhado |
| **Template Method** | GoF | laço comum no default de `Servico::executar` | o esqueleto (consumir → tratar → contar → resumir) escrito uma vez |
| **Facade** | GoF | módulo `amqp` sobre o `lapin` | o resto do código não conhece a biblioteca AMQP |
| **Adapter** | GoF | módulos `amqp` e `io_imagem` | trocar `lapin`/`image` sem tocar no domínio |
| **Factory Method** (forma simples) | GoF | `main::construir(papel, cfg) -> Box<dyn Servico>` | um único lugar cria o serviço certo |
| **Ports & Adapters** | Cockburn (arquitetural) | domínio + portas `amqp`, `io_imagem`, `pasta` | o fluxo não depende do broker nem do codec |
| **Circuit Breaker + Retry com backoff** | Nygard, *Release It!* | reconexão ao broker no cliente | o cliente que publica e sai não morre com um broker reiniciando (opcional) |

### 4.2 Os que realmente importam aqui

**Publish-Subscribe Channel (fanout)** — é o padrão que *é* o requisito R4. A alternativa
intuitiva (um componente que distribui para as filas de destino) é o **Content-Based Router /
Recipient List**, que entrega cada mensagem a um destino escolhido — ou seja, particiona os
dados em vez de replicá-los. O fanout não escolhe: copia para todos os binds. Em uma frase: se o
requisito é redundância, o roteamento tem que ser feito pelo broker, não pela aplicação.

**Competing Consumers** — vários consumidores na *mesma* fila. Isso dá escalabilidade (R2)
porque cada mensagem vai para **um** consumidor; é exatamente o oposto do fanout e é o que
permite subir `conversor3` sem mexer em ninguém. O par que evita confundir os dois conceitos:
*uma fila com N consumidores = trabalho dividido; um exchange com N filas = dado replicado.*

**Guaranteed Delivery + Idempotent Receiver** — o par que atende "não perder arquivo" sem
inventar código de retry. Com `prefetch=1` e ack manual depois da conversão, uma queda do
conversor no meio do caminho faz o broker reentregar a mensagem. Isso significa **entrega
at-least-once**: o arquivo pode ser convertido duas vezes. O padrão que torna isso inofensivo é o
Idempotent Receiver — como a saída mantém o nome original, reprocessar apenas sobrescreve o
arquivo com o mesmo conteúdo. Sem essa propriedade, o at-least-once do RabbitMQ viraria arquivo
duplicado.

**Strategy (trait `Servico`)** — cliente, conversor e storage são o mesmo binário, com o mesmo
formato de execução: um laço que conta o que processou e imprime o total. `Servico` com
`executar(&mut self) -> Result<u64>` concentra a diferença no momento da construção e deixa
`main` com um único caminho. A alternativa seria uma enumeração de papéis com `match` espalhado
por todo o programa; o retorno em `u64` existe porque R6 pede números por execução, não só log.

**Template Method no trait** — o esqueleto de execução (declarar topologia → consumir/publicar
em laço → contar → imprimir resumo) é idêntico nos três papéis. Escrever isso como método default
de `Servico` e deixar cada implementação fornecer só os passos variáveis evita três cópias da
mesma estrutura — e é o lugar onde o ack manual e o prefetch ficam garantidos para todos.

**Facade + Adapter no módulo `amqp`** — todo acesso ao `lapin` passa por `conectar`,
`propriedades_persistentes`, `prefetch_um`, `declarar`, `vincular`. Ganho concreto, não
estético: a troca de `rust-rabbit` por `lapin` já foi cogitada, e com a fachada ela seria uma
alteração em um módulo só. `io_imagem` faz o mesmo papel para o crate `image` (o domínio conhece
`converter(bytes) -> bytes`, não `to_luma8`, `DynamicImage` nem `save`).

**Ports & Adapters** — consequência dos dois acima: o fluxo de conversão é expressável sem citar
RabbitMQ. `Conversor` chama a porta `amqp` e a porta `io_imagem`; qual biblioteca está atrás é
detalhe de adaptador. É o que permite, por exemplo, testar a conversão sem broker.

### 4.3 O que foi deliberadamente **não** usado

| Não usar | Por quê |
|---|---|
| **Singleton** para a conexão/canal | Estado global em serviço concorrente é bug esperando acontecer. A conexão é campo da struct; o canal nasce no `novo()` e morre com o serviço. |
| **Abstract Factory** / hierarquia de fábricas | Só existe uma família de objetos (um broker, um codec). `construir(papel, cfg)` é a forma simples do Factory Method e resolve com um `match` de três braços. |
| **Builder para `Config`** | Rust já dá `Default` + struct update (`..Default::default()`). Builder vale a pena quando há muitas combinações válidas de campos; aqui são seis variáveis de ambiente e uma validação. |
| **Pool de conexões próprio** | O modelo do AMQP já é conexão por processo com canais leves dentro dela. Criar pool é resolver um problema que o protocolo não tem. |
| **UUID no nome do arquivo** | Atende "não sobrescrever", mas viola R5 (mesmo nome do original) e destrói a idempotência da regravação. Se a colisão de nomes de clientes diferentes for problema real, a solução é prefixo de pasta por cliente, não renomear. |
| **Content-Based Router / Recipient List** | Substitui o fanout por escolha de destino — entrega parcial, colide com R4. |
| **Message Translator / XML / schema registry** | O payload é bytes + nome, decidido na modelagem. Trazer serialização para cá é dependência nova sem requisito novo. |
| **Um repositório/ORM para "persistência"** | A persistência é "grava arquivo na pasta" — `std::fs` resolve. |

### 4.4 Onde cada padrão aparece no contrato

Referência cruzada com a figura 5 ([diagrama de classes de implementação](docs/uml/05%20classes%20implementacao.puml)):

| Elemento do contrato | Padrão |
|---|---|
| trait `Servico` + `Cliente` / `Conversor` / `Storage` | Strategy (e Template Method no default de `executar`) |
| `main::construir(papel, cfg) -> Servico` e enum `Papel` | Command (despacho por subcomando) + forma simples de Factory Method |
| módulo `amqp` (`conectar`, `propriedades_persistentes`, `prefetch_um`) | Facade sobre o `lapin`; Adapter da porta AMQP |
| módulo `io_imagem` (`converter`, `extensao`) | Adapter sobre o crate `image` |
| `Topologia::declarar` / `vincular` | o bind no fanout que materializa o Publish-Subscribe Channel |
| `Mensagem` (`escrever` / `ler`) | o payload que torna o Idempotent Receiver possível |
| `Storage::salvar` (nome original, sobrescrevendo) | Idempotent Receiver |
| `amqp::prefetch_um` + ack manual em `Conversor::tratar` | Guaranteed Delivery (ack depois do trabalho) |

## 5. Estrutura do repositório

```
rabbit_mq/
├── README.md                  # este arquivo
├── Cargo.toml                 # binário único "app"
├── Dockerfile                 # multi-stage: rust:slim (build) → debian slim (runtime)
├── docker-compose.yml         # rabbitmq + cliente1/2 + conversor1/2 + storage1/2
├── .gitignore
├── src/
│   ├── main.rs                # main + enum Papel + Config::from_env + construir()
│   ├── servico.rs             # trait Servico (executar, e o template default)
│   ├── cliente.rs             # publica a pasta de entrada em fila.originais
│   ├── conversor.rs           # consome, converte, publica no exchange fanout, ack
│   ├── storage.rs             # consome a fila de storage e grava com o nome original
│   ├── mensagem.rs            # payload: tamanho do nome (4 bytes BE) + nome UTF-8 + bytes
│   ├── topologia.rs           # declaração de filas/exchange e binds
│   ├── amqp.rs                # fachada sobre o lapin
│   ├── io_imagem.rs           # adaptador do crate image (to_luma8 + save)
│   ├── relatorio.rs           # contadores por execução
│   └── erro.rs                # tipo de resultado comum
├── pastas-cliente/
│   ├── cliente1/              # imagens que o cliente1 publica
│   └── cliente2/
├── pastas-storage/
│   ├── storage1/              # destino do storage1 (bind mount rw)
│   └── storage2/
└── docs/
    ├── arquitetura-proposta.png
    └── uml/                   # fontes PlantUML + PNG das 5 figuras
```

## 6. Como rodar

```bash
docker compose up --build -d     # sobe broker, clientes, conversores e storages
docker compose logs -f           # acompanha o processamento
docker compose down              # derruba o ambiente
```

Configuração por variável de ambiente, um serviço por instância (a pasta de entrada/saída é um
bind mount por instância, por isso **não** se usa `--scale`):

| Variável | Papel |
|---|---|
| `AMQP_URL` | endereço do broker (ex.: `amqp://guest:guest@rabbitmq:5672/%2f`) |
| `FILA_ORIGINAIS` | nome da fila de entrada (`fila.originais`) |
| `EXCHANGE_CONVERTIDAS` | exchange fanout (`convertidas`) |
| `FILA_STORAGE` | fila de storage daquela instância (`fila.storage.1`, `fila.storage.2`) |
| `PASTA_ENTRADA` | pasta lida pelo cliente |
| `PASTA_SAIDA` | pasta gravada pelo storage |

## 7. Verificação

Log de terminal é indício, não prova. A prova é:

```bash
ls pastas-storage/storage1 pastas-storage/storage2   # os mesmos nomes enviados pelo cliente
md5sum pastas-storage/storage1/* pastas-storage/storage2/*   # hashes iguais = mesma imagem
```

Mais os contadores de cada execução: quantas imagens entraram na fila e quantas cada storage
gravou — número, não linha bonita de log.

## 8. Referências

- Hohpe, G.; Woolf, B. *Enterprise Integration Patterns*. Addison-Wesley. (Publish-Subscribe
  Channel, Competing Consumers, Guaranteed Delivery, Idempotent Receiver, Dead Letter Channel)
- Gamma, E. et al. *Design Patterns: Elements of Reusable Object-Oriented Software*. (Strategy,
  Command, Template Method, Facade, Adapter, Factory Method)
- Cockburn, A. *Hexagonal Architecture* (Ports & Adapters).
- Nygard, M. *Release It!* (Circuit Breaker, Retry com backoff).
- RabbitMQ — [Tutorials: Publish/Subscribe](https://www.rabbitmq.com/tutorials/tutorial-three-python),
  [Reliability Guide](https://www.rabbitmq.com/docs/reliability), [Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch).
- Boschi, S.; Santomaggio, G. *RabbitMQ Cookbook*. Packt Publishing.
