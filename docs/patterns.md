# docs/patterns.md — design patterns do projeto (só GoF)

Decisão do projeto: **apenas padrões GoF** (*Design Patterns*, Gamma et al.). Os padrões de
integração (EIP) e os padrões de resiliência ficam fora do escopo e a arquitetura foi ajustada
para que os requisitos sejam atendidos sem eles.

Onde cada um aparece e **por que ele existe aqui** — a pergunta é sempre "o que quebra sem ele",
nunca "que padrão eu poderia encaixar".

---

## 1. O mapa

| # | Padrão GoF | Categoria | Onde entra | O que quebra sem ele |
|---|---|---|---|---|
| 1 | **Facade** | Estrutural | `estrutural/facade/broker.rs` | Toda a aplicação passa a conhecer a API do `lapin`, e trocar de biblioteca AMQP vira refatoração global |
| 2 | **Adapter** | Estrutural | `estrutural/adapter/` | O domínio fala em `to_luma8`/`DynamicImage`/`save` do crate `image` em vez de "converter para tons de cinza" |
| 3 | **Strategy** | Comportamental | `comportamento/strategy/servico.rs` | Três `main`s ou um `match` de papel espalhado por todo o fluxo |
| 4 | **Template Method** | Comportamental | default de `Servico::executar` | O esqueleto (declarar → consumir/publicar → contar → resumir) copiado três vezes, com o ack manual em cada cópia |
| 5 | **Command** | Comportamental | `comportamento/command/comando.rs` | `main` com `if/else` por argumento e contadores espalhados |
| 6 | **Mediator** | Comportamental | `comportamento/mediator/topologia.rs` | Cada serviço declara fila, exchange e bind por conta própria; adicionar uma réplica exige editar cliente e conversor |
| 7 | **Observer** | Comportamental | `comportamento/observer/relatorio.rs` | O componente de execução precisa saber quem imprime e contar para cada destino |
| 8 | **Decorator** | Estrutural | `estrutural/decorator/` | Para passar a validar imagem, medir tempo ou reenviar, seria preciso editar dentro do conversor |
| 9 | **Factory Method** | Criacional | `criacional/factory_method/` | O `main` precisa saber construir cada serviço; a substituição de fábrica (real ↔ portas falsas) deixa de ser possível |

### Onde a redundância entra, agora sem padrão de integração

A cópia da mensagem para todas as filas de armazenamento é **configuração do broker**, não código
nosso: exchange `convertidas` do tipo `fanout` com uma fila por réplica fazendo bind nele. O
RabbitMQ copia para todos os binds; a aplicação não tem uma linha de roteamento.

Isso é pré-requisito do desenho, não padrão de projeto. O ponto que importa para quem lê o
código: **não existe nenhum roteador de réplicas na aplicação** — se você procura essa lógica, ela
está no `Mediator`/`Topologia` apenas como *declaração* (`exchange_declare`, `queue_declare`,
`bind`), e a entrega é feita pelo broker.

Pela mesma razão, entrega durável (`durable`, `delivery_mode=PERSISTENT`) e confirmação manual
(`prefetch 1` + ack depois do trabalho) são **configuração da conexão e do consumo**, não um
padrão a mais no diagrama.

### Diagramas de implementação

Cada recorte tem o seu próprio diagrama (fonte PlantUML + PNG em `docs/uml/`):

| Figura | Diagrama | Arquivo |
|---|---|---|
| 06 | Mapa de módulos de `src/` e dependências entre os padrões | [`06 mapa de modulos por padrao.puml`](uml/06%20mapa%20de%20modulos%20por%20padrao.puml) |
| 07 | Factory Method — quem constrói cada serviço | [`07 factory method.puml`](uml/07%20factory%20method.puml) |
| 08 | Strategy + Template Method — o contrato dos três papéis | [`08 strategy e template method.puml`](uml/08%20strategy%20e%20template%20method.puml) |
| 09 | Mediator + Command — topologia e despacho | [`09 mediator e command.puml`](uml/09%20mediator%20e%20command.puml) |
| 10 | Facade + Adapter — a fronteira onde o `lapin` fica confinado | [`10 facade e adapter.puml`](uml/10%20facade%20e%20adapter.puml) |
| 11 | Decorator + Observer — empilhar comportamento sem editar o conversor | [`11 decorator e observer.puml`](uml/11%20decorator%20e%20observer.puml) |

PNGs em [`uml/png/`](uml/png/). Re-render: `java -jar plantuml.jar -tpng -charset UTF-8 -o png docs/uml/*.puml`.

---

## 2. Padrões estruturais

### 2.1 Facade — `estrutural/facade/broker.rs`

**Problema.** `lapin` é assíncrono, devolve `Result` em cada chamada, exige declarar fila, exchange
e bind em ordem, e usa `BasicProperties` para marcar persistência. Sem fachada, essas três
responsabilidades aparecem em cada um dos três serviços.

**Solução.** Uma fachada `Broker` com a operação que a aplicação realmente usa:

```rust
pub struct Broker { canal: Channel, cfg: Config }

impl Broker {
    pub async fn conectar(cfg: &Config) -> Res<Broker>;
    pub async fn declarar_fila(&self, nome: &str) -> Res<()>;
    pub async fn declarar_fanout(&self, nome: &str) -> Res<()>;
    pub async fn vincular(&self, fila: &str, exchange: &str) -> Res<()>;
    pub async fn definir_prefetch(&self, n: u16) -> Res<()>;
    pub async fn publicar_persistente(&self, destino: &str, corpo: &[u8]) -> Res<()>;
    pub async fn consumir(&self, fila: &str) -> Res<...>;
    pub async fn confirmar(&self, tag: u64) -> Res<()>;
}
```

`publicar_persistente` já embute `delivery_mode = 2`; `definir_prefetch` já é chamado com `1` pelo
consumo. Nenhum módulo fora desta pasta nomeia um tipo do `lapin`.

**Consequência.** Trocar `lapin` por outro cliente AMQP é reescrever um arquivo. Foi exatamente a
troca cogitada no projeto (`rust-rabbit` → `lapin`) — com a fachada, custa um arquivo.

### 2.2 Adapter — `estrutural/adapter/`

Dois adaptadores, uma razão cada:

- `io_imagem.rs` (adaptador do crate `image`): traduz `converter(bytes) -> Res<Vec<u8>>` e
  `extensao(nome) -> Option<String>` para as chamadas `load_from_memory`, `to_luma8`, `save` —
  o domínio nunca vê `DynamicImage` nem `ImageFormat`.
- `lapin_adapter.rs`: o tipo que materializa a fachada sobre o `lapin`.

**Consequência.** Um adaptador de porta falsa (sem broker, sem crate de imagem) permite rodar o
fluxo em teste, porque o domínio só conhece as assinaturas do adaptador.

---

## 3. Padrões comportamentais

### 3.1 Strategy — `comportamento/strategy/servico.rs`

**Problema.** Cliente, conversor e storage compartilham o formato de execução (laço que conta e
imprime o total) e diferem no que fazem por item.

**Solução.** O trait é a estratégia; cada serviço é uma implementação.

```rust
#[async_trait]
pub trait Servico {
    async fn preparar(&mut self) -> Res<()>;
    async fn passo(&mut self, corpos: &[Vec<u8>]) -> Res<usize>;
    fn resumo(&self) -> String;
}
```

**Consequência.** `main` não tem três caminhos: tem uma `Vec<Box<dyn Servico>>`. Uma instância
nova de qualquer papel entra sem tocar no despacho.

### 3.2 Template Method — default de `Servico::executar`

**Problema.** O esqueleto de execução é idêntico nos três papéis: preparar a topologia, consumir
em laço com `prefetch = 1`, confirmar cada item **depois** de tratá-lo, contar, imprimir o resumo.

**Solução.** Esse esqueleto é um método pronto no trait; cada serviço implementa só os passos
variáveis (`preparar`, `passo`, `resumo`).

```rust
pub async fn executar(&mut self) -> Res<u64> {
    self.preparar().await?;
    let mut total = 0;
    loop {
        let corpos = self.receber().await?;   // consumir
        let n = self.passo(&corpos).await?;   // ponto variável
        if n > 0 { self.confirmar().await?; } // ack DEPOIS do trabalho
        total += n as u64;
        if n == 0 { break; }
    }
    println!("{}", self.resumo());
    Ok(total)
}
```

**Consequência.** A ordem correta (trabalhar e só então confirmar) existe em **um** lugar. Se
alguém escrevesse o laço em cada serviço, uma das três cópias acabaria confirmando antes — e a
perda de arquivo não apareceria em teste de fluxo normal.

### 3.3 Command — `comportamento/command/comando.rs`

**Problema.** O binário tem três subcomandos e precisa registrar o que foi publicado e o que foi
salvo para o resumo final da execução.

**Solução.** Cada pedido do usuário vira um objeto com a mesma interface, e o invocador
(`main`) não conhece o interior:

```rust
pub enum Pedido { Cliente, Conversor, Storage }
pub struct Comando { servico: Box<dyn Servico>, pedido: Pedido }
impl Comando { pub async fn executar(self) -> Res<u64> { self.servico.executar().await } }
```

**Consequência.** Adicionar `app inspecionar` é um braço no `match` de `main` mais uma
implementação de `Servico` — sem `if` novo dentro dos outros comandos.

### 3.4 Mediator — `comportamento/mediator/topologia.rs`

**Problema.** As instâncias precisam concordar sobre nomes de fila, exchange e vínculos. Espalhar
`queue_declare`/`exchange_declare`/`bind` entre cliente, conversor e storage cria dependência
cruzada: mudar o nome da fila de storage obriga a editar o conversor.

**Solução.** Um mediador centraliza a topologia e a entrega pronta para cada papel:

```rust
pub struct Topologia { cfg: Config }

impl Topologia {
    pub async fn preparar(&self, broker: &Broker, papel: Papel) -> Res<()> {
        broker.declarar_fila(&self.cfg.fila_originais).await?;   // fila durável
        broker.declarar_fanout(&self.cfg.exchange_convertidas).await?; // exchange durável
        match papel {
            Papel::Cliente   => { /* só publica na fila.originais */ }
            Papel::Conversor => { /* publica no exchange */ }
            Papel::Storage   => {
                broker.declarar_fila(&self.cfg.fila_storage).await?;
                broker.vincular(&self.cfg.fila_storage, &self.cfg.exchange_convertidas).await?;
            }
        }
        Ok(())
    }
    pub async fn confirmar(&self, broker: &Broker, tag: u64) -> Res<()>;
}
```

**Consequência.** Os serviços param de se conhecer. Subir `storage3` é acrescentar um serviço no
compose e uma linha de configuração de fila — o código do cliente e do conversor não é tocado. O
`bind` é o ponto exato onde a fanout acontece: quem lê o mediador encontra a redundância numa
linha.

### 3.5 Observer — `comportamento/observer/relatorio.rs`

**Problema.** O requisito de verificação pede números por execução (publicadas × salvas por
storage). Se cada componente escrever seu próprio contador, o formato diverge e a soma não bate.

**Solução.** O loop de execução é o *sujeito*; os observadores são registradores registrados nele:

```rust
pub trait Observador: Send { fn registrar(&mut self, evento: &Evento); }

pub enum Evento {
    Publicada   { nome: String },
    Convertida  { nome: String, antes: usize, depois: usize },
    Salva       { nome: String, bytes: usize },
    Descartada  { motivo: String },
}

pub struct Assunto { observadores: Vec<Box<dyn Observador>> }
impl Assunto {
    pub fn inscrever(&mut self, o: Box<dyn Observador>);
    pub fn notificar(&mut self, e: Evento);
}
```

**Consequência.** Somar um registro (arquivo de log, contador por réplica, métrica de bytes) é
inscrever mais um observador — nenhum serviço é editado. O resumo impresso no fim da execução é
apenas a view de um dos observadores.

### 3.6 Decorator — `estrutural/decorator/`

**Problema.** O fluxo normal não valida imagem nem mede tempo — e foi declarado fora do escopo
validar. Mas se a validação for pedida, a tentação é escrever um `if` dentro do conversor, e a
partir daí "validar" e "converter" ficam no mesmo lugar.

**Solução.** Envolver o passo com decoradores empilháveis, sem tocar no conversor:

```rust
#[async_trait]
pub trait Passo { async fn tratar(&mut self, corpo: Vec<u8>) -> Res<Vec<u8>>; }

pub struct Converter;                          // Passo base (usa o adapter io_imagem)
pub struct Validar<P: Passo>   { interno: P }  // recusa entrada inválida antes de gastar CPU
pub struct MedirTempo<P: Passo> { interno: P } // observa o tempo de cada item (alimenta o Observer)
pub struct Reenviar<P: Passo>  { interno: P, tentativas: u8 } // última defesa, antes de descartar
```

**Consequência.** Empilhar é escolher o comportamento: `Reenviar::new(MedirTempo::new(Converter))`.
O conversor continua sem saber que existe validação. Hoje o stack é `Converter` puro — os outros
três já estão com lugar reservado e ganham uso quando a validação for requisito.

---

## 4. Padrões criacionais

### 4.1 Factory Method — `criacional/factory_method/`

**Problema.** `main` decide qual serviço rodar a partir do argumento. Se ela também souber
*construir* cada serviço (config, conexão, topologia, observadores), o ponto de entrada vira o
arquivo mais complexo do projeto.

**Solução.** O objeto relevante é qual `Servico` roda, então o padrão aparece na forma simples:
uma função de criação (com o trait de fábrica disponível quando entrar a segunda família).

```rust
#[async_trait]
pub trait FabricaServico: Send + Sync {
    async fn criar(&self, cfg: Config) -> Res<Box<dyn Servico>>;
}

pub async fn criar(papel: Papel, cfg: Config) -> Res<Box<dyn Servico>> {
    match papel {
        Papel::Cliente   => Cliente::novo(cfg).await,
        Papel::Conversor => Conversor::novo(cfg).await,
        Papel::Storage   => Storage::novo(cfg).await,
    }
}
```

**Consequência.** `main` tem uma linha de criação; e a substituição por uma fábrica de teste
(serviços sobre portas falsas, sem broker) é só outra implementação do trait.

### 4.2 Criacionais deliberadamente não usados

| Padrão | Por que **não** |
|---|---|
| **Singleton** | Estado global em serviço concorrente é bug esperando acontecer. Conexão e canal são campos da struct: nascem no `novo()` e morrem com o serviço. |
| **Builder** | Rust já entrega `Default` + struct update (`Config { pasta_entrada, ..Default::default() }`). Builder paga o próprio custo quando existe um conjunto grande de combinações válidas; aqui são seis variáveis de ambiente e uma validação. |
| **Abstract Factory** | Existe uma única família de objetos (um broker, um codec de imagem). Criar a hierarquia seria fábrica com uma implementação só. |
| **Prototype** | Nada é clonado a partir de um modelo já construído; os objetos nascem do `Config`, não de cópia. |

---

## 5. Árvore de pastas

```
src/
├── main.rs                      # ponto de entrada: lê args, monta Config, cria o Comando
├── comum/
│   ├── config/                  # Config::from_env — seis variáveis, um lugar
│   ├── mensagem/                # payload: tamanho do nome (4 bytes BE) + nome UTF-8 + bytes
│   ├── erro/                    # tipo de resultado comum (Res<T>)
│   └── relatorio/               # formatação do resumo da execução
├── criacional/
│   └── factory_method/          # FabricaServico + fn criar(Papel, Config) -> Box<dyn Servico>
├── estrutural/
│   ├── facade/                  # Broker: fachada do lapin (conectar, declarar, publicar, consumir, confirmar)
│   ├── adapter/                 # io_imagem (crate image) + lapin_adapter (implementa a fachada)
│   └── decorator/               # Passo + Validar / MedirTempo / Reenviar
└── comportamento/
    ├── strategy/                # trait Servico + Cliente / Conversor / Storage
    ├── template_method/         # Servico::executar — o esqueleto do laço, com ack depois do trabalho
    ├── command/                 # Pedido, Comando + Invocador
    ├── observer/                # Evento, Observador, Assunto + registradores
    └── mediator/                # Topologia: nomes de fila/exchange e os binds por papel
```

Correspondência pasta ↔ módulo Rust: cada pasta tem seu `mod.rs` (ou o arquivo do padrão) e o
`main.rs` declara `mod criacional::factory_method;` etc. O nome da pasta é o nome do padrão,
para que a intenção apareça na primeira linha de um caminho de arquivo.

---

## 6. O fluxo de uma imagem, descrito em padrões

1. O `Comando` de cliente é criado pela `Factory Method` e executado pelo `Template Method`.
2. O cliente lê a pasta de entrada e publica cada imagem via `Facade` do broker, na fila de
   entrada que o `Mediator` declarou; cada publicação notifica os `Observer`.
3. O conversor é a `Strategy` que consome a mesma fila — o broker entrega a mensagem a **um**
   consumidor por vez, que é o que dá escala.
4. O passo do conversor é um passo `Decorator` que usa o `Adapter` de imagem para converter.
   A mensagem é confirmada **depois** da conversão e da publicação no exchange.
5. O `Mediator` já declarou o exchange `fanout` e o `bind` de cada fila de storage; o broker
   copia a mensagem para todas as filas vinculadas — é aqui que a redundância acontece, sem uma
   linha de roteamento na aplicação.
6. Cada storage (mesma `Strategy`) consome sua fila, usa o `Adapter` para gravar com o nome
   original e notifica os `Observer`, que produzem o resumo numérico da execução.
