//! Porta do broker (diagrama 10): a fronteira onde o lapin fica confinado.
//!
//! O dominio conhece so estes metodos. Trocar a biblioteca AMQP e reescrever
//! `Broker`; uma implementacao falsa permite rodar o fluxo em teste sem broker.
//!
//! Desvio consciente do diagrama 10: la existe um unico
//! `publicar_persistente(destino)`. Em AMQP sao dois caminhos diferentes —
//! publicar numa fila (exchange default, routing key = nome da fila) e
//! publicar num exchange nomeado (fanout, routing key ignorada). Um metodo so
//! exigiria adivinhar o destino pelo nome, entao virou dois metodos explicitos.

use async_trait::async_trait;

use super::{Recebida, Res};

#[async_trait]
pub trait PortaBroker: Send + Sync {
    async fn declarar_fila(&self, nome: &str) -> Res<()>;
    async fn declarar_fanout(&self, nome: &str) -> Res<()>;
    async fn vincular(&self, fila: &str, exchange: &str) -> Res<()>;
    async fn definir_prefetch(&self, n: u16) -> Res<()>;

    /// Publica na fila pelo exchange default.
    async fn publicar_na_fila(&self, fila: &str, corpo: &[u8]) -> Res<()>;

    /// Publica no exchange nomeado (fanout): o broker copia para todos os binds.
    async fn publicar_no_fanout(&self, exchange: &str, corpo: &[u8]) -> Res<()>;

    /// Consome no maximo um item (prefetch 1), com o delivery tag para o ack.
    async fn consumir(&self, fila: &str) -> Res<Vec<Recebida>>;

    async fn confirmar(&self, tag: u64) -> Res<()>;
}
