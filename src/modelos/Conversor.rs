//! Conversor — consome fila.originais, converte e publica no exchange fanout.
//!
//! ```text
//! class "Conversor" {
//!   - cfg : Config   - broker : PortaBroker   - processadas : u64
//!   + novo(cfg, broker) : Result<Conversor>
//!   + executar(&mut self) : Result<u64>   (Template Method)
//!   - tratar(payload, tag) : Result
//! }
//! ```
//!
//! Ordem do `tratar`: converter -> publicar -> ack. Ack depois do publish =
//! at-least-once. Se o processo cair entre os dois, a mensagem volta e o
//! convertido e regravado com o MESMO nome (sobrescreve, sem duplicar).

use async_trait::async_trait;

use super::{
    io_imagem, relatorio, Config, Mensagem, Papel, PortaBroker, Recebida, Res, Servico, Topologia,
};

pub struct Conversor {
    broker: Box<dyn PortaBroker>,
    processadas: u64,
    topologia: Topologia,
}

impl Conversor {
    /// Construcao pura: a conexao fica com o `Broker` (injetado).
    pub fn novo(cfg: Config, broker: Box<dyn PortaBroker>) -> Res<Conversor> {
        let topologia = Topologia::novo(cfg);
        Ok(Conversor {
            broker,
            processadas: 0,
            topologia,
        })
    }

    /// Converte, publica no exchange e confirma. `tag` e o delivery tag da
    /// mensagem; o ack so acontece aqui, depois do trabalho feito.
    async fn tratar(&mut self, payload: Vec<u8>, tag: u64) -> Res<()> {
        let mensagem = Mensagem::ler(payload)?;
        let convertida = io_imagem::converter(mensagem.bytes)?;

        let exchange = self.topologia.nome_exchange().to_string();
        self.broker
            .publicar_no_fanout(&exchange, &Mensagem::nova(mensagem.nome, convertida).escrever())
            .await?;
        self.broker.confirmar(tag).await?;

        self.processadas += 1;
        Ok(())
    }
}

#[async_trait]
impl Servico for Conversor {
    async fn preparar(&mut self) -> Res<()> {
        self.topologia
            .preparar(self.broker.as_ref(), Papel::Conversor)
            .await?;
        self.broker.definir_prefetch(1).await
    }

    async fn receber(&mut self) -> Res<Vec<Recebida>> {
        let fila = self.topologia.nome_fila_originais().to_string();
        self.broker.consumir(&fila).await
    }

    async fn passo(&mut self, itens: &[Recebida]) -> Res<usize> {
        for item in itens {
            self.tratar(item.mensagem.escrever(), item.tag).await?;
        }
        Ok(itens.len())
    }

    fn resumo(&self) -> String {
        relatorio(Papel::Conversor, self.processadas)
    }
}
