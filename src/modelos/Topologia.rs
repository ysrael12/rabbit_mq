//! Topologia — Mediador (diagrama 09). Ponto unico dos nomes de fila e
//! exchange e da fanout: nenhum modulo escreve nome de fila literal.
//!
//! Depende so do trait `PortaBroker`, nunca do lapin.

use super::{Config, Papel, PortaBroker, Res};

#[derive(Clone)]
pub struct Topologia {
    cfg: Config,
}

impl Topologia {
    pub fn novo(cfg: Config) -> Self {
        Topologia { cfg }
    }

    pub fn nome_fila_originais(&self) -> &str {
        &self.cfg.fila_originais
    }

    pub fn nome_exchange(&self) -> &str {
        &self.cfg.exchange_convertidas
    }

    pub fn nome_fila_storage(&self) -> &str {
        &self.cfg.fila_storage
    }

    /// Deixa pronta a topologia do papel. Todos declaram a fila de entrada e o
    /// exchange; so o storage declara a propria fila e faz o bind — e o bind
    /// que faz o broker copiar a mensagem para mais uma replica.
    pub async fn preparar(&self, broker: &dyn PortaBroker, papel: Papel) -> Res<()> {
        broker.declarar_fila(self.nome_fila_originais()).await?;
        broker.declarar_fanout(self.nome_exchange()).await?;
        if papel == Papel::Storage {
            broker.declarar_fila(self.nome_fila_storage()).await?;
            broker
                .vincular(self.nome_fila_storage(), self.nome_exchange())
                .await?;
        }
        Ok(())
    }
}
