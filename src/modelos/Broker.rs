//! Broker — Fachada do lapin (diagramas 09 e 10). Unico arquivo que nomeia
//! um tipo do lapin; implementa `PortaBroker` e esconde o async e o `Result`
//! por dentro, alem do `delivery_mode = PERSISTENT` e do prefetch.

use std::sync::Mutex;

use async_trait::async_trait;
use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
};

use super::{Config, Mensagem, PortaBroker, Recebida, Res};

pub struct Broker {
    canal: Channel,
    /// O consumidor e aberto na primeira chamada de `consumir`, na fila pedida.
    consumidor: Mutex<Option<(String, Consumer)>>,
}

impl Broker {
    pub async fn conectar(cfg: &Config) -> Res<Broker> {
        let conexao = Connection::connect(&cfg.amqp_url, ConnectionProperties::default()).await?;
        Ok(Broker {
            canal: conexao.create_channel().await?,
            consumidor: Mutex::new(None),
        })
    }
}

/// delivery_mode = 2 (persistente): sem isso a mensagem morre com o broker.
fn propriedades_persistentes() -> lapin::BasicProperties {
    lapin::BasicProperties::default().with_delivery_mode(2)
}

#[async_trait]
impl PortaBroker for Broker {
    async fn declarar_fila(&self, nome: &str) -> Res<()> {
        self.canal
            .queue_declare(
                nome.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }

    async fn declarar_fanout(&self, nome: &str) -> Res<()> {
        self.canal
            .exchange_declare(
                nome.into(),
                ExchangeKind::Fanout,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }

    async fn vincular(&self, fila: &str, exchange: &str) -> Res<()> {
        self.canal
            .queue_bind(
                fila.into(),
                exchange.into(),
                "".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }

    async fn definir_prefetch(&self, n: u16) -> Res<()> {
        self.canal.basic_qos(n, BasicQosOptions::default()).await?;
        Ok(())
    }

    async fn publicar_na_fila(&self, fila: &str, corpo: &[u8]) -> Res<()> {
        self.canal
            .basic_publish(
                "".into(),
                fila.into(),
                BasicPublishOptions::default(),
                corpo,
                propriedades_persistentes(),
            )
            .await?
            .await?;
        Ok(())
    }

    async fn publicar_no_fanout(&self, exchange: &str, corpo: &[u8]) -> Res<()> {
        self.canal
            .basic_publish(
                exchange.into(),
                "".into(),
                BasicPublishOptions::default(),
                corpo,
                propriedades_persistentes(),
            )
            .await?
            .await?;
        Ok(())
    }

    async fn consumir(&self, fila: &str) -> Res<Vec<Recebida>> {
        let abrir = {
            let guard = self.consumidor.lock().expect("consumidor envenenado");
            guard.as_ref().is_none_or(|(nome, _)| nome != fila)
        };
        if abrir {
            let consumidor = self
                .canal
                .basic_consume(
                    fila.into(),
                    fila.into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?;
            *self.consumidor.lock().expect("consumidor envenenado") =
                Some((fila.to_string(), consumidor));
        }

        let mut consumidor = {
            let guard = self.consumidor.lock().expect("consumidor envenenado");
            guard
                .as_ref()
                .map(|(_, consumidor)| consumidor.clone())
                .expect("consumidor aberto acima")
        };

        match consumidor.next().await {
            Some(Ok(entrega)) => Ok(vec![Recebida {
                mensagem: Mensagem::ler(entrega.data.clone())?,
                tag: entrega.delivery_tag,
            }]),
            Some(Err(erro)) => Err(erro.into()),
            None => Ok(Vec::new()),
        }
    }

    async fn confirmar(&self, tag: u64) -> Res<()> {
        self.canal.basic_ack(tag, BasicAckOptions::default()).await?;
        Ok(())
    }
}
