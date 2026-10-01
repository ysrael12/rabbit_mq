//! Factory Method (diagrama 07): quem sabe construir cada servico.
//!
//! `FabricaReal` monta o `Broker` do lapin; `FabricaFalsa` (nos testes) monta
//! servicos sobre um broker em memoria, sem RabbitMQ. Adicionar a familia
//! falsa e o motivo de existir o trait `FabricaServico`.
//!
//! Desvio consciente do diagrama 07: `FabricaServico::criar` recebe o `Papel`
//! alem do `Config` — sem ele seriam tres fabricas, uma por papel.

use async_trait::async_trait;

use crate::modelos::{Broker, Cliente, Config, Conversor, Papel, PortaBroker, Res, Servico, Storage};

#[async_trait]
pub trait FabricaServico: Send + Sync {
    async fn criar(&self, papel: Papel, cfg: Config) -> Res<Box<dyn Servico>>;
}

/// Familia real: lapin + crate `image`.
pub struct FabricaReal;

impl FabricaReal {
    fn montar(papel: Papel, cfg: Config, broker: Box<dyn PortaBroker>) -> Res<Box<dyn Servico>> {
        Ok(match papel {
            Papel::Cliente => Box::new(Cliente::novo(cfg, broker)?),
            Papel::Conversor => Box::new(Conversor::novo(cfg, broker)?),
            Papel::Storage => Box::new(Storage::novo(cfg, broker)?),
        })
    }
}

#[async_trait]
impl FabricaServico for FabricaReal {
    async fn criar(&self, papel: Papel, cfg: Config) -> Res<Box<dyn Servico>> {
        let broker = Box::new(Broker::conectar(&cfg).await?);
        Self::montar(papel, cfg, broker)
    }
}

/// Forma simples do padrao: uma linha de criacao para o `main`.
pub async fn criar(papel: Papel, cfg: Config) -> Res<Box<dyn Servico>> {
    FabricaReal.criar(papel, cfg).await
}

#[cfg(test)]
mod testes {
    use std::{
        collections::{HashMap, VecDeque},
        io::Cursor,
        path::Path,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::modelos::{Mensagem, Recebida};

    /// Broker em memoria: sem RabbitMQ, sem rede. Guarda o que foi declarado,
    /// publicado e confirmado para o teste poder provar a ordem.
    #[derive(Clone, Default)]
    struct BrokerFalso {
        estado: Arc<Mutex<Estado>>,
    }

    #[derive(Default)]
    struct Estado {
        filas: HashMap<String, VecDeque<Vec<u8>>>,
        vinculos: Vec<(String, String)>,
        em_voo: VecDeque<u64>,
        confirmadas: Vec<u64>,
        publicacoes: usize,
        proxima_tag: u64,
    }

    #[async_trait]
    impl PortaBroker for BrokerFalso {
        async fn declarar_fila(&self, nome: &str) -> Res<()> {
            self.estado
                .lock()
                .unwrap()
                .filas
                .entry(nome.to_string())
                .or_default();
            Ok(())
        }

        async fn declarar_fanout(&self, _nome: &str) -> Res<()> {
            Ok(())
        }

        async fn vincular(&self, fila: &str, exchange: &str) -> Res<()> {
            let mut estado = self.estado.lock().unwrap();
            estado
                .vinculos
                .push((fila.to_string(), exchange.to_string()));
            estado.filas.entry(fila.to_string()).or_default();
            Ok(())
        }

        async fn definir_prefetch(&self, _n: u16) -> Res<()> {
            Ok(())
        }

        async fn publicar_na_fila(&self, fila: &str, corpo: &[u8]) -> Res<()> {
            let mut estado = self.estado.lock().unwrap();
            estado.publicacoes += 1;
            estado
                .filas
                .entry(fila.to_string())
                .or_default()
                .push_back(corpo.to_vec());
            Ok(())
        }

        async fn publicar_no_fanout(&self, exchange: &str, corpo: &[u8]) -> Res<()> {
            let mut estado = self.estado.lock().unwrap();
            estado.publicacoes += 1;
            let destinos: Vec<String> = estado
                .vinculos
                .iter()
                .filter(|(_, ex)| ex == exchange)
                .map(|(fila, _)| fila.clone())
                .collect();
            for fila in destinos {
                estado
                    .filas
                    .get_mut(&fila)
                    .expect("fila vinculada foi declarada")
                    .push_back(corpo.to_vec());
            }
            Ok(())
        }

        async fn consumir(&self, fila: &str) -> Res<Vec<Recebida>> {
            let mut estado = self.estado.lock().unwrap();
            let Some(corpo) = estado
                .filas
                .get_mut(fila)
                .and_then(|fila| fila.pop_front())
            else {
                return Ok(Vec::new());
            };
            estado.proxima_tag += 1;
            let tag = estado.proxima_tag;
            estado.em_voo.push_back(tag);
            Ok(vec![Recebida {
                mensagem: Mensagem::ler(corpo)?,
                tag,
            }])
        }

        async fn confirmar(&self, tag: u64) -> Res<()> {
            let mut estado = self.estado.lock().unwrap();
            let pos = estado
                .em_voo
                .iter()
                .position(|pendente| *pendente == tag)
                .expect("ack de uma mensagem que nao estava em voo");
            estado.em_voo.remove(pos);
            estado.confirmadas.push(tag);
            Ok(())
        }
    }

    /// Portas falsas: mesma familia de servicos, broker em memoria.
    struct FabricaFalsa {
        broker: BrokerFalso,
    }

    #[async_trait]
    impl FabricaServico for FabricaFalsa {
        async fn criar(&self, papel: Papel, cfg: Config) -> Res<Box<dyn Servico>> {
            FabricaReal::montar(papel, cfg, Box::new(self.broker.clone()))
        }
    }

    fn cfg(entrada: &Path, saida: &Path, fila_storage: &str) -> Config {
        Config {
            amqp_url: "amqp://ignorado".into(),
            fila_originais: "fila.originais".into(),
            exchange_convertidas: "convertidas".into(),
            fila_storage: fila_storage.into(),
            pasta_entrada: entrada.to_path_buf(),
            pasta_saida: saida.to_path_buf(),
        }
    }

    #[tokio::test]
    async fn fluxo_completo_sem_broker() -> Res<()> {
        let raiz = std::env::temp_dir().join(format!("rabbit_mq_teste_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&raiz);
        let entrada = raiz.join("cliente1");
        let saida1 = raiz.join("storage1");
        let saida2 = raiz.join("storage2");
        std::fs::create_dir_all(&entrada)?;

        // uma imagem colorida 2x2: a conversao tem que devolver cinza
        let imagem = image::RgbImage::from_fn(2, 2, |x, y| {
            image::Rgb([(x * 120) as u8, (y * 200) as u8, 40])
        });
        let mut png = Cursor::new(Vec::new());
        image::DynamicImage::ImageRgb8(imagem).write_to(&mut png, image::ImageFormat::Png)?;
        std::fs::write(entrada.join("foto.png"), png.into_inner())?;

        let broker = BrokerFalso::default();
        let fabrica = FabricaFalsa {
            broker: broker.clone(),
        };

        // os storages primeiro: e o bind delas que faz o fanout copiar
        let mut storage1 = fabrica
            .criar(Papel::Storage, cfg(&entrada, &saida1, "fila.storage.1"))
            .await?;
        let mut storage2 = fabrica
            .criar(Papel::Storage, cfg(&entrada, &saida2, "fila.storage.2"))
            .await?;
        storage1.preparar().await?;
        storage2.preparar().await?;

        let mut cliente = fabrica
            .criar(Papel::Cliente, cfg(&entrada, &saida1, "fila.storage.1"))
            .await?;
        let mut conversor = fabrica
            .criar(Papel::Conversor, cfg(&entrada, &saida1, "fila.storage.1"))
            .await?;

        cliente.executar().await?;
        conversor.executar().await?;
        storage1.executar().await?;
        storage2.executar().await?;

        // R4/R5: mesma imagem, mesmo nome, nas duas replicas
        let replica1 = std::fs::read(saida1.join("foto.png"))?;
        let replica2 = std::fs::read(saida2.join("foto.png"))?;
        assert_eq!(replica1, replica2);

        // conversao de verdade, nao so copia
        let cinza = image::load_from_memory(&replica1)?.to_rgb8();
        assert!(cinza.pixels().all(|p| p[0] == p[1] && p[1] == p[2]));

        // ack so depois do trabalho: nada ficou em voo
        let estado = broker.estado.lock().unwrap();
        assert!(estado.em_voo.is_empty());
        assert_eq!(estado.confirmadas.len(), 3); // 1 conversor + 2 storages
        assert_eq!(estado.publicacoes, 2); // 1 cliente + 1 conversor

        drop(estado);
        std::fs::remove_dir_all(&raiz)?;
        Ok(())
    }
}
