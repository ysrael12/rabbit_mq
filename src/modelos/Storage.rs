//! Storage — consome a propria fila de storage e grava com o nome original.
//!
//! ```text
//! class "Storage" {
//!   - cfg : Config   - broker : PortaBroker   - salvas : u64
//!   + novo(cfg, broker) : Result<Storage>
//!   + executar(&mut self) : Result<u64>   (Template Method)
//!   - salvar(nome, bytes) : Result<PathBuf>
//! }
//! ```

use std::path::{Path, PathBuf};

use anyhow::Context;
use async_trait::async_trait;

use super::{relatorio, Config, Papel, PortaBroker, Recebida, Res, Servico, Topologia};

pub struct Storage {
    cfg: Config,
    broker: Box<dyn PortaBroker>,
    salvas: u64,
    topologia: Topologia,
}

impl Storage {
    /// Construcao pura: a conexao fica com o `Broker` (injetado).
    pub fn novo(cfg: Config, broker: Box<dyn PortaBroker>) -> Res<Storage> {
        let topologia = Topologia::novo(cfg.clone());
        Ok(Storage {
            cfg,
            broker,
            salvas: 0,
            topologia,
        })
    }

    /// Grava com o nome vindo da mensagem, sem uuid e sem renomear. O nome
    /// chega pelo broker, entao so o componente final do caminho e usado
    /// (defesa contra `../`).
    fn salvar(&mut self, nome: &str, bytes: &[u8]) -> Res<PathBuf> {
        let nome = Path::new(nome)
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| !n.is_empty())
            .with_context(|| format!("nome de arquivo invalido: {nome:?}"))?;

        std::fs::create_dir_all(&self.cfg.pasta_saida)?;
        let destino = self.cfg.pasta_saida.join(nome);
        std::fs::write(&destino, bytes)?;
        self.salvas += 1;
        Ok(destino)
    }
}

#[async_trait]
impl Servico for Storage {
    async fn preparar(&mut self) -> Res<()> {
        self.topologia
            .preparar(self.broker.as_ref(), Papel::Storage)
            .await?;
        self.broker.definir_prefetch(1).await
    }

    async fn receber(&mut self) -> Res<Vec<Recebida>> {
        let fila = self.topologia.nome_fila_storage().to_string();
        self.broker.consumir(&fila).await
    }

    async fn passo(&mut self, itens: &[Recebida]) -> Res<usize> {
        for item in itens {
            self.salvar(&item.mensagem.nome, &item.mensagem.bytes)?;
            self.broker.confirmar(item.tag).await?;
        }
        Ok(itens.len())
    }

    fn resumo(&self) -> String {
        relatorio(Papel::Storage, self.salvas)
    }
}
