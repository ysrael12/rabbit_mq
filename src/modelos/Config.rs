//! Config — seis variaveis de ambiente, um lugar so (diagrama 05).
//!
//! ```text
//! class "Config" {
//!   + amqp_url, fila_originais, exchange_convertidas, fila_storage
//!   + pasta_entrada, pasta_saida : PathBuf
//!   + from_env(papel: Papel) : Result<Config>
//! }
//! ```

use std::path::PathBuf;

use anyhow::Result;

use super::Papel;

#[derive(Clone, Debug)]
pub struct Config {
    pub amqp_url: String,
    pub fila_originais: String,
    pub exchange_convertidas: String,
    pub fila_storage: String,
    pub pasta_entrada: PathBuf,
    pub pasta_saida: PathBuf,
}

fn ambiente(chave: &str, padrao: &str) -> String {
    std::env::var(chave).unwrap_or_else(|_| padrao.to_string())
}

impl Config {
    /// Le o ambiente. As pastas padrao mudam com o papel, porque cada instancia
    /// do compose tem o seu bind mount (nao se usa `--scale`).
    pub fn from_env(papel: Papel) -> Result<Config> {
        let padrao_entrada = match papel {
            Papel::Cliente => "./pastas-cliente/cliente1",
            _ => "./entrada",
        };
        let padrao_saida = match papel {
            Papel::Storage => "./pastas-storage/storage1",
            _ => "./saida",
        };

        Ok(Config {
            amqp_url: ambiente("AMQP_URL", "amqp://guest:guest@localhost:5672/%2f"),
            fila_originais: ambiente("FILA_ORIGINAIS", "fila.originais"),
            exchange_convertidas: ambiente("EXCHANGE_CONVERTIDAS", "convertidas"),
            fila_storage: ambiente("FILA_STORAGE", "fila.storage.1"),
            pasta_entrada: PathBuf::from(ambiente("PASTA_ENTRADA", padrao_entrada)),
            pasta_saida: PathBuf::from(ambiente("PASTA_SAIDA", padrao_saida)),
        })
    }
}
