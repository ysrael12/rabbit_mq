//! Tipos comuns: papel do processo, resultado padrao, contrato dos servicos
//! (Strategy + Template Method, diagrama 08) e o resumo numerico da execucao
//! (modulo `relatorio` do diagrama 05).

use anyhow::Result;
use async_trait::async_trait;

use super::Mensagem;

/// Alias de resultado usado em todo o projeto (modulo `erro`, diagrama 05).
pub type Res<T> = Result<T>;

/// Papel do processo, lido do primeiro argumento (diagrama 07).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Papel {
    Cliente,
    Conversor,
    Storage,
}

impl Papel {
    pub fn de_args(argumentos: &[String]) -> Res<Papel> {
        match argumentos.get(1).map(String::as_str) {
            Some("cliente") => Ok(Papel::Cliente),
            Some("conversor") => Ok(Papel::Conversor),
            Some("storage") => Ok(Papel::Storage),
            outro => anyhow::bail!("papel invalido: {outro:?} (use cliente | conversor | storage)"),
        }
    }

    pub fn nome(&self) -> &'static str {
        match self {
            Papel::Cliente => "cliente",
            Papel::Conversor => "conversor",
            Papel::Storage => "storage",
        }
    }
}

/// Uma mensagem consumida, com o delivery tag para o ack manual.
pub struct Recebida {
    pub mensagem: Mensagem,
    pub tag: u64,
}

/// Resumo de uma execucao (diagrama 05: modulo `relatorio`).
/// O numero e a prova da verificacao; texto bonito nao prova nada.
pub fn relatorio(papel: Papel, total: u64) -> String {
    let verbo = match papel {
        Papel::Cliente => "publicadas",
        Papel::Conversor => "convertidas",
        Papel::Storage => "salvas",
    };
    format!("{}: {} {}", papel.nome(), total, verbo)
}

/// Strategy (cada papel e uma implementacao) + Template Method (`executar`).
///
/// `executar` NAO e reescrito por ninguem: preparar -> (receber -> passo ->
/// confirmar) em laco -> resumo. O ack acontece depois do trabalho, em um
/// unico lugar (diagrama 08).
#[async_trait]
pub trait Servico: Send {
    async fn preparar(&mut self) -> Res<()>;

    /// Default vazio: o cliente le a pasta, os consumidores usam o broker.
    async fn receber(&mut self) -> Res<Vec<Recebida>> {
        Ok(Vec::new())
    }

    async fn passo(&mut self, itens: &[Recebida]) -> Res<usize>;

    async fn confirmar(&mut self) -> Res<()> {
        Ok(())
    }

    fn resumo(&self) -> String;

    async fn executar(&mut self) -> Res<u64> {
        self.preparar().await?;
        let mut total = 0u64;
        loop {
            let itens = self.receber().await?;
            let n = self.passo(&itens).await?;
            if n > 0 {
                self.confirmar().await?;
            }
            total += n as u64;
            if n == 0 {
                break;
            }
        }
        println!("{}", self.resumo());
        Ok(total)
    }
}
