//! Ponto de entrada: args -> Papel -> Config -> criar (Factory Method) ->
//! Invocador (Command) -> executar (Template Method).

mod controladores;
mod modelos;

use anyhow::Result;
use controladores::{criar, Invocador};
use modelos::{Config, Papel};

#[tokio::main]
async fn main() -> Result<()> {
    let argumentos: Vec<String> = std::env::args().collect();
    let papel = Papel::de_args(&argumentos)?;
    let cfg = Config::from_env(papel)?;

    let invocador = Invocador::novo(papel, criar(papel, cfg).await?);
    println!("[{}] pedido aceito", invocador.pedido().nome());

    let total = invocador.executar().await?;
    println!("total: {total}");
    Ok(())
}
