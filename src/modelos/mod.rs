//! Camada de modelo — o binario unico (diagrama 05, refinado por 07..11).
//!
//! Um arquivo por classe do diagrama: `Config`, `Mensagem`, `Topologia`,
//! `Cliente`, `Conversor`, `Storage`, alem dos modulos auxiliares `Broker`
//! (fachada do lapin), `PortaBroker` (porta que ele implementa), `io_imagem`
//! (adaptador do crate `image`) e `utils` (papel, resultado padrao, contrato
//! `Servico` e formatacao do resumo).
//!
//! Os arquivos ficam com inicial maiuscula (nome da classe no diagrama); o
//! nome do modulo Rust e o `#[path]` que normalizam.

#[path = "Broker.rs"]
mod broker;
#[path = "Cliente.rs"]
mod cliente;
#[path = "Config.rs"]
mod config;
#[path = "Conversor.rs"]
mod conversor;
#[path = "Mensagem.rs"]
mod mensagem;
#[path = "PortaBroker.rs"]
mod porta_broker;
#[path = "Storage.rs"]
mod storage;
#[path = "Topologia.rs"]
mod topologia;
mod io_imagem;
mod utils;

pub use broker::Broker;
pub use cliente::Cliente;
pub use config::Config;
pub use conversor::Conversor;
pub use mensagem::Mensagem;
pub use porta_broker::PortaBroker;
pub use storage::Storage;
pub use topologia::Topologia;
pub use utils::{relatorio, Papel, Recebida, Res, Servico};
