//! Controladores — quem constroi e quem dispara (diagramas 07 e 09).
//!
//! O `main` so conhece `Papel` e o trait `Servico`; a escolha da familia de
//! implementacoes mora em `criar` e o disparo em `Invocador`.

mod criar;
mod invocador;

pub use criar::criar;
pub use invocador::Invocador;
