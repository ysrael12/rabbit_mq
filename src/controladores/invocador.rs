//! Command (diagrama 09): o pedido do usuario vira um objeto e o `main` nao
//! conhece o interior.
//!
//! `Pedido` e o mesmo enum `Papel` — nao se duplica. Adicionar "inspecionar"
//! e um braco novo no enum mais uma implementacao de `Servico`, sem `if` novo
//! dentro dos outros comandos.

use crate::modelos::{Papel, Res, Servico};

pub struct Invocador {
    servico: Box<dyn Servico>,
    pedido: Papel,
}

impl Invocador {
    pub fn novo(pedido: Papel, servico: Box<dyn Servico>) -> Invocador {
        Invocador { servico, pedido }
    }

    pub fn pedido(&self) -> Papel {
        self.pedido
    }

    pub async fn executar(mut self) -> Res<u64> {
        self.servico.executar().await
    }
}
