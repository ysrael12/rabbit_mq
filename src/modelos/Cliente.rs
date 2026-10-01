//! Cliente — publica cada imagem da pasta de entrada na fila.originais.
//!
//! ```text
//! class "Cliente" {
//!   - cfg : Config   - broker : PortaBroker   - publicadas : u64
//!   + novo(cfg, broker) : Result<Cliente>
//!   + executar(&mut self) : Result<u64>   (Template Method)
//!   - listar_imagens() : Result<Vec<PathBuf>>
//!   - publicar(nome, bytes) : Result
//! }
//! ```

use std::path::PathBuf;

use async_trait::async_trait;

use super::{
    io_imagem, relatorio, Config, Mensagem, Papel, PortaBroker, Recebida, Res, Servico, Topologia,
};

pub struct Cliente {
    cfg: Config,
    broker: Box<dyn PortaBroker>,
    publicadas: u64,
    topologia: Topologia,
    lidas: bool,
}

impl Cliente {
    /// Construcao pura: a conexao fica com o `Broker` (injetado).
    pub fn novo(cfg: Config, broker: Box<dyn PortaBroker>) -> Res<Cliente> {
        let topologia = Topologia::novo(cfg.clone());
        Ok(Cliente {
            cfg,
            broker,
            publicadas: 0,
            topologia,
            lidas: false,
        })
    }

    async fn publicar(&mut self, nome: String, bytes: Vec<u8>) -> Res<()> {
        let corpo = Mensagem::nova(nome, bytes).escrever();
        let fila = self.topologia.nome_fila_originais().to_string();
        self.broker.publicar_na_fila(&fila, &corpo).await?;
        self.publicadas += 1;
        Ok(())
    }

    fn listar_imagens(&self) -> Res<Vec<PathBuf>> {
        let mut caminhos = Vec::new();
        for entrada in std::fs::read_dir(&self.cfg.pasta_entrada).map_err(|e| {
            anyhow::anyhow!(
                "pasta de entrada {} inacessivel: {e}",
                self.cfg.pasta_entrada.display()
            )
        })? {
            let caminho = entrada?.path();
            if !caminho.is_file() {
                continue;
            }
            let Some(extensao) = io_imagem::extensao(&caminho.to_string_lossy()) else {
                continue;
            };
            if io_imagem::FORMATOS.contains(&extensao.as_str()) {
                caminhos.push(caminho);
            }
        }
        caminhos.sort();
        Ok(caminhos)
    }
}

#[async_trait]
impl Servico for Cliente {
    async fn preparar(&mut self) -> Res<()> {
        self.topologia
            .preparar(self.broker.as_ref(), Papel::Cliente)
            .await
    }

    /// Le a pasta uma unica vez; a segunda chamada encerra o laco do template.
    async fn receber(&mut self) -> Res<Vec<Recebida>> {
        if self.lidas {
            return Ok(Vec::new());
        }
        self.lidas = true;
        let mut itens = Vec::new();
        for caminho in self.listar_imagens()? {
            let nome = caminho
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            itens.push(Recebida {
                mensagem: Mensagem::nova(nome, std::fs::read(&caminho)?),
                tag: 0,
            });
        }
        Ok(itens)
    }

    async fn passo(&mut self, itens: &[Recebida]) -> Res<usize> {
        for item in itens {
            self.publicar(item.mensagem.nome.clone(), item.mensagem.bytes.clone())
                .await?;
        }
        Ok(itens.len())
    }

    fn resumo(&self) -> String {
        relatorio(Papel::Cliente, self.publicadas)
    }
}
