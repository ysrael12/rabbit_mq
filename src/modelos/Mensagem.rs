//! Payload sem dependencia nova (nota do diagrama 05): 4 bytes big-endian com
//! o tamanho do nome, o nome UTF-8 e os bytes da imagem. Evita serde/base64 e
//! mantem o nome original junto do arquivo — e o que faz o storage gravar com
//! o mesmo nome (R5).

use anyhow::{ensure, Context};

use super::Res;

const CABECALHO: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mensagem {
    pub nome: String,
    pub bytes: Vec<u8>,
}

impl Mensagem {
    pub fn nova(nome: impl Into<String>, bytes: Vec<u8>) -> Self {
        Mensagem {
            nome: nome.into(),
            bytes,
        }
    }

    pub fn escrever(&self) -> Vec<u8> {
        let nome = self.nome.as_bytes();
        let mut saida = Vec::with_capacity(CABECALHO + nome.len() + self.bytes.len());
        saida.extend_from_slice(&(nome.len() as u32).to_be_bytes());
        saida.extend_from_slice(nome);
        saida.extend_from_slice(&self.bytes);
        saida
    }

    pub fn ler(payload: Vec<u8>) -> Res<Mensagem> {
        ensure!(
            payload.len() >= CABECALHO,
            "payload curto: {} bytes (minimo {CABECALHO})",
            payload.len()
        );
        let tamanho = u32::from_be_bytes(payload[..CABECALHO].try_into().unwrap()) as usize;
        ensure!(
            payload.len() >= CABECALHO + tamanho,
            "cabecalho diz {tamanho} bytes de nome, payload tem {}",
            payload.len() - CABECALHO
        );
        let nome = String::from_utf8(payload[CABECALHO..CABECALHO + tamanho].to_vec())
            .context("nome da mensagem nao e UTF-8")?;
        Ok(Mensagem {
            nome,
            bytes: payload[CABECALHO + tamanho..].to_vec(),
        })
    }
}

#[cfg(test)]
mod testes {
    use super::*;

    #[test]
    fn ida_e_volta() {
        let original = Mensagem::nova("foto.jpg", vec![0x89, b'P', b'N', b'G']);
        let lida = Mensagem::ler(original.escrever()).unwrap();
        assert_eq!(original, lida);
    }

    #[test]
    fn recusa_payload_truncado() {
        assert!(Mensagem::ler(vec![0, 0]).is_err());
        // cabecalho promete 10 bytes de nome, so vem 2
        assert!(Mensagem::ler(vec![0, 0, 0, 10, b'a', b'b']).is_err());
    }
}
