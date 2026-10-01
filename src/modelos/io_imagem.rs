//! Modulo auxiliar `io_imagem` (diagramas 05 e 10): adaptador do crate `image`.
//!
//! O dominio conhece "converter para tons de cinza", nao `DynamicImage` nem
//! `ImageFormat`. Trocar de crate de imagem mexe so aqui.

use std::io::Cursor;
use std::path::Path;

use image::ImageFormat;

use super::Res;

/// Extensoes que o cliente varre na pasta de entrada.
pub const FORMATOS: [&str; 8] = ["jpg", "jpeg", "png", "webp", "bmp", "gif", "tif", "tiff"];

/// Le os bytes de qualquer formato suportado e devolve tons de cinza em PNG.
// ponytail: sempre PNG no corpo; o nome do arquivo original e mantido (R5).
// Se o formato de saida importar, escolher o encoder por `extensao(nome)`.
pub fn converter(bytes: Vec<u8>) -> Res<Vec<u8>> {
    let imagem = image::load_from_memory(&bytes)?;
    let cinza = imagem.to_luma8();
    let mut saida = Cursor::new(Vec::new());
    cinza.write_to(&mut saida, ImageFormat::Png)?;
    Ok(saida.into_inner())
}

/// Extensao em minusculas, quando houver.
pub fn extensao(nome: &str) -> Option<String> {
    Path::new(nome)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
}
