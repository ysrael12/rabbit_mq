# syntax=docker/dockerfile:1

# =============================================================================
# Estágio 1 — build
# -----------------------------------------------------------------------------
# rust:slim (Debian) em vez de rust:alpine: os crates `image` e `lapin` são
# Rust puro, o slim já resolve, e o alpine trocaria glibc por musl sem ganho
# (só aumentaria a chance de incompatibilidade). A tag 1.98 casa com o rustc
# local (1.98.1) e com o `edition = "2024"` do Cargo.toml.
# =============================================================================
FROM rust:1.98-slim AS build
WORKDIR /app

# 1) Manifestos antes do código: esta camada só é invalidada quando
#    Cargo.toml/Cargo.lock mudam. O stub `fn main(){}` existe porque o cargo
#    exige um alvo (`src/main.rs`) até para o `fetch`. `cargo fetch --locked`
#    baixa TODAS as dependências do lockfile (a parte mais lenta do build =
#    cache de rede) sem compilar nada.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs \
    && cargo fetch --locked

# 2) Só agora o código real: recompila quando `src/` mudar, reaproveitando o
#    download da camada anterior. `--locked` usa exatamente o Cargo.lock
#    versionado (build reprodutível), sem re-resolver dependências.
COPY src ./src
RUN cargo build --release --locked

# =============================================================================
# Estágio 2 — runtime
# -----------------------------------------------------------------------------
# Imagem final enxuta: só o binário + glibc, sem o toolchain Rust (centenas de
# MB a menos). Não instalamos `ca-certificates`: o fluxo é AMQP puro
# (amqp://), sem TLS — seria peso morto. Se o broker subir com amqps://,
# adicione o pacote aqui.
#
# Por que `trixie` e não `bookworm`? O estágio de build (rust:1.98-slim) é
# baseado no Debian 13 (trixie, glibc 2.41). Um binário compilado lá exige
# GLIBC_2.38+, que o bookworm (glibc 2.36) não tem — a imagem subiria e o
# binário falharia com "GLIBC_2.38 not found". O runtime precisa de glibc
# >= o do build; manter a MESMA família Debian (trixie-slim) garante isso.
# =============================================================================
FROM debian:trixie-slim AS runtime

# Usuário sem privilégio: a imagem não roda como root por padrão. O compose
# sobrescreve com o uid do dono das pastas montadas (bind mount não remapeia
# usuário); este USER é o fallback para um `docker run` avulso.
RUN useradd --system --uid 10001 --no-create-home app

COPY --from=build /app/target/release/rabbit_mq /usr/local/bin/rabbit_mq

USER app
ENTRYPOINT ["rabbit_mq"]
