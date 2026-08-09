FROM rust:1-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin forgekv-server --bin forgekv-cli

FROM debian:bookworm-slim AS runtime

RUN groupadd --system forgekv \
    && useradd --system --gid forgekv --home-dir /nonexistent --shell /usr/sbin/nologin forgekv \
    && mkdir --parents /data \
    && chown forgekv:forgekv /data

COPY --from=builder /build/target/release/forgekv-server /usr/local/bin/forgekv-server
COPY --from=builder /build/target/release/forgekv-cli /usr/local/bin/forgekv-cli

USER forgekv:forgekv
WORKDIR /data

ENV FORGEKV_HOST=0.0.0.0 \
    FORGEKV_PORT=6380 \
    FORGEKV_DATA_DIR=/data \
    FORGEKV_METRICS_HOST=0.0.0.0 \
    RUST_LOG=info

VOLUME ["/data"]
EXPOSE 6380
EXPOSE 9090

ENTRYPOINT ["forgekv-server"]
