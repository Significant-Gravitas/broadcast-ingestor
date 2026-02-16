FROM rust:1.93.1-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/

RUN cargo build --release

FROM chainguard/glibc-dynamic:latest

COPY --from=builder /app/target/release/trace-ingestor /usr/local/bin/trace-ingestor

ENV RUST_LOG=info

EXPOSE 8080
ENTRYPOINT ["trace-ingestor"]
