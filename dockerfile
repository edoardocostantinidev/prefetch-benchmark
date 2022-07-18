FROM rust:1.62 AS builder
COPY . .
RUN cargo build --release

FROM debian:buster-slim
COPY --from=builder ./target/release/prefetch-benchmark ./target/release/prefetch-benchmark

CMD ["/target/release/prefetch-benchmark"]
