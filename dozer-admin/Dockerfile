FROM rust:1.63

RUN cargo install diesel_cli --no-default-features --features sqlite

RUN cargo install cargo-watch

WORKDIR /usr/src/dozer-orchestration

EXPOSE 3001

VOLUME ["/usr/local/cargo"]