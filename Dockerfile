FROM rust:1.96-bookworm AS build

ENV RUSTUP_TOOLCHAIN=1.96.0
WORKDIR /src
COPY . .
RUN cargo build --locked --release \
    -p henosis-connector-k8s-server \
    -p henosis-k8s-contract-harness

FROM node:22.23.1-bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl git util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && corepack enable

RUN install -d -m 0750 /var/lib/henosis-connector-k8s

FROM runtime AS contract-harness

COPY --from=build /src/target/release/henosis-k8s-contract-harness /usr/local/bin/henosis-k8s-contract-harness
ENTRYPOINT ["/usr/local/bin/henosis-k8s-contract-harness"]

FROM runtime AS final

COPY --from=build /src/target/release/henosis-connector-k8s-server /usr/local/bin/henosis-connector-k8s

EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/henosis-connector-k8s"]
