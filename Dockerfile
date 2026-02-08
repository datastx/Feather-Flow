# --- Stage 1: Dependency cache ---
FROM rust:1-slim AS planner
RUN apt-get update && apt-get install -y make && rm -rf /var/lib/apt/lists/*
WORKDIR /src

# Copy only manifests first — this layer is cached until deps change
COPY Cargo.toml Cargo.lock ./
COPY crates/ff-cli/Cargo.toml crates/ff-cli/Cargo.toml
COPY crates/ff-core/Cargo.toml crates/ff-core/Cargo.toml
COPY crates/ff-sql/Cargo.toml crates/ff-sql/Cargo.toml
COPY crates/ff-jinja/Cargo.toml crates/ff-jinja/Cargo.toml
COPY crates/ff-db/Cargo.toml crates/ff-db/Cargo.toml
COPY crates/ff-test/Cargo.toml crates/ff-test/Cargo.toml
COPY crates/ff-analysis/Cargo.toml crates/ff-analysis/Cargo.toml

# Create stub source files so cargo can resolve the workspace
RUN for dir in crates/*/; do mkdir -p "$dir/src" && echo "" > "$dir/src/lib.rs"; done
RUN mkdir -p crates/ff-cli/src && echo "fn main() {}" > crates/ff-cli/src/main.rs

# Build only dependencies (cached until Cargo.toml or Cargo.lock changes)
RUN cargo build --release -p ff-cli || true

# --- Stage 2: Full build ---
FROM planner AS builder
# Copy real source code (invalidates cache only when source changes)
COPY . .
# Touch source files to ensure they're newer than the cached dependency build
RUN find crates -name "*.rs" -exec touch {} +
RUN cargo build --release -p ff-cli

# --- Stage 3: Runtime ---
FROM debian:bookworm-slim AS runtime
RUN groupadd -r ff && useradd -r -g ff ff
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/ff /usr/local/bin/ff

LABEL org.opencontainers.image.source="https://github.com/datastx/Feather-Flow"
LABEL org.opencontainers.image.description="Feather-Flow: a lightweight dbt-like CLI for SQL transformation"

USER ff
ENTRYPOINT ["ff"]
