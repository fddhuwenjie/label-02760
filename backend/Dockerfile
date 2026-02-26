# Build stage - uses native compilation for the target platform
FROM rust:1.75-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for dependency caching
COPY Cargo.toml ./

# Create dummy main.rs to cache dependencies
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo build --release || true

# Copy actual source code
COPY src ./src

# Build the application
RUN touch src/main.rs && cargo build --release

# Runtime stage - minimal image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/kafka-relay /app/kafka-relay

EXPOSE 9092

ENTRYPOINT ["/app/kafka-relay"]
CMD ["--listen-host", "0.0.0.0", "--listen-port", "9092", "--upstream-host", "kafka", "--upstream-port", "9093"]
