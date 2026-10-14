FROM golang:1.26-bookworm AS builder

# Install build dependencies (CGO required for DuckDB)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git make gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy go module files first for layer caching
COPY go.mod go.sum ./
RUN GOWORK=off go mod download

# Copy source code
COPY . .

# Build binary with version from build arg. The legacy executable name is the
# same program under a transitional alias; see docs/compatibility.md.
ARG VERSION=dev
RUN CGO_ENABLED=1 GOWORK=off go build -ldflags "-X main.Version=${VERSION}" -o stellar-ingest . && \
    cp stellar-ingest stellar-history-loader

FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 stellar && \
    useradd -u 1000 -g stellar -s /bin/bash -m stellar

WORKDIR /app

# Copy both executable names
COPY --from=builder /build/stellar-ingest /usr/local/bin/stellar-ingest
COPY --from=builder /build/stellar-history-loader /usr/local/bin/stellar-history-loader

# Bronze DDL, referenced by --ducklake-schema-sql
COPY --from=builder /build/v3_bronze_schema.sql /app/v3_bronze_schema.sql

# Create output directory
RUN mkdir -p /data/output && chown -R stellar:stellar /app /data

USER stellar

ENTRYPOINT ["stellar-ingest"]
