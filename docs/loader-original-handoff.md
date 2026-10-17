<!--
Preserved from obsrvr-lake/stellar-history-loader/HANDOFF.md when that
duplicate copy was retired (2026-10-17) and stellar-ingest became the single
canonical loader.

Historical document. Specifics have drifted -- it says 15 Go files and 19
extractors; this repository has 35 and covers all 21 Bronze tables -- but the
architecture it describes is still the shape of the loader, and it is the
clearest single statement of that design. Read it for the pipeline, not for
the counts.
-->

# stellar-history-loader — Developer Handoff

## What Was Built

A parallel batch tool for processing Stellar blockchain history into Parquet/DuckLake at 100x the speed of the current streaming ingester.

### Architecture

```
Data Source (GCS/S3/RPC/Captive Core/XDR/FS)
    ↓
Orchestrator (splits range into N shards)
    ↓
N Workers (parallel goroutines)
    ↓ per worker:
    Prefetch Buffer (200 ledgers ahead)
    ↓
    XDR Decode (ONCE per ledger)
    ↓
    19 Parallel Extractors (goroutines)
    ↓
    Batch Accumulate → Parquet Writer
    ↓
Silver Transforms (DuckDB SQL over Parquet)
```

### Files (15 Go files, 6,198 lines)

| File | Purpose |
|------|---------|
| `main.go` | CLI entry point |
| `orchestrator.go` | Shard splitting, worker lifecycle, progress |
| `worker.go` | Pipeline: prefetch → decode → extract → write |
| `types.go` | 19 data types + LedgerMeta, BatchData |
| `extractors_core.go` | Transactions, operations, effects, trades |
| `extractors_accounts.go` | Accounts, trustlines, signers, native balances |
| `extractors_market.go` | Offers, claimable balances, liquidity pools |
| `extractors_state.go` | Config settings, TTL, evicted keys |
| `extractors_soroban.go` | Contract events, data, code, restored keys, creations |
| `scval_converter.go` | Soroban ScVal → JSON decoder |
| `parquet_writer.go` | Generic Parquet writer with 19 table schemas |
| `silver.go` | DuckDB SQL transforms for silver layer |
| `source_stream.go` | Canonical RPC/GCS/S3/Captive Core streaming adapter with bounded-range validation |
| `catalog.go` | Output report with DuckDB query examples |

### Data Sources

| Type | Flag | Description |
|------|------|-------------|
| `GCS` | `--storage-type GCS --bucket path` | Google Cloud Storage archive |
| `S3` | `--storage-type S3 --bucket path` | Amazon S3 archive |
| `RPC` | `--storage-type RPC --bucket url` | Stellar RPC endpoint |
| `CAPTIVE_CORE` | `--storage-type CAPTIVE_CORE` | Captive Core configured through environment variables |
| `XDR` | `--storage-type XDR --bucket file` | nebu fetch output |
| `FS` | `--storage-type FS --bucket dir` | Individual XDR files |

### Test Results

**1,001 mainnet ledgers from GCS (obsrvr pubnet bucket):**
- 67.3 ledgers/sec with 8 workers
- 5.9M bronze rows across 16 tables
- 146K silver rows across 6 tables
- 15 seconds total (bronze + silver)
- 345 MB Parquet output

**Projected for 63M ledger full history:**
- On GCE VM near bucket with 64 workers: ~1-2 days
- On local NVMe with FS backend: ~35 minutes (projected)

### Usage

```bash
# Build
cd obsrvr-lake/stellar-history-loader
make build

# Run from GCS (obsrvr pubnet archive)
./bin/stellar-history-loader \
  --start 61790000 --end 61791000 \
  --workers 8 --batch-size 50 \
  --output /data/output \
  --storage-type GCS \
  --bucket "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet" \
  --ledgers-per-file 1 --files-per-partition 64000 \
  --network-passphrase "Public Global Stellar Network ; September 2015" \
  --silver

# Run from nebu fetch output
nebu fetch 61790000 61791000 --output ledgers.xdr
./bin/stellar-history-loader \
  --start 61790000 --end 61791000 \
  --workers 8 \
  --output /data/output \
  --storage-type XDR --bucket ledgers.xdr \
  --network-passphrase "Public Global Stellar Network ; September 2015" \
  --silver

# Query output with DuckDB
duckdb -c "SELECT count(*) FROM read_parquet('/data/output/bronze/**/*.parquet')"
```

### What's Left

**Cycle 2 remaining:**
- Hot buffer tail population (PostgreSQL COPY for recent ledgers)
- Checkpoint/resume per shard

**Future:**
- Incremental catch-up mode
- DuckLake PostgreSQL catalog registration
- Docker image for deployment
