# stellar-ingest

A Go toolkit for transforming historical Stellar ledger data into analytical
datasets, with support for multiple data sources and storage destinations.

`stellar-ingest` acquires Stellar ledgers from archives, RPC, Captive Core, or
local files; decodes them once; extracts 21 Bronze datasets in parallel; and
writes them to Parquet, DuckLake, and optionally a PostgreSQL hot buffer. It is
a standalone batch CLI — a full historical backfill is one command, with no
gRPC service and no control plane required.

```
Sources: RPC | GCS/S3 archive | Captive Core | XDR | FS
            |
            v
   decode -> 21 Bronze extracts -> validate -> batch
            |             |              |
         Parquet ----> DuckLake      PostgreSQL hot
```

## Status

This repository was bootstrapped on 2026-10-14 by promoting the existing
`stellar-history-loader` application from `ttp-processor-demo` in its entirety.
It is **behaviour-identical** to that application — same flags, same defaults,
same output, same exit codes. See [`PROVENANCE.md`](PROVENANCE.md) for exactly
what changed and what parity was verified.

Incremental refactoring now includes a private worker/Parquet boundary and
the `internal/bronze/restored` mapper. Most code remains in `package main`;
no public 21-table Bronze API is claimed. See
[`docs/module-boundaries.md`](docs/module-boundaries.md),
[`docs/target-architecture.md`](docs/target-architecture.md), and
[`docs/ticket-03-progress.md`](docs/ticket-03-progress.md).

## Build with Nix

```sh
nix build .#stellar-ingest     # binary + stellar-history-loader alias
nix run . -- --start ... --end ...
nix develop                    # dev shell: Go, CGO toolchain, duckdb, zstd, gcloud
nix flake check                # builds and runs the hermetic test suite
nix build .#docker             # container image tarball
```

Flakes only see git-tracked files, so `git add` a new file before building it.

## Requirements

- Go 1.26.1+
- A C toolchain (`gcc`, `g++`) — `duckdb-go/v2` requires cgo
- `CGO_ENABLED=1` and `GOWORK=off`, both set by the `Makefile`

## Build

```sh
make build-all     # bin/stellar-ingest and bin/stellar-history-loader
make build         # stellar-ingest only
make test
make ci-build      # deps, vet, build, test, CLI parity gate
```

`stellar-history-loader` is a transitional alias for the same program so
existing scripts keep working. See [`docs/compatibility.md`](docs/compatibility.md).

## Usage

The binary takes flags, not a config file. Forty-one flags; `--help` is the
full list and `docs/baseline/cli-help.txt` is the recorded contract.

### Extract a mainnet range from a GCS archive

```sh
./bin/stellar-ingest \
  --start 61790000 --end 61791000 \
  --workers 8 --batch-size 50 \
  --output /data/output \
  --storage-type GCS \
  --bucket "obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet" \
  --ledgers-per-file 1 --files-per-partition 64000 \
  --network-passphrase "Public Global Stellar Network ; September 2015"
```

### Extract from `nebu fetch` output

```sh
nebu fetch 61790000 61791000 --output ledgers.xdr

./bin/stellar-ingest \
  --start 61790000 --end 61791000 \
  --workers 8 \
  --output /data/output \
  --storage-type XDR --bucket ledgers.xdr \
  --network-passphrase "Public Global Stellar Network ; September 2015"
```

### Load an existing output directory into DuckLake

Extraction is skipped when `--bucket` is empty; a `manifest.json` in
`--output` is used instead.

```sh
./bin/stellar-ingest \
  --start 61790000 --end 61791000 \
  --output /data/output \
  --ducklake \
  --ducklake-catalog "$DUCKLAKE_CATALOG_DSN" \
  --ducklake-data-path s3://obsrvr-lake-pubnet/ \
  --b2-key-id "$B2_KEY_ID" --b2-key-secret "$B2_KEY_SECRET"
```

### Query the output

```sh
duckdb -c "SELECT count(*) FROM read_parquet('/data/output/bronze/**/*.parquet')"
```

## Tests

```sh
make test     # unit suite
make golden   # hermetic Bronze snapshot suite over 8 committed pubnet fixtures
```

The golden suite needs no network and no credentials — fixtures are committed
raw pubnet XDR. It covers all 21 Bronze tables, including 22 automatic
restorations in ledger 61,500,126.

## Two things that will bite you

- **`--network-passphrase` defaults to testnet.** A mainnet run that omits it
  is silently mis-labelled. Always pass it explicitly.
- **`--workers` defaults to `runtime.NumCPU()`.** Runs are not comparable
  across machines unless you pin it.

## Sources

| `--storage-type` | Location from | Notes |
|---|---|---|
| `FS` | `--bucket` as a directory | Individual XDR files |
| `XDR` | `--bucket` as a file | `nebu fetch` output |
| `RPC` | `--bucket` as the URL | Auth via `RPC_AUTH_HEADER` |
| `GCS` | `--bucket` as `bucket/path` | |
| `S3` | `--bucket` as `bucket/path` | Endpoint via `S3_ENDPOINT_URL` |
| `CAPTIVE_CORE` | environment only | No `--bucket` required |

`RPC`, `GCS`, `S3`, and `CAPTIVE_CORE` delegate in-process to
`stellar-raw-ledger-origin/source/ledgerstream`. `CAPTIVE_CORE` is configured
entirely through environment variables — see
[`docs/phase0-inventory.md`](docs/phase0-inventory.md) §3.

## Destinations

| Destination | Enabled by |
|---|---|
| Parquet | Always, to `--output` |
| DuckLake | `--ducklake` |
| PostgreSQL hot buffer | `--pg-host` non-empty |

The 21 Bronze tables and their DuckLake names are listed in
[`docs/phase0-inventory.md`](docs/phase0-inventory.md) §6. The DDL is
`v3_bronze_schema.sql`.

## Repair

`--enrich-contract-balances` is a mode switch that backfills missing Bronze
contract `balance_holder` and `balance` fields in place and returns without
extracting. It requires DuckLake credentials and, unless
`--enrichment-dry-run` is set, a durable `--enrichment-run-id`.

```sh
./bin/stellar-ingest \
  --enrich-contract-balances --enrichment-dry-run \
  --start 61790000 --end 61891000 \
  --ducklake-catalog "$DUCKLAKE_CATALOG_DSN" \
  --ducklake-data-path s3://obsrvr-lake-pubnet/ \
  --b2-key-id "$B2_KEY_ID" --b2-key-secret "$B2_KEY_SECRET"
```

## flowctl

Control-plane reporting is opt-in and off by default. With `ENABLE_FLOWCTL`
unset, the program never contacts a control plane and never fails on its
absence. Setting `ENABLE_FLOWCTL=true` requires `FLOWCTL_ENDPOINT` and is
fail-closed: registration failure aborts the run.

## Credentials

`--b2-key-secret`, `--pg-password`, and the DSN in `--ducklake-catalog` are
currently accepted as command-line arguments, which exposes them in process
listings and shell history. Environment fallbacks are a known gap, recorded in
[`docs/phase0-inventory.md`](docs/phase0-inventory.md) §2.

## Documentation

| Document | Contents |
|---|---|
| [`PROVENANCE.md`](PROVENANCE.md) | Where this code came from, what changed, what parity was verified |
| [`docs/phase0-inventory.md`](docs/phase0-inventory.md) | Flags, environment variables, source/destination matrix, the 21 tables, flowctl, build requirements |
| [`docs/compatibility.md`](docs/compatibility.md) | Executable names, what is deliberately unchanged, rollback |
| [`docs/phase0-fixtures.md`](docs/phase0-fixtures.md) | Golden fixture plan (TICKET 02) |
| [`docs/target-architecture.md`](docs/target-architecture.md) | Ownership boundaries and the target package layout |
| [`docs/roadmap.md`](docs/roadmap.md) | Phases, tickets, and current state |
| [`docs/ticket-04-developer-handoff.md`](docs/ticket-04-developer-handoff.md) | Historical/live Bronze parity work plan and discrepancy-report acceptance gate |
| `docs/baseline/cli-help.txt` | Recorded CLI contract, enforced by `make parity-baseline` |

## Related repositories

- [`withObsrvr/stellar-raw-ledger-origin`](https://github.com/withObsrvr/stellar-raw-ledger-origin) — raw ledger acquisition
- [`withObsrvr/ttp-processor-demo`](https://github.com/withObsrvr/ttp-processor-demo) — Obsrvr Lake services and the pre-migration history of this code
