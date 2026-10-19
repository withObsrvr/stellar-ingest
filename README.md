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

Since then the cold storage layer changed shape, under
[`docs/bronze-storage-contract.md`](docs/bronze-storage-contract.md):
identity is stored compactly, the shapes consumers query are presentation
views over it, and tables are partitioned by `ledger_range`. A backfill is
resumable across restarts through a range catalog, and builds partitions
concurrently. [`docs/bronze-presentation-views.md`](docs/bronze-presentation-views.md)
has the measurements and what is deliberately deferred.

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

The binary takes flags, not a config file. Forty-five flags; `--help` is the
full list and `docs/baseline/cli-help.txt` is the recorded contract, enforced
by `make parity-baseline` — a flag change that does not update it fails CI.

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
make test                        # unit suite
make golden                      # Bronze snapshot suite over 8 committed pubnet fixtures
make integration                 # DuckLake end to end, PostgreSQL catalog in a container
make integration-ducklake-file   # the same, against a DuckDB file catalog
make ci-build                    # everything CI runs
```

The golden suite needs no network and no credentials — fixtures are committed
raw pubnet XDR. It covers all 21 Bronze tables, including 22 automatic
restorations in ledger 61,500,126. `make golden-update` regenerates the
snapshots; review the diff, because they are the reviewed record of what the
extractors produce.

The integration suite is the only one that exercises the path a re-ingest
actually runs: Parquet, through `read_parquet`, into a `BLOB` column, through a
view that renders it back and joins a parent. `make integration` brings up a
throwaway PostgreSQL catalog in podman or docker and tears it down after;
`make integration-ducklake-file` needs no container and no credentials, so it
is the one to reach for while iterating. Both skip cleanly when their
environment is unset, so `go test ./...` is unaffected.

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
`v3_bronze_schema.sql` and the views are `v3_bronze_views.sql`.

In DuckLake the two live in separate schemas:

```
bronze_store.transactions_row_v2   physical table, transaction_hash as 32 bytes
bronze.transactions_row_v2         view, renders lower(hex(..)) as 64 chars
```

Writers target the store schema (`--ducklake-schema`, default `bronze_store`);
readers use the view schema (`--ducklake-view-schema`, default `bronze`), which
is the name consumers already query. That is what lets cold stop storing an
identity three times without any reader changing. Tables are partitioned by
`ledger_range`, so files land in `ledger_range=64000000/` directories.

A column removal or type change only takes effect on files written afterwards,
so these changes are observable on a catalog created after them — which is why
a re-ingest is a re-ingest rather than a catch-up.

## Backfilling a long range

A multi-day backfill is interruptible. `--range-state` records each
10,000-ledger range in a PostgreSQL catalog as it is claimed and completed, so
a rerun skips finished work instead of starting over, and a range whose worker
died is reclaimed after its lease expires.

```sh
./bin/stellar-ingest --plan --start 1 --end 64000000 \
  --range-state --range-state-table public.ingest_range_state
```

`--plan` prints the ranges still to build, one `start end` pair per line, with
everything else commented — so it pipes into a dispatch loop. `--chunk-size`
and `--chunk-concurrency` then control how much of that one process takes on
at a time.

Memory scales with concurrency, not range size: peak is roughly
`chunks x workers x 200 x ledger size`, because each worker prefetches 200
decoded ledgers. A 64-million-ledger range costs the same RAM as a
2,000-ledger one at equal concurrency.

## Program status

Long runs report their state as [OSC
7501](https://mitchellh.com/writing/program-status-osc7501) program status on
stderr, so a terminal, dashboard or supervising tool can follow a run from the
program's own account of it rather than by scraping output:

```
ESC]7501;state=working:app=stellar-ingest:title=5 ledgers, 2284/s:progress=100ESC\
ESC]7501;state=done:app=stellar-ingest:msg=<base64>ESC\
```

`--program-status` is `auto` by default, which reports only when stderr is a
terminal, because the sequence is noise in a log file or a pipe. `always` is
for a consumer reading a captured stream; `never` keeps the stream clean.
Reports never go to stdout — that carries results, and an escape sequence in a
result stream corrupts it.

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

Pass credentials through the environment, or through a file:

| Credential | Variable | File form |
|---|---|---|
| DuckLake catalog DSN | `DUCKLAKE_CATALOG_DSN` | `DUCKLAKE_CATALOG_DSN_FILE` |
| B2/S3 access key ID | `B2_KEY_ID` | `B2_KEY_ID_FILE` |
| B2/S3 secret key | `B2_KEY_SECRET` | `B2_KEY_SECRET_FILE` |
| PostgreSQL password | `PG_PASSWORD` | `PG_PASSWORD_FILE` |

```sh
export DUCKLAKE_CATALOG_DSN="postgresql://obsrvr:...@127.0.0.1:5432/catalog"
export B2_KEY_SECRET_FILE=/run/secrets/b2            # Nomad, Kubernetes, Docker
./bin/stellar-ingest --ducklake ...                  # no credential on the command line
```

The `_FILE` form wins over the plain variable, and a file that is named but
unreadable is an error rather than a fallthrough — a misconfigured secret
mount should fail where it is wrong, not as an authentication error somewhere
further away. A trailing newline is trimmed, because that is how every editor
and `echo` writes a file and a credential with one appended fails for a reason
nobody can see.

The matching flags — `--ducklake-catalog`, `--b2-key-id`, `--b2-key-secret`,
`--pg-password` — still work and now warn. An argument is visible in `ps` to
every user on the box, recorded in shell history, and **inherited by child
processes**: this loader re-invokes itself per chunk, so a secret on the
parent's command line is on every child's too. Passing it through the
environment fixes the children for free, because a child inherits the
environment rather than the argument list.

The warning names the flag and the variables, never the value.

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
| [`docs/bronze-storage-contract.md`](docs/bronze-storage-contract.md) | What a Bronze row costs, measured, and the rules decided from it |
| [`docs/bronze-presentation-views.md`](docs/bronze-presentation-views.md) | The compact-identity change: what moved, what it recovered, what is deferred |
| [`docs/cold-authority-matrix.md`](docs/cold-authority-matrix.md) | Which producer wrote which part of cold, and which generations are trustworthy |
| [`docs/engineering-standard.md`](docs/engineering-standard.md) | How changes here are expected to be built and tested |
| [`docs/cli-standard-adoption.md`](docs/cli-standard-adoption.md) | Gap analysis against the Obsrvr CLI Engineering Standard, and the staged plan |
| `docs/baseline/cli-help.txt` | Recorded CLI contract, enforced by `make parity-baseline` |

## Related repositories

- [`withObsrvr/stellar-raw-ledger-origin`](https://github.com/withObsrvr/stellar-raw-ledger-origin) — raw ledger acquisition
- [`withObsrvr/ttp-processor-demo`](https://github.com/withObsrvr/ttp-processor-demo) — Obsrvr Lake services and the pre-migration history of this code
