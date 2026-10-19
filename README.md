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

The code is now layered, per §2 and §6 of the
[CLI Engineering Standard](docs/cli-standard-adoption.md):

```
cmd/stellar-ingest/     composition root and surface dispatch
internal/cli/           flags, resolution, Cobra commands
internal/app/           the operations: a run, callable without a terminal
internal/domain/        value objects, configuration, validation
internal/adapters/      sources and sinks: Parquet, DuckLake, hot buffer
internal/jobs/          orchestration: worker, checkpoint, chunks, lineage
internal/fixtures/      the committed fixtures, shared by every layer's tests
internal/credential/    where a secret came from
internal/status/        OSC 7501 program status
```

The dependency direction is enforced by the compiler, not by intention:
`domain` depends on nothing in this repository, `adapters` on `domain`, and
`jobs` on both. An API, a UI or a test can run an ingestion without a process
boundary or a flag parser. No public 21-table Bronze API is claimed —
`internal/` is private by construction. See
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
make build-all      # bin/stellar-ingest and bin/stellar-history-loader
make build          # stellar-ingest only
make test
make version-stamp  # assert the build stamp reached the binary
make ci-build       # deps, vet, build, test, CLI parity gate, build stamp
```

`stellar-history-loader` is a transitional alias for the same program so
existing scripts keep working. See [`docs/compatibility.md`](docs/compatibility.md).

## Usage

```sh
stellar-ingest doctor                                         # is this machine configured?
stellar-ingest extract   --start 64000000 --end 64001000 --output-dir ./bronze
stellar-ingest validate  --output-dir ./bronze --output json
stellar-ingest load      --output-dir ./bronze --ducklake --ducklake-data-path s3://bucket/
stellar-ingest repair    --start 61790000 --end 61891000 --enrichment-dry-run
stellar-ingest run       --start 64000000 --end 64001000 --output-dir ./bronze --ducklake
stellar-ingest inspect   --start 64000000 --end 64010000 --chunk-size 1000
stellar-ingest wait      --start 64000000 --end 64010000 --chunk-size 1000 --for succeeded
stellar-ingest version
stellar-ingest completion bash                                # also zsh, fish
```

The flag-only form — no verb — keeps working and is equivalent to `run`. It
warns once per invocation. Deployed jobs pass about twenty flags with no verb,
so breaking them to gain a noun was not worth it.

The two surfaces are parsed by different libraries, which matters in one
visible way: the verb form requires `--flag`, while the flag-only form also
accepts `-flag`. pflag, which Cobra uses, reads a single dash followed by
several characters as a cluster of shorthands.

`--help` is the full flag list, and `docs/baseline/cli-help.txt` is the
recorded contract, enforced by `make parity-baseline` — a flag change that
does not update it fails CI.

### Global options

| Flag | Meaning |
|---|---|
| `--output table\|json\|ndjson` | Response format. Default `table` |
| `--output-dir` | Where Parquet goes. See the note below on `--output` |
| `--config`, `--context` | A named set of defaults; see below |
| `--network mainnet\|testnet\|futurenet` | Sets the passphrase |
| `--timeout 30m` | Bounds the run. `0`, the default, is no limit |
| `--quiet`, `--verbose` | Less or more narration on stderr |
| `--no-color` | Also honoured: `NO_COLOR`, and a non-terminal stderr |
| `--events <path>\|-` | Append an NDJSON event stream |
| `--doctor` | Run the diagnostic instead of ingesting (flag-only form) |

### Streams

stdout carries the **result**: the rendered `RunResult`, `--plan` output, or
`--version`. Everything else — the banner, progress, phase headers, warnings
— goes to stderr. So this works:

```sh
stellar-ingest run --config ingest.yaml --start 64000000 --end 64001000 \
  --output json --quiet | jq .ducklake.total_rows
```

The result schema is versioned as `stellar-ingest.run/v1` and carried in the
payload, so a consumer can tell it is reading something newer rather than
silently misreading a renamed field.

### Exit codes

`0` success, and documented classes for everything else, so a scheduler can
tell "retry this" from "stop and ask someone":

| Code | Class | Means |
|---|---|---|
| 0 | ok | |
| 1 | internal | A bug, or a dependency behaving unexpectedly |
| 2 | usage | Invalid input. Retrying the same invocation cannot help |
| 3 | not-found | A resource the run needs is not there |
| 4 | permission | A credential or authorisation failure |
| 5 | timeout | `--timeout` elapsed |
| 6 | cancelled | SIGINT or SIGTERM |

### A config file

A mainnet dispatch passes about twenty flags, the same twenty every time. A
context is those flags under a name:

```yaml
# ingest.yaml
default-context: mainnet
contexts:
  mainnet:
    network: mainnet
    storage-type: GCS
    bucket: obsrvr-stellar-ledger-data-mainnet-data/landing/ledgers
    ducklake-data-path: s3://obsrvr-lake-mainnet/
    ducklake-metadata-schema: bronze_meta
    workers: "12"
    chunk-size: "10000"
```

```sh
stellar-ingest run --config ingest.yaml --start 64000000 --end 64010000
stellar-ingest run --config ingest.yaml --context testnet --start 1 --end 1000
```

A context sets **defaults**. An explicit flag always wins, and the tool says
so when one shadows a context value rather than leaving you to debug a file
that is being correctly ignored. Credentials are not accepted in this file —
they stay in the environment.

### Checking the machine first

```sh
stellar-ingest doctor --ducklake
```

Credentials and where each resolved, the output directory's writability,
DuckDB opening with the `ducklake` and `httpfs` extensions and a UTC-pinned
session, catalog reachability and whether the metadata schema has tables. It
never prints a credential value — presence and length only, and any DSN it
reports is redacted. Exit 2 if any check fails, so CI can gate on it.

### Watching a backfill

Every run carries a job ID, which is the same value the range catalog stores
as `attempt_id`, so an event stream and the durable record join on one
identifier.

```sh
stellar-ingest run ... --events ./events.ndjson     # append-only, one line per event
stellar-ingest inspect --start 64000000 --end 64010000 --chunk-size 1000 --output json
stellar-ingest wait    --start 64000000 --end 64010000 --chunk-size 1000 \
  --for succeeded --timeout 6h
```

`inspect` reports each range as `queued`, `running`, `succeeded` or
`failed`, derived from the range catalog. `wait --for settled` is a
different question from `--for succeeded`: a span containing a failed range
is settled but has not succeeded.

Event delivery is **at-most-once**. There is no broker: a killed process
loses what it had not written. The durable record is the range catalog —
read it with `inspect` when you need a guarantee, and use events for
liveness.

### Extract a mainnet range from a GCS archive

```sh
./bin/stellar-ingest \
  --start 61790000 --end 61791000 \
  --workers 8 --batch-size 50 \
  --output-dir /data/output \
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
  --output-dir /data/output \
  --storage-type XDR --bucket ledgers.xdr \
  --network-passphrase "Public Global Stellar Network ; September 2015"
```

### Load an existing output directory into DuckLake

Credentials come from the environment — see [Credentials](#credentials). None
of these examples passes one as a flag.

Extraction is skipped when `--bucket` is empty; a `manifest.json` in
`--output-dir` is used instead.

```sh
./bin/stellar-ingest \
  --start 61790000 --end 61791000 \
  --output-dir /data/output \
  --ducklake \
  --ducklake-data-path s3://obsrvr-lake-pubnet/
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

## `--output` changed meaning

`--output` now selects the **response format**, which is what the Obsrvr CLI
Engineering Standard reserves it for. The output directory is `--output-dir`.

```sh
--output-dir ./bronze     # where Parquet goes
--output json             # how results are rendered: table|json|ndjson
```

A path passed to `--output` is still accepted for one release, resolved as the
old directory meaning, and warned about on stderr — so existing scripts and
jobs keep working while they move. Passing two different directories, one to
each flag, is refused rather than guessed at.

The transition is resolved by value: `table`, `json` and `ndjson` are formats,
and anything else is a directory. The only input that gets wrong is an output
directory named exactly one of those three, which fails immediately with
`--output-dir` unset rather than writing somewhere surprising.

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
  --ducklake-data-path s3://obsrvr-lake-pubnet/
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
