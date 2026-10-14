# Phase 0 — Compatibility inventory

Baseline captured from `ttp-processor-demo@bd618bb4`,
`obsrvr-lake/stellar-history-loader`. This document records what the program
accepts and does *today*, so that later phases can prove they changed nothing
they did not intend to change.

Everything here was read from the source or observed from a built binary. Where
something was not verified by running it, that is stated.

---

## 1. Executables

| Name | Status |
|---|---|
| `stellar-ingest` | Primary executable. |
| `stellar-history-loader` | Transitional alias — the same program under the legacy name, so existing operational scripts keep working. See `docs/compatibility.md`. |

Both are built from the same flat `package main` and have an identical flag
surface. There is no subcommand structure yet; the proposed `run` / `extract` /
`validate` / `load` / `repair` / `inspect` hierarchy is TICKET 06.

---

## 2. CLI flags

Forty-one flags, all top-level, parsed with the standard library `flag`
package. The verbatim `--help` output is committed at
`docs/baseline/cli-help.txt` and enforced by `make parity-baseline`.

### Range and execution

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--start` | uint | `0` | Required, must be `> 0`. |
| `--end` | uint | `0` | Required, must be `> 0` and **strictly greater** than `--start`. |
| `--workers` | int | `runtime.NumCPU()` | Default is machine-dependent — `16` on the baseline host. Not reproducible across machines; pin it in any comparison run. |
| `--batch-size` | int | `1000` | Ledgers per extraction batch. |
| `--chunk-size` | uint | `0` | Max ledgers per sequential extract/validate/publish chunk. `0` processes the range as one chunk. |
| `--output` | string | — | Required. Created with `MkdirAll(0o755)` if absent. |

**Range semantics.** `--start`/`--end` are *inclusive* — `totalLedgers` is
computed as `end - start + 1`. But validation rejects `end <= start`, so a
single-ledger range cannot be expressed. This is an inconsistency worth
resolving deliberately rather than by accident; the handoff's invariant
"ledger range semantics must be documented (inclusive/exclusive) and consistent"
applies here. Do not change it without a decision — scripts may rely on it.

### Source

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--storage-type` | string | `FS` | One of `GCS`, `S3`, `RPC`, `CAPTIVE_CORE`, `XDR`, `FS`. |
| `--bucket` | string | — | Required *unless* `--storage-type CAPTIVE_CORE`, or `--ducklake` over an already-extracted output directory. |
| `--ledgers-per-file` | uint | `1` | Archive layout, `GCS`/`S3` only. |
| `--files-per-partition` | uint | `64000` | Archive layout, `GCS`/`S3` only. |
| `--network-passphrase` | string | `Test SDF Network ; September 2015` | **Defaults to testnet.** A mainnet run that omits this flag is silently mis-labelled. |

### Output selection

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--only-tables` | CSV | empty | Extractor / source / DuckLake table names. Empty extracts all 21. |
| `--validate` | bool | `false` | Quality checks after extraction. |
| `--era-id` | string | empty | Era identifier for DuckLake partitioning. |

### DuckLake sink

| Flag | Type | Default |
|---|---|---|
| `--ducklake` | bool | `false` |
| `--ducklake-catalog` | string | — |
| `--ducklake-data-path` | string | — |
| `--ducklake-metadata-schema` | string | `bronze_meta` |
| `--ducklake-schema-sql` | string | empty |
| `--ducklake-only-tables` | CSV | empty — falls back to `--only-tables` when that is set |
| `--ducklake-skip-tables` | CSV | empty |
| `--b2-key-id` | string | — |
| `--b2-key-secret` | string | — |
| `--b2-endpoint` | string | `s3.us-west-004.backblazeb2.com` |
| `--b2-region` | string | `us-west-004` |

### PostgreSQL hot buffer

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--pg-host` | string | empty | **Non-empty enables hot-buffer loading.** There is no separate on/off flag. |
| `--pg-port` | int | `25060` | DigitalOcean managed-PG default. |
| `--pg-database` | string | `stellar_hot` | |
| `--pg-user` | string | — | |
| `--pg-password` | string | — | |
| `--pg-sslmode` | string | `require` | |
| `--tail-ledgers` | uint | `100000` | Recent ledgers loaded into the hot buffer. |

### Contract-balance enrichment / repair

`--enrich-contract-balances` is a **mode switch**: when set, the program runs
enrichment and returns before any extraction work. Its validation block runs
before the normal required-flag checks, so enrichment has its own distinct
error messages for the same flags.

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--enrich-contract-balances` | bool | `false` | Mode switch. |
| `--enrichment-chunk-size` | uint | `100000` | Must be `> 0`. |
| `--enrichment-run-id` | string | empty | Required unless `--enrichment-dry-run`. |
| `--enrichment-dry-run` | bool | `false` | Counts candidates without mutating DuckLake. |
| `--enrichment-resume` | bool | `true` | Skips chunks already completed under the run ID. |
| `--enrichment-manifest-schema` | string | `bronze_operations` | |

Enrichment additionally requires `--ducklake-catalog`, `--ducklake-data-path`,
`--b2-key-id`, and `--b2-key-secret`, and a valid inclusive `--start`/`--end`.

### Credential handling — known gap

`--b2-key-secret`, `--pg-password`, and the DSN in `--ducklake-catalog` are
accepted as **command-line arguments**, which exposes them in process listings
and shell history. The handoff's invariant is that credentials "should use
environment/secret references rather than printed CLI arguments". This is a
real deviation, carried over unchanged by the bootstrap. Adding environment
fallbacks is additive and does not break existing invocations; it is not yet
scheduled to a ticket.

---

## 3. Environment variables

### Consumed by this program directly

| Variable | Read by | Effect |
|---|---|---|
| `ENABLE_FLOWCTL` | `flowcomponent.ConfigFromEnv` | `true` (case-insensitive) enables control-plane reporting. Anything else disables it. |
| `FLOWCTL_ENDPOINT` | same | Required when `ENABLE_FLOWCTL=true`. |
| `FLOWCTL_COMPONENT_ID` | same | Component identity. |
| `FLOWCTL_RUN_ID` | same | Run identity. |
| `FLOWCTL_ATTEMPT` | same | Positive integer, default `1`. |
| `FLOWCTL_HEARTBEAT_INTERVAL_MS` | same | Positive integer milliseconds. |

### Consumed indirectly via `stellar-raw-ledger-origin/source/ledgerstream`

`NewSDKStreamSource` calls `ledgerstream.ConfigFromEnv()` and then overrides
only `NetworkPassphrase`, `Type`, and the per-backend location fields. Every
other field still comes from the environment:

| Variable | Applies to | Overridden by a flag? |
|---|---|---|
| `RPC_AUTH_HEADER` | `RPC` | No — env only. The only way to authenticate an RPC source. |
| `S3_ENDPOINT_URL` | `S3` | No — env only. |
| `STELLAR_CORE_BINARY_PATH` | `CAPTIVE_CORE` | No — env only. |
| `STELLAR_CORE_CONFIG_PATH` | `CAPTIVE_CORE` | No — env only. |
| `HISTORY_ARCHIVE_URLS` | `CAPTIVE_CORE` | No — env only. |
| `CAPTIVE_CORE_STORAGE_PATH` | `CAPTIVE_CORE` | No — env only. |
| `BACKEND_TYPE`, `STORAGE_TYPE`, `BUCKET_NAME`, `BUCKET_PATH` | archive/RPC | **Yes** — flags win for `RPC`/`GCS`/`S3`. Setting them has no effect on those backends. |

This split matters: `CAPTIVE_CORE` is configured **entirely** through the
environment and has no flag coverage at all. Any config-file or
`--config`-based interface added later must reach these.

### Test-only

`DUCKLAKE_INGESTION_TEST_*` (7 vars) and `DUCKLAKE_VERIFY_*` (9 vars) gate the
DuckLake integration tests. Unset, those tests skip — which is why the
committed suite passes in under a tenth of a second without any infrastructure.

---

## 4. Source backend matrix

| `--storage-type` | Implementation | Location from | Requires `--bucket` |
|---|---|---|---|
| `FS` | `NewFilesystemSource` — local, in-repo | `--bucket` as a directory | Yes |
| `XDR` | `NewXDRStreamSource` — local, in-repo | `--bucket` as a `nebu fetch` output file | Yes |
| `RPC` | `ledgerstream` (**external**) | `--bucket` as the RPC URL | Yes |
| `GCS` | `ledgerstream` (**external**) | `--bucket` split into bucket + path | Yes |
| `S3` | `ledgerstream` (**external**) | `--bucket` split into bucket + path | Yes |
| `CAPTIVE_CORE` | `ledgerstream` (**external**) | environment only | No |

**Finding that changes Phase 3's scope.** Four of the six backends already
delegate to `stellar-raw-ledger-origin/source/ledgerstream` (dependency pinned
at `v0.2.5`), in-process as a Go library rather than over gRPC. The handoff
frames Phase 3 as "use `stellar-raw-ledger-origin` as the candidate canonical
raw source"; for `RPC`, `GCS`, `S3`, and `CAPTIVE_CORE` that integration is
already done, and done in the shape the architecture notes prefer — a library
adapter with no network hop. The remaining Phase 3 work is therefore narrower
than written: establishing the contract tests, deciding whether `FS` and `XDR`
should also move behind `ledgerstream`, and the separate question of retiring
`stellar-live-source-datalake`.

`SDKStreamSource.StreamRange` already enforces several of the handoff's
correctness invariants: it rejects out-of-order sequences, rejects ledgers past
the bounded range end, and reports incomplete ranges. That is a useful starting
point for the source contract tests in TICKET 05.

---

## 5. Output destinations

| Destination | Enabled by | Notes |
|---|---|---|
| Parquet | always | Written to `--output`. The other two sinks read from it. |
| DuckLake | `--ducklake` | `INSERT INTO` path. The handoff records this as the dominant cost — 75.1 s of a 98.9 s run — and flags it for independent optimisation before any storage-architecture change. |
| PostgreSQL hot buffer | `--pg-host` non-empty | Loads the most recent `--tail-ledgers`. |

`--ducklake` can run over an already-extracted output directory without a
source: when `--bucket` is empty and `--storage-type` is not `CAPTIVE_CORE`,
extraction is skipped and a `manifest.json` in `--output` is used instead.
This load-only mode is the existing behaviour behind the proposed
`stellar-ingest load` subcommand.

---

## 6. The 21 Bronze tables

Source name → DuckLake table, from `table_selection.go`, cross-checked against
`CREATE TABLE` statements in `v3_bronze_schema.sql`. Both lists agree at 21.

| # | Extractor | Output directory | DuckLake table |
|---|---|---|---|
| 1 | `ledgers` | `ledgers` | `ledgers_row_v2` |
| 2 | `transactions` | `transactions` | `transactions_row_v2` |
| 3 | `operations` | `operations` | `operations_row_v2` |
| 4 | `effects` | `effects` | `effects_row_v1` |
| 5 | `trades` | `trades` | `trades_row_v1` |
| 6 | `accounts` | `accounts_snapshot` | `accounts_snapshot_v1` |
| 7 | `trustlines` | `trustlines_snapshot` | `trustlines_snapshot_v1` |
| 8 | `account_signers` | `account_signers_snapshot` | `account_signers_snapshot_v1` |
| 9 | `native_balances` | `native_balances` | `native_balances_snapshot_v1` |
| 10 | `offers` | `offers_snapshot` | `offers_snapshot_v1` |
| 11 | `liquidity_pools` | `liquidity_pools_snapshot` | `liquidity_pools_snapshot_v1` |
| 12 | `claimable_balances` | `claimable_balances_snapshot` | `claimable_balances_snapshot_v1` |
| 13 | `contract_events` | `contract_events` | `contract_events_stream_v1` |
| 14 | `contract_data` | `contract_data_snapshot` | `contract_data_snapshot_v1` |
| 15 | `contract_code` | `contract_code_snapshot` | `contract_code_snapshot_v1` |
| 16 | `config_settings` | `config_settings` | `config_settings_snapshot_v1` |
| 17 | `ttl` | `ttl_snapshot` | `ttl_snapshot_v1` |
| 18 | `evicted_keys` | `evicted_keys` | `evicted_keys_state_v1` |
| 19 | `restored_keys` | `restored_keys` | `restored_keys_state_v1` |
| 20 | `contract_creations` | `contract_creations` | `contract_creations_v1` |
| 21 | `token_transfers` | `token_transfers` | `token_transfers_stream_v1` |

Note the three-way naming: extractor name, Parquet output directory, and
DuckLake table are all different for most tables, and `--only-tables` accepts
any of the three spellings. Any future rename has to preserve all three.

---

## 7. flowctl dependency

**Registration is already opt-in.** This corrects an assumption in the handoff.

Phase 1 item 3 asks to "make control-plane registration optional for standalone
runs, but preserve the existing fail-closed requirement when managed execution
is explicitly enabled." Reading `flowcomponent.NewReporter`, that is already
the behaviour:

- `ConfigFromEnv()` sets `Enabled` only when `ENABLE_FLOWCTL=true`.
- `NewReporter` returns a no-op reporter immediately when `Enabled` is false.
- When `Enabled` is true, a missing `FLOWCTL_ENDPOINT` is an error, and both
  `NewReporter` and `Register` failures are `log.Fatalf` — fail-closed.

So a standalone run with `ENABLE_FLOWCTL` unset never contacts a control plane
and never fails on its absence. **No code change is required for Phase 1
item 3.** What remains is a test that pins this behaviour so it cannot regress,
and resolving open decision (f) — which *deployment* paths set
`ENABLE_FLOWCTL=true` today. That second half cannot be answered from this
repository; it needs the deployment manifests.

When enabled, the program registers as `SERVICE_TYPE_SOURCE` with labels
`component`, `version`, `network`, `storage_type`, `range_start`, `range_end`,
runs a heartbeat loop, and reports chunk failures classified by
`classifyFlowctlFailure` into `NON_RETRYABLE_SCHEMA`, `NON_RETRYABLE_DATA`,
`RETRYABLE_INFRASTRUCTURE`, or `UNKNOWN`.

---

## 8. Build and runtime requirements

| Requirement | Value | Why |
|---|---|---|
| Go | `1.26.1` (from `go.mod`) | |
| `CGO_ENABLED` | `1` | `duckdb-go/v2` is cgo. |
| `GOWORK` | `off` | `duckdb-go/v2` bundles a newer DuckDB than the other Obsrvr services, which is why this module was kept outside `ttp-processor-demo`'s `go.work`. Retained so builds behave identically regardless of any `go.work` above the directory. |
| C toolchain | `gcc`, `g++` | |
| Version string | `-ldflags "-X main.Version=..."`, defaults to `dev` | Reported in flowctl labels and enrichment manifests. |

Direct dependencies: `duckdb/duckdb-go/v2`, `jackc/pgx/v5`,
`parquet-go/parquet-go`, `stellar/go-stellar-sdk`,
`withObsrvr/stellar-raw-ledger-origin/source`, `withobsrvr/flowctl`.

---

## 9. Baseline measurements

### Observed on this host

| Check | Result |
|---|---|
| `go build` | exit 0 |
| `go test ./...` | pass, 0.07 s |
| `--help` | exit 0, 41 flags |
| Exit code with no flags / `--start 1` / `--start 1 --end 2` | `1` in all three cases |

The whole committed test suite runs in under a tenth of a second, which tells
you what it covers: unit-level logic, with every DuckLake integration test
skipped behind unset environment variables. There is **no** test today that
processes a real ledger range end-to-end. That gap is TICKET 02.

### Carried forward from the handoff, not reproduced here

For 1,000 mainnet ledgers: 23.162 s extraction, 75.092 s DuckLake ingestion,
98.92 s total; ~8.79 M rows, 1.47 GB local Parquet, ~1.21 GB active B2, ~14.5 GiB
peak RAM. Historical comparison figures, not an acceptance SLA. Not re-measured
during the bootstrap — no ledger data was processed.

---

## 10. Items this inventory could not resolve

These need information outside this repository:

1. **Which deployments set `ENABLE_FLOWCTL=true`** — open decision (f). Needs
   the deployment manifests.
2. **Live production dependencies and deployment manifests** for the loader,
   per Phase 0 item 1. Not present in the source repository.
3. **Published Docker image tags** currently in use. The source `Makefile`
   targeted `withobsrvr/stellar-history-loader`; this repository targets
   `withobsrvr/stellar-ingest`. Whether anything pulls the old image, and
   whether it should keep being published, is unresolved.
4. **`stellar-postgres-ingester` field mappings** — TICKET 04. Deliberately out
   of scope for Phase 0.
