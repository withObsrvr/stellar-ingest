# Processing module boundaries — current and target

Ticket 03 (behavior-preserving extraction boundary). This describes code that
**runs now**; `target-architecture.md` describes future packages. The legacy
CLI, checkpoint format, Parquet schema, and sink commit semantics have not
been migrated as part of this ticket.

| Responsibility | Current owner | Contract / next boundary |
|---|---|---|
| CLI and workflow modes | `main.go`, `chunk_runner.go` | Parse flags, compose in-process operations; do not move mapping rules into handlers. |
| Source acquisition | `worker.go` source adapters, `source_stream.go`, `stellar-raw-ledger-origin/ledgerstream` | `LedgerSource.StreamRange` yields bounded raw XDR; backends must not own Bronze semantics. |
| Decode and selection | `Worker.decodeLedger`, `extractBronzeLedger`, `table_selection.go` | Decode once, fan out selected extractors; mapping can run without a worker/CLI/sink. Selection recognizes extractor, output, and DuckLake names. |
| Bronze mapping | `stellar-extract@v0.1.2` (16), local functions (4), `internal/bronze/restored` (1) | See table below. No generic exported extractor interface until the live ingester's requirements and 21-table parity are established. |
| Validation | `validate.go` | Consumes output independently of acquisition. |
| Batching and Parquet | `BatchData`, `ParquetWriterFull` | Worker writes batches, closes Parquet footers, then marks its shard complete. The private `shardWriter` seam is only this Parquet boundary. |
| Attempt/checkpoint/manifest | `checkpoint.go`, `orchestrator.go`, `manifest.go` | Checkpoint is per-worker/per-shard, bound to configuration and isolated attempt ID. An incomplete shard's files are removed on restart; only finalized attempts publish a manifest. |
| Downstream sinks | `ducklake.go`, `hot_buffer.go` | Destination commits are not modeled by `shardWriter`. No claim of atomic DuckLake/PG checkpointing or exactly-once writes. |

## Which implementation produces each Bronze output

`Worker.extractLedger` delegates to a worker-independent `extractBronzeLedger`.
Its 21 selected mappings are:

| Implementation | Tables | Count |
|---|---|---:|
| Pinned `github.com/withObsrvr/stellar-extract@v0.1.2` | effects, trades, offers, trustlines, account_signers, claimable_balances, liquidity_pools, config_settings, ttl, evicted_keys, contract_events, contract_data, contract_code, native_balances, contract_creations, token_transfers | 16 |
| Local adapter/mapping still in flat package | transactions, operations, accounts, ledgers | 4 |
| `internal/bronze/restored` | restored_keys | 1 |

The worker adapts library rows to the existing v3 application row types. It
also sanitizes transaction memos and event UTF-8, reconciles config settings,
and stamps `era_id`. These are **part of the Bronze contract**; moving a
library extractor without these adapters is not behavior-preserving.
`restored.Extract` returns ledger-derived values; the application adds its
legacy `created_at` ingestion timestamp. It reads XDR `LEDGER_ENTRY_RESTORED`
changes, including automatic restores without a RestoreFootprint operation.
Its own fixture test and the full-output goldens protect the same production
path.

## Safety gates for later work

- Eight committed XDR fixtures cover all 21 historical table outputs with
  per-table row, schema, content, and column digests. They are not proof of
  parity with the **live PostgreSQL ingester**. That field-by-field comparison
  is Ticket 04; resolve row/null/version differences before making a public
  Bronze API or retiring any duplicate mapper.
- Upgrading the shared extraction dependency to v0.1.6 changes unrelated
  trades counts and config-settings values in existing fixtures. Do not
  regenerate goldens to hide that drift; analyze/approve it independently.
- Simulated Parquet write/finalization failures and disk-state restarts test
  checkpoint sequencing and manifest publication. Process kills, destination
  transaction failure, and worker-count changes **during the same checkpointed
  attempt** remain different, unproven recovery scenarios (Tickets 02/04).
- Leave CLI compatibility and new commands to Ticket 06; storage comparison
  and remote commit design to Ticket 07. No flowctl or raw-source rewrite is
  required for this package boundary.
