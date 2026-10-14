# TICKET 03 — incremental processing boundaries

Status: in progress. The application still builds from flat `package main`; no
importable Bronze package is claimed yet. Keep the CLI and output unchanged
while carving out tested boundaries.

## First slice: Parquet worker boundary

`Worker` now accepts a private `shardWriter` with `WriteBatch` and `Close`.
Production still supplies the same `ParquetWriter`. A fixture-driven failure
test injects a batch-write failure and a finalization failure, reloads the
checkpoint from disk, and verifies neither marks the shard complete. A retry
then processes the full range and checkpoints only after successful Close.
This is a **Parquet footer** boundary, not proof of a DuckLake or PostgreSQL
commit, and the test simulates failures rather than killing a process.

## Recovery slice: real Parquet restart

`TestOrchestratorResumesIncompleteParquetShard` stages two real Parquet
shards and a saved checkpoint: shard 0 is complete; shard 1 is incomplete.
It simulates both a truncated file and a finalized file left before its
checkpoint, then runs the real orchestrator. It verifies that the complete
shard's bytes stay unchanged, all stale incomplete files are removed, the
resumed attempt matches a clean Bronze digest, and a manifest is published
only after recovery. This is a deterministic disk-state simulation, **not**
a process-kill or remote DuckLake/PostgreSQL commit test. Those remain open.

## Second slice: mapping separated from worker state

`extractBronzeLedger` now accepts decoded ledger metadata, network passphrase,
table selection, and a diagnostic worker ID. `Worker.extractLedger` is a small
adapter; a direct test processes the restored-entry fixture without a worker,
checkpoint, CLI, or destination. The function is still private and remains in
the flat package until the row contracts and second consumer are specified.
All eight full-output goldens and the worker fault tests guard the change.

## Extraction ownership discovered during inspection

At the beginning of this ticket, `Worker.extractLedger` called
`stellar-extract@v0.1.2` for 17 tables, including `restored_keys`. The local
function fixed in commit `9d77335` was unused by the worker; the production
path still missed automatic restorations. This is now corrected deliberately:
`restored_keys` uses local authoritative `LEDGER_ENTRY_RESTORED` changes,
with a real pubnet fixture; the other 16 library mappings remain pinned.

A trial upgrade to `stellar-extract@v0.1.6` yielded the same 22 restored keys
for mainnet ledger **61,500,126**, but **also changed trades row counts in six
existing fixtures and 13 config-settings columns in the protocol-boundary
fixture**. The trial upgrade was reverted rather than silently approving
unrelated Bronze contract changes. The newer library adds executable-type
fields not present in the v3 contract-creations schema. Record these as parity
questions before any full dependency upgrade; the local restored mapping is a
scoped interim fix, not a second canonical implementation to keep forever.

The four other local paths still invoked for `transactions`, `operations`,
`ledgers`, and `accounts` have additional mapping/normalization logic.
Do not move the 21 extractors blindly into new packages: first inventory
which implementation each table actually uses and test its row-level contract.

## Next slices

- TICKET 02's real `restored_keys` fixture and 21-table coverage are now in
  place. Investigate the `stellar-extract@v0.1.6` trades, config-settings,
  and contract-creation differences before switching the other mappings.
- Add true process-kill tests and destination transaction fault tests;
  simulated disk-state recovery does not prove remote sink idempotence.
- Move the now-isolated extraction logic into a cohesive package one table
  group at a time, leaving storage transactions destination-specific.
- Add externally importable Bronze APIs only after the live ingester's second
  consumer requirements and all-table parity are characterized.
