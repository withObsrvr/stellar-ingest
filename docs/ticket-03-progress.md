# TICKET 03 — incremental processing boundaries

Status: **Ticket 03's bounded module-boundary gate complete.** The CLI and
most application code still build from flat `package main`; a private
`internal/bronze/restored` mapper and worker-independent orchestration are
now separated and tested. See `module-boundaries.md` for the 21-table ownership
inventory and the explicitly deferred public API / live-parity work. This is
not a claim that the target architecture or Ticket 04 is complete.

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

## Third slice: private Bronze mapping package

`internal/bronze/restored` owns XDR-change-to-restored-row mapping. Its
`Row` has only ledger-derived fields; the legacy ingestion timestamp is added
at the application's row adapter. A package-local fixture test verifies 22
rows with **zero RestoreFootprint operations**, key durability, and raw XDR
round-trip; the full-output goldens verify the adapter and Parquet fields.
No CLI, worker, checkpoint, or sink is needed to invoke the package. We kept
it `internal` rather than publishing a premature 21-table external API.

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
- The 16 library mappings already live in the separate `stellar-extract`
  module. The four historical local mappings (transactions, operations,
  accounts, ledgers), their adapter transformations, and versioned public row
  contracts still need comparison with the live ingester in Ticket 04. Do not
  promote a partial API as canonical 21-table Bronze.
- Add externally importable Bronze APIs only after the live ingester's second
  consumer requirements and all-table parity are characterized.
