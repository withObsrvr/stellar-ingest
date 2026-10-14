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

## Extraction ownership discovered during inspection

`Worker.extractLedger` calls `github.com/withObsrvr/stellar-extract@v0.1.2`
for 17 tables, including `restored_keys`. The local `extractRestoredKeys`
function in `extractors_soroban.go` is **not called by the worker**. The fix
in commit `9d77335` is tested for that local function's footprint selector but
**does not fix production extraction**. The pinned library still searches for a
`RestoreFootprint` operation and misses fee bumps and automatic restoration.
A newer `stellar-extract@v0.1.6` implements restored-key extraction from
`LEDGER_ENTRY_RESTORED` changes, which is a different and more comprehensive
contract. Its documentation points to mainnet ledger **61,500,126** as an
example with 22 automatic restorations. That ledger was fetched successfully
from the pubnet GCS archive; pin an XDR fixture and verify parity before
upgrading the dependency. See TICKET 02 for the fixture gate.

The four local paths still invoked for `transactions`, `operations`,
`ledgers`, and `accounts` have additional mapping/normalization logic.
Do not move the 21 extractors blindly into new packages: first inventory
which implementation each table actually uses and test its row-level contract.

## Next slices

- Close TICKET 02's real `restored_keys` fixture using the discovered ledger;
  compare old and proposed extractor output deliberately.
- Add an orchestrator-level failure/restart test using real Parquet files;
  a writer mock alone does not prove cleanup or manifest publication.
- Isolate extraction independently of scheduling after output parity; migrate
  one table group at a time, leaving storage transactions destination-specific.
- Add externally importable Bronze APIs only after the live ingester's second
  consumer requirements and all-table parity are characterized.
