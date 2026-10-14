# Ticket 04 — developer handoff: historical/live Bronze discrepancy report

**Scope:** compare `stellar-ingest` and `stellar-postgres-ingester` field by field
for all 21 Bronze outputs, using identical authoritative `LedgerCloseMeta` XDR.
Deliver a reviewed discrepancy report and reproducible parity harness. **Do not
switch the live ingester to a new mapper, upgrade `stellar-extract`, change its
batch/commit/checkpoint behavior, or reroute production in this ticket.**
Ticket 03's boundary inventory is in [`module-boundaries.md`](module-boundaries.md).

## Starting revisions and locations

Pin these again when work starts; they are observations at handoff time, not
floating branch names.

| Codebase | Path / revision observed | Entry points |
|---|---|---|
| Historical | this repo, `64886287fb2b6fff19c57563c199f8e7cae90876` | `worker.go:extractBronzeLedger`, `types.go`, `v3_bronze_schema.sql`; `go.mod` pins `stellar-extract@v0.1.2` |
| Live | `../ttp-processor-demo/obsrvr-lake/stellar-postgres-ingester`, parent repo `bd618bb4706e983eb305bc43bb5fc90c9f6c6fb5` | `go/writer.go:WriteBatch`, `extractLedgerDataFromLCM`, `convert*`, `insert*`; `go/types.go`; `go/go.mod` pins `stellar-extract@v0.1.5` |
| Live source message | `../ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service/raw_ledger_service.pb.go` | `RawLedger{Sequence, LedgerCloseMetaXdr}`; the live Go module has a relative `replace` to this sibling module |
| Hot schema | `../ttp-processor-demo/postgres-hot-buffer/schema/` plus the live ingester's numbered `migrations/001`–`014` | Compare **actual deployed migration level** separately; `stellar-postgres-ingester/schema/` is not the full hot-buffer DDL |

Both working trees were clean when inspected. Both modules' `go test ./...`
passed locally (historical from this repo; live from its `go/` directory).
These unit passes are **not** a parity result or a database integration test.
Build live code from the parent monorepo layout: its `go.mod` uses a relative
`replace`, so an isolated copy of `go/` will not resolve the source module.

## Actual extraction path — avoid dead code

Historical `extractBronzeLedger` runs 16 table mappings through the pinned
`stellar-extract` library, four locally (`transactions`, `operations`,
`accounts`, `ledgers`), and `restored_keys` through
`internal/bronze/restored`. The worker additionally sanitizes transaction
memos and event UTF-8, converts config-setting rows, and stamps `era_id`.
Parquet rows are **not** identical to upstream library rows.

Live `WriteBatch` decodes each `RawLedger.LedgerCloseMetaXdr` with
`extract.NewLedgerInputFromXDR`, uses `extractLedgerDataFromLCM` for the ledger
row, calls the **v0.1.5 library** for the other 20 mappings, applies
`writer.go:convert*` and additional transaction-XDR/auth-credentials work,
then writes via `insert*`. Its older `go/extractors*.go` methods are not proof
of production behavior: confirm call sites before comparing any similarly
named function. In particular, `go/extractors_soroban.go:extractRestoredKeys`
is *not* called by `WriteBatch`; the library call is. The local function's
`restored_from_ledger=0` and non-applicable durability labels must **not** be
reported as live output unless a call path to it is established.

Live reconciliation may append **RPC-observed** system-managed config-setting
rows after XDR extraction (`writer.go:WriteBatch`,
`config_settings_reconciliation.go`). For raw-XDR parity, disable that
optional source first, then report reconciliation as a separate provenance and
semantic policy question. Live extract errors on many non-transaction tables
are logged and skipped, whereas transaction errors fail the batch; compare
failure behavior, not just successful rows. `commitAndPersistCheckpoint`
commits PostgreSQL before persisting progress; keep these semantics intact.

## Fixtures and first executable comparison

Committed historical inputs are under `testdata/fixtures/<name>/` with
`fixture.json` (network, inclusive sequence range, SHA-256 of **decompressed**
XDR) and `ledgers.xdr` or `ledgers.xdr.zst`. Matching snapshots are in
`testdata/golden/<name>.json`. Useful starting cases:

- `pubnet-genesis-p1`: empty-table and early-protocol behavior.
- `pubnet-classic-p19` and `pubnet-protocol-boundary`: classic and P19→P20,
  including config-settings.
- `pubnet-state-eviction-p23`: archival changes.
- `pubnet-auto-restore-p25`: **ledger 61,500,126**, 22 automatic restorations
  without `RestoreFootprint` operations; restored-key identity/null semantics.
- `pubnet-contract-creation`, `pubnet-soroban-p29`, `pubnet-tiny-dump`:
  contract creation, modern events/state, and compact diagnostics. P29 also
  has two restored keys.

```sh
# Historical baseline (in this repo, CGO is required by DuckDB):
GOWORK=off CGO_ENABLED=1 go test ./...
make golden

# Live baseline (preserve the sibling source-module layout):
cd ../ttp-processor-demo/obsrvr-lake/stellar-postgres-ingester/go
go test ./...
```

**Recommended harness:** in a *test-only* live adapter, decode each committed
XDR record, verify its sequence against `RawLedger.Sequence`, set the exact
fixture network passphrase, and invoke the same live library calls, `convert*`
functions, transaction-XDR enrichment, and ledger-local mapping used by
`WriteBatch`. Do not need a real gRPC server or PostgreSQL for extraction
comparison; avoid assuming `WriteBatch` is a pure extractor (it begins a DB
transaction and writes checkpoints). If splitting it to test, keep a golden
regression around the existing production call path. Represent outputs as
versioned typed rows or canonical JSON from each **application adapter**,
not directly from the upstream library. Separately test `insert*` against an
isolated hot-buffer schema/migration setup to catch persisted-column omissions
or `ON CONFLICT` transformations.

For the historical side, drive `extractBronzeLedger` or read Parquet using
DuckDB as `golden_harness.go` does. Normalize *only documented* run-dependent
values (`created_at`, `updated_at`, `ingestion_timestamp`, `version_label`,
`era_id` as appropriate); retain network, XDR/base64, TOIDs, closed-at times,
booleans, and NULL-vs-empty distinctions. Align names/types, canonicalize
JSON objects without flattening away arrays, compare both row counts and
row multisets keyed by a documented logical key (or sorted canonical rows if
no unique key exists). Record whether a table is absent, emitted with zero
rows, or nonzero. The existing golden hashes are a **historical regression
baseline**, not substitutes for live row-level comparisons.

## Investigation matrix (one row per table; no silent exclusions)

| Table / extractor | Historical owner | Live owner | Special checks |
|---|---|---|---|
| transactions | local | library + XDR overlay | Envelope/result/meta/fee-meta/signatures, memo UTF-8, fee bumps, account sequence, TOID and index |
| operations | local | library + auth overlay | TOID, invocation/call graph, auth credentials, footprint counts, contract errors, ordered arrays |
| effects | library | library | Type and JSON payload, ordering/index and retry behavior |
| trades | library | library | Offer/claim atomics, pool trades, indices; version change risk |
| accounts | local | library | Snapshot/delete semantics, liabilities, sequence and timestamps |
| offers | library | library | Asset identity, amount/price, delete state |
| trustlines | library | library | Asset identity, authorization/liabilities, deletion |
| account_signers | library | library | Signer identity/weight and sponsorship |
| claimable_balances | library | library | ID, asset, claimants, deleted snapshots |
| liquidity_pools | library | library | Pool ID, reserves/shares, deleted snapshots |
| config_settings | library + adapter | library + adapter + optional RPC | Nulls, system-managed IDs, observation source/resolved ledger, reconciliation versus XDR |
| ttl | library | library | Key hash, expiry, last-modified and delete semantics |
| evicted_keys | library | library | Key hash, change type, archival-era coverage |
| contract_events | library + UTF-8 repair | library | Diagnostic versus transaction/operation events, success, XDR, indices, topics |
| contract_data | library | library | Entry change/delete, balance holder/value, persistent vs temporary, token fields |
| contract_code | library | library | Hash/XDR and WASM metadata NULLs |
| native_balances | library | library | Account balance, sponsoring counts, sequence |
| restored_keys | private package | library | Automatic restore, durability, key hash, `restored_from_ledger` meaning |
| contract_creations | library | library | Rare deployment, WASM hash and schema evolution |
| ledgers | local | local | Protocol eras, operation counts, Soroban aggregates, entry-change stats, hash/header bytes |
| token_transfers | library | library | Fees without parent operation, amount precision, TOIDs and nullable addresses |

For each table: enumerate every **actual Parquet and persisted PostgreSQL
column** (not just Go struct fields), mapping source field → adapter → writer
column; list row identity, data type, NULL/default, normalization, and any
hot-only/cold-only/provenance-only fields. Use historical
`v3_bronze_schema.sql` plus the hot-buffer base DDL and numbered migrations;
do not assume schemas are identical because names look alike. Explicitly
separate `source difference`, `extractor version`, `adapter conversion`,
`storage schema/insert`, and `intended provenance` causes.

## Known risks to confirm, not pre-approved discrepancies

1. **Pinned dependency mismatch:** historical `stellar-extract@v0.1.2`, live
   `@v0.1.5`. A trial historical bump to `v0.1.6` changed trades counts in
   six existing fixtures and 13 config-settings columns at the P19/P20
   fixture. The bump was reverted; isolate the `v0.1.2→v0.1.5` delta instead
   of assuming the `v0.1.6` results apply unchanged to the live app.
2. **Restored keys:** both *active* paths read `LEDGER_ENTRY_RESTORED`. The
   historical package and live v0.1.5 library derive durability and
   `restored_from_ledger` from the restored entry. Check the 22-row fixture
   row-by-row. `LastModifiedLedgerSeq` is the *restoration* ledger here, not
   necessarily the ledger from which the entry was originally archived;
   document/rename this field only after a schema decision.
3. **Live-only ledger fields:** live `LedgerData`/migration 014 expose
   `entry_changes_{created,updated,deleted,restored,total}`, per-type JSON,
   and extra aggregates. Determine if they are legitimately hot-only or a
   missing historical mapping. Likewise inspect migration 012 footprint
   counts and 013 contract errors in operation rows.
4. **Config reconciliation:** live can insert RPC-observed rows absent from
   authoritative XDR; treat differing provenance/availability separately
   from a broken raw-ledger extractor. Check `observation_source`,
   `resolved_at_ledger`, state table and fail-open/fail-closed behavior.
5. **Failure modes and replay:** some live library errors are warn-and-skip;
   post-commit checkpoint save failures only warn, while historical extraction
   fails a shard on extractor errors. Record the impact without changing
   either production policy in this ticket.

## Deliverables and exit gate

1. `docs/ticket-04-discrepancies.md` (or equivalent) with a **21-row status
   matrix**: `equal`, `provenance-only`, `approved semantic difference`,
   `bug`, or `unverified`; links to files/lines, fixture/ledger, counts,
   column/value samples, cause, downstream impact, decision owner. An
   `unverified` row is not a pass.
2. Repeatable test-only parity runner and machine-readable per-fixture,
   per-table counts/differences, including the full 22-row restore ledger;
   deterministic normalization rules and reference revision pins. A
   credential-free extraction tier should run in CI; PG persistence tests can
   be separately gated on an ephemeral migrated database.
3. Versioned candidate canonical row contract: table/column types, IDs,
   key/duplicate policy, null semantics, raw XDR fidelity, and handling of
   protocol eras, missing data, and provenance. Do **not** assume that
   `stellar-extract`'s Go structs alone are the stored contract.
4. Conclusions and follow-up tickets ranked by data-loss risk. No automatic
   golden regeneration, dependency bump, live route change, or database
   migration until discrepancies are reviewed and explicitly approved.

The handoff is a starting inventory, **not** a claim that historical/live
rows currently match. If live deployment revisions or migrations differ from
these local files, record the deployed reality first.
