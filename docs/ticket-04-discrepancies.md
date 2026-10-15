# Ticket 04 — historical/live Bronze discrepancy report

**Status: fifth pass. Both ingesters on `stellar-extract` v0.2.1, 18 of 21
tables equal at the extraction tier, and both storage tiers verified — the
live one against a real migrated hot buffer, the historical one against
Parquet read back through DuckDB.**

The storage tier was worth running: it found that **86% of contract events
were being discarded on insert** — invisible to every extraction-tier
comparison, because both ingesters extract the same rows and agree exactly.
See **F12**.

F1, F2, F3 and F5 are closed, along with the three findings the adapter
audit produced. Three tables still differ, and two of the three differences
are decisions rather than defects. One new defect appeared: **F10**, in this
repository.

Both sides are committed and both pins are clean, so these numbers are
reproducible from the two recorded revisions.

This report compares the Bronze output of this repository's historical
extractor against `stellar-postgres-ingester`'s live extractor over identical
authoritative `LedgerCloseMeta` XDR. Scope, method and the harness are in
[`ticket-04-progress.md`](ticket-04-progress.md); the per-fixture numbers are
in [`ticket-04-parity-matrix.md`](ticket-04-parity-matrix.md), regenerated
from the digests committed under `testdata/parity/`.

Nothing here has been fixed, bumped, migrated or rerouted. The ticket's exit
condition is a reviewed report, and every row below carries a decision owner
precisely because the decisions have not been made.

## Pinned revisions

| Side | Revision | `stellar-extract` |
|---|---|---|
| Historical | `stellar-ingest` @ `afa8da7` | `v0.2.1` |
| Live | `ttp-processor-demo` @ `83cbd2e`, `obsrvr-lake/stellar-postgres-ingester` | `v0.2.1` |

Both trees were clean apart from this ticket's own additions. Fixtures:
the eight committed pubnet fixtures under `testdata/fixtures/`, SHA-256
verified on the decompressed XDR by both sides before extraction.

## What was compared, and what was not

Compared: the rows each side's **application adapter** produces — for the
historical side `extractBronzeLedger`, for the live side the extraction half
of `WriteBatch` including its `convert*` functions, transaction-XDR overlay
and auth-credential enrichment. Not the upstream library structs.

**Not compared — the storage tier.** Neither side's `INSERT`/Parquet write is
exercised. This is not a formality: `insertTransactions` recomputes
`envelope_size_bytes` from the stored envelope and sanitizes memo strings
*after* extraction, so at least two differences below are repaired before
they reach PostgreSQL. Every table is therefore `unverified` at the storage
tier, and the two tiers must not be conflated.

Also out of scope by design: RPC config-settings reconciliation (disabled, so
the comparison stays raw-XDR), and the two sides' asymmetric extractor-failure
policies, recorded under *Findings* rather than reproduced.

Columns a side stamps with a wall clock are detected by re-extracting each
fixture and left out of the comparison; they are listed per table in the
matrix. See finding **F7** — which columns those turned out to be is itself a
result.

## Status matrix

Review status vocabulary: `equal`, `provenance-only`, `approved semantic
difference`, `bug`, `unverified`. An `unverified` row is not a pass.

| # | Table | Extraction status | Finding | Decision owner |
|---|---|---|---|---|
| 1 | `trades` | **bug** | **F1** — amounts in different units (10⁷); live finds 62 trades historical misses; historical emits 3 zero-amount rows live does not | Data platform |
| 2 | `config_settings` | **bug** | **F2** — 13 columns NULL on historical, populated on live | Data platform |
| 3 | `operations` | **bug** | **F3** — live loses contract call-graph fields; **F4** — 36 classic operation detail columns absent from live | Live ingester |
| 4 | `accounts` | **bug** | **F5** — `sequence_ledger` / `sequence_time` are zero on live into columns that exist | Live ingester |
| 5 | `transactions` | **approved semantic difference** + unverified | **F6** — memo representation differs by policy; `envelope_size_bytes` differs at extraction but is repaired at insert | Data platform |
| 6 | `ledgers` | **provenance-only** | **F8** — live-only entry-change aggregates; `SorobanFeeWrite1kb` vs `1KB` casing | Both |
| 7 | `restored_keys` | **equal** | 22/22 rows match on the automatic-restoration ledger; 2/2 on P29 | — |
| 8 | `effects` | **equal** | — | — |
| 9 | `offers` | **equal** | — | — |
| 10 | `trustlines` | **equal** | — | — |
| 11 | `account_signers` | **equal** | — | — |
| 12 | `claimable_balances` | **equal** | — | — |
| 13 | `liquidity_pools` | **equal** | — | — |
| 14 | `ttl` | **equal** | — | — |
| 15 | `evicted_keys` | **equal** | 2,000 and 10,000 rows matched on the archival fixtures | — |
| 16 | `contract_events` | **equal** | — | — |
| 17 | `contract_data` | **equal** | — | — |
| 18 | `contract_code` | **unverified** | No fixture emits a row | Data platform |
| 19 | `native_balances` | **equal** | — | — |
| 20 | `contract_creations` | **equal** | 1 row matched on the contract-creation fixture | — |
| 21 | `token_transfers` | **equal** | — | — |

Eleven tables match on every compared column across every fixture where they
produce rows. Four carry a defect. One is an intended difference. One is
provenance. Two — `contract_code`, and `config_settings` outside the
protocol-boundary fixture — are not exercised by the fixture set.

## Where each finding stands after the v0.2.1 bump

| Finding | Status | Note |
|---|---|---|
| F1 units (10⁷) | **closed** | both sides use `amount.String` |
| F1 row set | **closed** | `trades` is equal on every fixture; 135 pool trades recovered on `pubnet-tiny-dump` alone |
| F2 config NULLs | **closed** | all 13 columns match |
| F3 call graph | **closed** | library populates it; live's `convertOperation` already copied the fields |
| F4 36 op columns | **closed** | migration 021; `operations` has no one-sided columns |
| F5 account sequence | **closed** | `convertAccount` now maps them; the columns had existed since migration 009 and were receiving zeros |
| F6 memo policy | **closed** | raw bytes kept in `memo_raw`; `memo_type` is the protocol value |
| F6 `envelope_size_bytes` | open — unverified | recomputed at insert; needs the storage tier |
| F7 `created_at` | **closed** | split into `closed_at` and `ingested_at` |
| F8 ledgers | open — provenance-only | — |
| **F10 ScVal rendering** | **closed** | local converter deleted; both sides use the library's |
| **F11 `soroban_contract_id`** | open, minor | 1 row of 2,913 |
| **F12 contract events discarded** | **fixed** | 86% of rows lost on insert; migration 018 |
| **F13 event identity** | **closed** | `stellar-extract` v0.3.0; one index across all three streams |
| **F14 Parquet all-NULL column** | **fixed** | `tx_max_footprint_entries` never populated |
| **F15 `claimable_balances`** | open, minor | declares `ingested_at` in SQL, Parquet never writes it |
| **F16 `token_transfers` has no key** | **closed** | `stellar-extract` v0.3.0 adds `event_index` |
| L1–L4 library gaps | **closed** | `stellar-extract` v0.2.1 |

Three tables still differ:

- **`operations`** — F4's 36 columns, plus F11 below.
- **`transactions`** — F6's memo policy and the `envelope_size_bytes`
  artefact, plus the known column-set differences.
- **`ledgers`** — provenance-only (F8).

Everything else is equal on every compared column across all eight fixtures.

Three findings are new since the first pass, all of them fields that both
stores were missing — so the comparison reported the tables as `equal` and
could not have surfaced them. They were found by auditing each adapter
against the library's field surface instead:

- `contract_creations.executable_type` (plus `external_ref_owner` / `_tag`).
  The library stopped making consumers infer SAC-vs-WASM from a NULL
  `wasm_hash` because Protocol 28's CAP-0085 added a third kind and broke the
  inference. Both stores still made it. **Now carried by historical.**
- `config_settings.tx_max_footprint_entries`, the CAP-0062 denominator.
  **Now carried by historical.**
- `trades.liquidity_pool_id`, which did not exist because no AMM trade was
  ever emitted. **Now carried by historical.**

That `equal` can mean "both implementations are wrong the same way" is the
central methodological limit of this comparison. Both repositories now carry
an adapter-coverage audit that asks the other question — does this ingester
read everything the library offers — and the historical one caught
`tx_max_footprint_entries` the moment the pin moved.

## Findings

### F1 — `trades`: amounts stored in different units, and a 28% row shortfall

Highest data-loss risk in the ticket.

**Units.** For trades both sides emit, every amount differs by exactly 10⁷.
Matched by `(transaction_hash, operation_index, trade_index)` on
`pubnet-tiny-dump`:

| Column | Historical (`v0.1.2`) | Live (`v0.1.5`) |
|---|---|---|
| `SellingAmount` | `70627` | `0.0070627` |
| `BuyingAmount` | `72485` | `0.0072485` |
| `Price` | `72485/70627` | `0.0072485/0.0070627` |

Historical stores raw stroops; live stores XLM-scaled decimals. A consumer
reading `selling_amount` from the cold store and the hot store gets values a
factor of ten million apart, with nothing in either schema to say which is
which. `Price` is rescaled on both sides of the ratio, so the price is
numerically unchanged but the stored text is not.

**Coverage.** On the same fixture the two sides agree on 157 trades by key
but live emits 219 rows to historical's 160:

- **62 trades present on live and missing from historical** — real trades
  with real amounts. Across the fixture set the shortfall runs in both
  directions by count (`pubnet-classic-p19` 867 vs 745; `pubnet-soroban-p29`
  14 vs 233), so this is not a uniform offset.
- **3 trades present on historical only**, all with `selling_amount` and
  `buying_amount` of zero — rows `v0.1.2` appears to emit for trades it could
  not resolve.

Cause: `extractor version`. Both sides call the library; only the pin differs.
This is the concrete shape of the handoff's risk 1, and it confirms that the
`v0.1.2→v0.1.5` delta cannot be waved through.

**Impact:** silent, large numerical error in any consumer joining or
comparing the two stores; missing trades in the historical record.
**Not addressed here:** the fix is a dependency bump, which this ticket
forbids until reviewed.

### F2 — `config_settings`: 13 columns NULL on historical

On `pubnet-protocol-boundary` (the only fixture that emits config settings),
3 of 14 rows differ. Historical writes NULL where live writes a value:

`ContractMaxSizeBytes` (NULL vs `2000`), `FeeRatePerInstructionsIncrement`
(NULL vs `100`), `LedgerMaxInstructions`, `LedgerMaxReadBytes`,
`LedgerMaxReadLedgerEntries`, `LedgerMaxWriteBytes`,
`LedgerMaxWriteLedgerEntries`, `TxMaxInstructions`, `TxMaxReadBytes`,
`TxMaxReadLedgerEntries`, `TxMaxWriteBytes`, `TxMaxWriteLedgerEntries`,
`TxMemoryLimit`.

Cause: `extractor version`. Same 13 columns the handoff recorded from the
trial `v0.1.6` bump, now confirmed against the live app's actual `v0.1.5`.

**Impact:** historical Bronze cannot answer protocol-capacity questions for
these ledgers. Direction is the opposite of F1's row counts — here historical
is the lossy side.

### F3 — `operations`: live loses the contract call graph

`ContractCallsJSON`, `ContractsInvolved` and `MaxCallDepth` are populated on
historical and NULL on live. On `pubnet-soroban-p29`, 287 operation rows
differ on these fields alone.

Cause: `adapter conversion`, and specifically the dead-code trap the handoff
warned about. Live *has* a call-graph implementation in
`go/call_graph.go`, and `integrateCallGraph` would populate these fields —
but its only caller is `(*Writer).extractOperations` in `go/extractors.go`,
which `WriteBatch` never calls. Production takes
`extract.ExtractOperations` → `convertOperation`, and `v0.1.5` leaves the
three fields nil. `convertOperation` copies them faithfully; there is nothing
to copy.

The library side is worse than "not implemented". `ContractCallsJSON`,
`ContractsInvolved` and `MaxCallDepth` are *declared* on
`extract.OperationData` (`types_core.go:150-152`) and assigned **nowhere in
the module** — they are dead fields that always read nil. A consumer has no
way to tell that from a ledger that genuinely has no cross-contract calls.
See **L1**.

**Impact:** cross-contract call analysis is unavailable from the hot store
despite the code to produce it being present and tested.

### F4 — `operations`: 36 classic detail columns absent from live

Historical operation rows carry 36 fields the live row type has no column
for: the asset triples (`AssetCode`/`AssetIssuer`/`AssetType` and the
buying/selling/source variants), `OfferID`, `Price`, `PriceR`,
`StartingBalance`, `TrustlineLimit`, `BalanceID`, `BumpTo`, the threshold and
weight fields, `HomeDomain`, `SetFlags`/`ClearFlags`, `DataName`/`DataValue`,
`DestinationMin`, `SourceAmount`, `SponsoredID`, `SourceAccountMuxed`,
`SorobanAuthRequired`.

Cause: `adapter conversion`, **not** a source limitation. `stellar-extract`
populates every one of these fields on its own `OperationData` — they are
assigned across `operations.go:79-309`, one branch per classic operation type —
and `convertOperation` simply does not copy them, because the live
`OperationData` is declared "simplified - core fields only" and has no
columns for them.

So the data reaches the live adapter and is discarded there. The decision is
still live's to make, but it is a decision to drop populated fields, not an
absence to be filled: widening the live row type and its insert is a
mechanical change, with the hot-buffer schema the only open question.

### F5 — `accounts`: `sequence_ledger` and `sequence_time` zeroed on live

Both columns exist in the live hot-buffer schema and are written by
`insertAccounts` (`sequence_ledger = EXCLUDED.sequence_ledger`), but
`convertAccount` never sets them, because `extract.AccountData` in `v0.1.5`
has no such fields. Live therefore persists `0` into real columns on every
account row — 581 to 4,934 rows per fixture. Historical populates both from
its local `extractAccounts`.

Cause: `adapter conversion` over an `extractor version` gap. This is the
worst kind of the three: a zero in a populated column reads as data, not as
absence.

### F6 — `transactions`: memo representation, and an extraction-tier artefact

Two separate things, one of which is not a defect.

**Memo (genuine, policy-level).** Historical base64-encodes *every* `text`
memo and retypes it `text_base64`; live stores the raw text with UTF-8
sanitization applied at insert time.

| | `MemoType` | `Memo` |
|---|---|---|
| Historical | `text_base64` | `MTI1NjQ0ODkzMTY3NDAwMzc0MTc=` |
| Live | `text` | `12564489316740037417` |

The historical behaviour is deliberate — `sanitizeTransactionMemo` exists
because memo bytes need not be valid UTF-8 and a Parquet text write rejects
them. But it is applied unconditionally, so a perfectly valid ASCII memo is
also base64'd. A consumer reading `memo` gets different content depending on
the store, with `memo_type` as the only signal.

**`EnvelopeSizeBytes` (extraction-tier artefact).** Live reports `0` at
extraction because `convertTransaction` does not map it and `WriteBatch`
copies only five fields from the transaction-XDR overlay. It is then
recomputed inside `insertTransactions` from the stored envelope. The
persisted value is probably equal; this harness cannot see it. Classified
`unverified`, not `bug`, pending the storage tier.

**Column-set differences** (no decision implied, listed for the record):
historical-only `SorobanContractID`, `SorobanHostFunctionType`,
`SourceAccountMuxed`, `TimeboundsMinTime`, `TimeboundsMaxTime`; live-only
`ContractErrorType`, `ContractErrorCode`, `SorobanFootprintReadEntries`,
`SorobanFootprintWriteEntries` (migrations 012 and 013).

### F7 — `created_at` means two different things, and the name does not say which

Not a historical/live difference — both sides do it — but it surfaced while
building the harness and it affects how every other row here must be read.

Re-extracting each fixture twice shows `created_at` is **ledger close time**
(reproducible) on `transactions`, `operations`, `trades` and `effects`, and an
**ingestion wall clock** (not reproducible) on `accounts`,
`account_signers`, `trustlines`, `offers`, `liquidity_pools`, `ttl`,
`contract_data`, `contract_events` and `token_transfers`. `accounts` also
stamps `updated_at`; `ledgers` stamps `ingestion_timestamp`.

So one column name carries two incompatible meanings across the Bronze
surface, and nine tables contain a column that cannot be reproduced from the
ledger. The existing golden suite's `nonDeterministicColumns` excludes
`created_at` globally, which is right for nine tables and wrong for four —
it has been suppressing a real comparison on transactions and operations.

**Impact:** any consumer treating `created_at` uniformly as event time is
wrong on nine tables. This belongs in the canonical row contract
(deliverable 3), not in a bug fix.

### F8 — `ledgers`: provenance-only, plus a casing mismatch

Live-only: `EntryChangesCreated`, `EntryChangesUpdated`,
`EntryChangesDeleted`, `EntryChangesRestored`, `EntryChangesTotal`,
`EntryChangesByType`, `EvictedKeysByType` — migration 014 aggregates, legitimately
hot-only. Historical-only: `PipelineVersion`.

Every shared column is equal on all eight fixtures.

One thing to fix cheaply: both sides carry the same Soroban write fee, named
`SorobanFeeWrite1kb` historically and `SorobanFeeWrite1KB` live. Identical
concept, and the harness reports it as two one-sided columns because it
compares by name. Harmless today, a trap for any shared row contract.

### F9 — asymmetric failure policy (recorded, not reproduced)

Historical fails the whole shard when any of its 21 extractors errors. Live
fails the batch on transaction errors only and logs-and-skips the other 19,
then commits and advances the checkpoint. A live extractor fault therefore
produces a committed ledger with a silently empty table and no replay, where
the historical pipeline would stop.

The parity adapter deliberately returns every error instead of mirroring
this, so a harness fault can never masquerade as "the live side emitted no
rows". Confirming the production behaviour's blast radius needs a fault-
injection test against a migrated database; it is not changed here.

### F10 — ScVal bytes rendered as a boolean — **closed**

This repository rendered a 32-byte ScVal as `false`.

`scval_converter.go` tried to decode `ScvBytes` as a nested XDR ScVal
first, on the theory that some contracts wrap structured data in bytes. The
decode was accepted whenever it merely parsed; it was never required to
consume the input. A 32-byte Ethereum address left-padded with zeros begins
with four zero bytes, which parse as `ScvBool`, and the next four as
`false` — the remaining 24 bytes were discarded and the value became a
boolean. Any byte string whose prefix forms a valid ScVal was exposed to the
same reinterpretation, and zero-padded addresses and hashes are common.

Fixed by deleting the local converter and using the library's, which has no
such heuristic. The two handle an identical set of ScVal arms; the only other
difference was `ScvExecutableTag`, reported under key `tag` locally and
`value` in the library.

Scope was the locally-extracted `operations` path —
`contract_calls_json` and `soroban_arguments_json`. `contract_events` takes
the library route on both sides and already compared equal, which is why
this survived until the call graph existed on both sides to compare.

### F11 — `operations.soroban_contract_id` on a contract-creation operation

One row of 2,913 on `pubnet-contract-creation`. Historical reports the
contract ID; the library reports NULL.

The operation is an `InvokeHostFunction` whose host function creates a
contract rather than invoking one. The library's
`extractContractInvocationDetails` resolves a contract ID only for the
invoke case; this repository's local extractor also derives one for the
create case.

Which is correct is a contract question, not a bug report: `contract_id` on
an operation that creates a contract is arguably the created contract, and
arguably nothing, given `contract_creations` already records the creation.
Left for the canonical row contract. It is the last remaining value
difference in `operations`.

## Storage tier — live

Verified by building a hot buffer from `v3_postgres_schema.sql`, the
root-level constraint files and all numbered migrations, then driving the
real `insert*` path over `pubnet-tiny-dump` and reading the rows back.
`go/storage_tier_test.go`, gated on `PARITY_PG_DSN`.

Fifteen of the tables the fixture exercises round-trip exactly: every row
extracted is a row stored. One did not.

### F12 — `contract_events` discarded 86% of its rows on insert

The primary key was `(ledger_sequence, transaction_hash, event_index)`, but
`event_index` restarts at 0 for each operation in a transaction. Events from
the second operation onward collided with rows already inserted, and
`insertContractEvents`' `SELECT DISTINCT ON` over the same triple discarded
most of them before PostgreSQL saw them.

| | rows |
|---|---:|
| extracted | 14,515 |
| stored, old key | **2,035** |
| stored, with `operation_index` added | 13,894 |
| worst single collision | 148 events into one row |

Three ledgers. The cold store, writing Parquet with no unique constraint,
kept all 14,515 — so the two stores have diverged by this margin for as long
as the key has been wrong, while the extraction-tier comparison reported
`contract_events` as **equal** on every fixture.

Fixed in migration 018 plus the matching `DISTINCT ON`/`ON CONFLICT` change.
Rows already lost are not recovered by the migration; affected ranges must be
re-ingested, and the migration carries the operational notes.

### F13 — `contract_events` has no unique identity

What F12's fix does not reach. 621 of 14,515 extracted rows still share an
identity with another row: the contract and diagnostic event streams index
separately, so a `transfer` contract event and a `fee` diagnostic event from
different contracts can occupy the same `operation_index` and `event_index`.

Every one of those pairs **also shares an `event_id`**, so the library cannot
distinguish them either. Adding `event_type` to the key recovers 391 and
still leaves 230. There is no complete key among the stored columns.

The key therefore stops at the identity `event_id` already claims — one
stored row per distinct `event_id` — rather than growing wider while still
not being unique. The real fix is upstream: either `event_index` is made
unique across streams within an operation, or `event_id` is derived from
something that distinguishes them.

### Schema reproducibility

Three gaps between the committed DDL and what the ingester requires, each
found by the build failing:

1. **`contract_creations_v1` has no `CREATE TABLE`** in the hot-buffer DDL or
   any numbered migration — only the cold-side Bronze schemas define it.
   Migration 007 alters it, so a deployment built from the documented chain
   fails there, and because 007 aborts at that point, every later `ALTER` in
   it — including `token_transfers_stream_v1.era_id` — never runs. Migration
   016 now creates the table so it is at least self-sufficient; 007 remains
   unguarded.
2. **Six columns are written but never created**:
   `ledgers_row_v2.soroban_op_count`, `.total_fee_charged`,
   `.contract_events_count`; `transactions_row_v2.rent_fee_charged`;
   `effects_row_v1.operation_id` and `.details_json`. The last is where the
   content of all 50+ effect types lives. Added in migration 017, all
   `IF NOT EXISTS`.
3. **`postgres-hot-buffer/schema/migrations/` is not the deployed schema.**
   Its `trades_row_v1` uses Horizon's base/counter model; the ingester writes
   a seller/buyer model. The real DDL is `v3_postgres_schema.sql` at the
   monorepo root, plus the root-level `add_*_constraints` files. The handoff
   anticipated this; it is now confirmed.

Together these mean the committed chain could not reproduce the deployed
database. Production must have been patched out-of-band, so a rebuilt
environment or a new region silently differed until the first batch failed.

### Storage tier — historical

Verified. `parquet_coverage_test.go` audits the `LedgerData` → `Parquet*`
mapping statically; `parquet_storage_tier_test.go` writes a fixture through
the real orchestrator and reads the files back through DuckDB, comparing
row counts, column presence, null rates and distinct counts against the
extracted rows. Both run in the default suite — no credentials, no database.

### F14 — `config_settings.tx_max_footprint_entries` shipped all-NULL

Found by the readback test, immediately. The field was added to
`ConfigSettingData`, to `ParquetConfigSetting` and to
`v3_bronze_schema.sql`, and the one edit that populates it did not land.
The column therefore appeared in the Parquet schema and in the golden
diff, looking like a successful addition, while every row held NULL.

Fixed. Worth recording because of how it hid: a new column showing up in a
golden diff reads as the change working, and no other check in the
repository compares what a column holds against what was extracted.

A null-rate check alone would not have caught the general case either. A
dropped field only reads NULL if its column is nullable; a non-nullable
one takes Go's zero value. The test therefore also compares distinct
counts, and both halves were verified by reintroducing real drops.

### F15 — `claimable_balances` has no `created_at` column

Every other snapshot table in Parquet carries one. The value is an
ingestion wall clock, so adding the column would add a field **F7**
already identifies as unreproducible from the ledger, and removing it from
the siblings is a schema break. Allowlisted and left for the canonical row
contract to settle alongside F7.

## Root causes in `stellar-extract`

Four findings are not defects in either ingester. They are library-side, and
fixing them in one app would only move the divergence.

Checked against `github.com/withObsrvr/stellar-extract` at `v0.1.6`
(`5e8c77e`, clean tree).

### L1 — call-graph fields declared and never assigned

`OperationData.ContractCallsJSON`, `.ContractsInvolved` and `.MaxCallDepth`
exist in `types_core.go:150-152`. A module-wide search for an assignment to
any of them returns nothing. They always read nil.

This is the worst shape for an API gap: a consumer that copies them — which
is exactly what the live `convertOperation` does — gets a silent nil with no
way to distinguish "the library does not populate this" from "this ledger had
no cross-contract calls". Either populate them or remove them. Root cause of
**F3**.

### L2 — liquidity-pool trades are dropped

`trades.go:145` skips them outright:

```go
if claim.Type == xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
    continue
}
```

and `TradeType` is hardcoded `"orderbook"` in `tradeFromClaim`, so the schema
cannot express a pool trade even if one were emitted. Both sides of this
ticket therefore report zero pool trades on every fixture, and agree — which
is why the comparison could not have caught it on its own. The handoff's
investigation matrix lists "pool trades" as a trades check; this is the
answer to it.

Note the ordering trap before fixing this. `appendTradesFromClaims`
increments `tradeIndex` only for *emitted* trades, so adding pool trades
renumbers every existing index on any operation that mixes them with
orderbook claims. Anything keyed on `(transaction_hash, operation_index,
trade_index)` breaks silently. Decide whether the index is positional over
claims or dense over emitted rows **before** closing L2.

### L3 — `AccountData` has no `SequenceLedger` / `SequenceTime`

Root cause of **F5**. The protocol carries both in
`AccountEntryExtensionV3`, and the SDK exposes them as
`entry.SeqLedger()` / `entry.SeqTime()` — which is exactly how this
repository's own `extractors_accounts.go:88-89` gets them. `accounts.go` in
the library never calls either accessor, so the live ingester has nothing to
map and writes `0` into two columns that exist in its schema.

A small, well-defined addition: two fields and two accessor calls.

### L4 — SDK pin lags both consumers

`stellar-extract` requires `go-stellar-sdk v0.7.1`; both ingesters require
`v0.7.3`.

**This is not a production correctness gap.** Go's minimal version selection
takes the maximum, so both apps already build the library against `v0.7.3` —
confirmed with `go list -m` in each. What lags is the version the library's
*own* tests run against, including `protocol_coverage_test.go`, the guard
that exists specifically to catch unhandled union arms. That guard is
currently proving something about a version neither application uses.

The bump itself is low risk, and the library's own
[`docs/SDK_UPGRADES.md`](../../stellar-extract/docs/SDK_UPGRADES.md)
checklist is the reason to be confident rather than hopeful:

- **No new XDR union arms.** Diffing the generated enum constants between
  `v0.7.1` and `v0.7.3` returns nothing, so the failure mode that doc is
  built around — a new arm silently falling through an existing `switch` —
  does not apply to this bump.
- **Builds and tests clean** against `v0.7.3` (verified on a scratch copy).
- Two `processors/token_transfer` fixes land with it, both relevant to the
  `token_transfers` table: a `to_muxed_id` bound to `Void` previously failed
  to parse and the event was **dropped from the stream silently**, and an
  `ScvBytes` muxed id was padded or truncated to 32 bytes, reporting an id
  that was never emitted. Production already has these via MVS; the library's
  tests do not cover them.
- `v0.7.2` carries breaking `strkey` length validation and an
  `Asset.LessThan` ordering change affecting `xdr.NewPoolId`. Neither breaks
  the build, but both deserve a read before the bump lands — the pool-id
  ordering change is adjacent to L2.

The `go` directive also lags: `1.25.4` against `1.26.1` in both consumers.

### What is *not* a library problem

Worth stating, because the first reading of the evidence suggested otherwise:

- **F1 and F2 are already fixed upstream.** `v0.1.2` rendered trade amounts
  as raw stroops (`fmt.Sprintf("%d", ...)`); `v0.1.5` uses `amount.String()`,
  which is the Stellar convention. `v0.1.2` handled only the three offer
  operation types; `v0.1.5` added both path-payment types, which is the 62
  missing trades. `v0.1.5` also skips zero-amount claims, which is the 3
  spurious historical rows. The library is right and `stellar-ingest` is
  three versions behind.
- **F4 is not a source limitation.** The library populates all 36 classic
  operation fields; the live adapter discards them.
- **The `SorobanFeeWrite1kb` / `1KB` casing (F8)** is between the two apps'
  local ledger types. The library uses `SorobanFeeWrite1kb` and neither app
  calls its `ExtractLedgers`, so its spelling is only a tiebreaker if the two
  ever converge.

### F16 — `token_transfers` has no logical key

Found while deriving the canonical row contract. The row carries no event
index, so two transfers cannot be told apart by anything the schema holds.
A key of eight attributes — ledger, transaction, operation, event type,
from, to, asset, raw amount — still collides once across the 40,606
token-transfer rows in the fixture corpus, and legitimately: the same
contract may transfer the same amount between the same parties twice in one
operation.

The live hot buffer has no primary key on this table either, so nothing
deduplicates on the missing identity today and no rows are being lost. The
cost is that the table cannot be joined, replayed idempotently, or compared
row-for-row between the two stores.

The fix is upstream: `stellar-extract` would have to emit an index within
the operation, as it does for `effects` and `trades`. Recorded in
`internal/parity.KeylessTables` so a sink cannot start deduplicating on a
key that does not exist.

## Coverage gaps

Honest limits of this pass, so nothing above reads as broader than it is.

1. **Storage tier unverified for all 21 tables.** No `INSERT`, no Parquet
   write, no `ON CONFLICT` behaviour, no persisted-column check. F6 is direct
   evidence that extraction-tier results can be overturned there.
2. **`contract_code` has no fixture.** Zero rows on both sides everywhere,
   so its mapping is untested. `config_settings` is exercised by one fixture
   only.
3. **`both-empty` is not a pass.** A table with no rows has no columns to
   compare. Per-fixture `both-empty` counts are in the matrix; the status
   above reports a table as `equal` only where some fixture produced rows.
4. **Row pairing in samples is by sorted rank, not logical key.** The
   per-table logical keys are part of deliverable 3 and are not yet decided,
   so the harness does not guess. F1's key-based analysis was done by hand.
5. **One network.** All eight fixtures are pubnet.

## Follow-up tickets, ranked by data-loss risk

| Rank | Proposed ticket | Findings | Why this rank |
|---|---|---|---|
| 1 | Resolve the `trades` unit and coverage divergence | F1 | Silent 10⁷ numerical error plus missing rows; affects any cross-store join |
| 2 | Populate `accounts.sequence_ledger` / `sequence_time` on live | F5 | Zeros in real columns read as data, not absence |
| 3 | Decide the `stellar-extract` upgrade path | F1, F2, F5 | One root cause behind three findings; needs a reviewed bump, not a trial |
| 4 | Restore the live operation call graph, or delete the dead code | F3 | Working implementation unreachable from production; the dead code also misleads readers. Blocked on `stellar-extract` — the library declares the three fields and never assigns them |
| 5 | Verify the storage tier | F6, all | Until this runs, no table is more than extraction-equal |
| 6 | Define the canonical Bronze row contract | F4, F6, F7, F8 | Blocks Ticket 03's public API; `created_at`'s two meanings need deciding before anything is shared |
| 7 | Fixtures for `contract_code` and more `config_settings` | gap 2 | Two tables currently unverifiable |
| 8 | Fault-injection test for live warn-and-skip | F9 | Real replay gap, but no evidence it has fired |

Four of these are library work and belong in `stellar-extract`, not in either
ingester — fixing them in one app would only move the divergence:

| Rank | `stellar-extract` change | Finding | Why this rank |
|---|---|---|---|
| a | Populate or remove the call-graph fields | L1 / F3 | Declared and never assigned; nil is indistinguishable from "no calls" |
| b | Emit liquidity-pool trades | L2 | Silently absent from both stores, so the comparison agreed and could not catch it |
| c | Add `AccountData.SequenceLedger` / `.SequenceTime` | L3 / F5 | Two fields and two SDK accessor calls; unblocks rank 2 above |
| d | Bump the SDK pin to `v0.7.3` and the `go` directive | L4 | No production change (MVS already resolves it); aligns the library's own test matrix with what ships |

Rank (b) must settle the `trade_index` numbering question first — see L2.

## What this report does not authorize

No golden regeneration, no dependency bump, no live route change, no database
migration. The `stellar-extract` bump in particular would change `trades` and
`config_settings` output on the historical side and must be reviewed against
all 21 tables' goldens first — that is rank 3 above, not a side effect of
fixing rank 1.
