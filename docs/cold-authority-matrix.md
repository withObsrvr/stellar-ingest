# Cold Bronze authority matrix

Which producer wrote each range of cold Bronze, and whether that producer was
sound for that table. Generated 2026-10-17 from the DuckLake catalog
(`obsrvr_lake_catalog_prod`, `bronze_meta`) and the Ticket 04 findings.

The question this answers is *not* "which of several copies wins." That was
the premise going in and it was wrong — see "Cold is not duplicated" below.
Each ledger range has exactly one producer, so the real question is whether
that producer was defective for that table, and therefore which ranges need
re-ingesting.

## Cold is not duplicated

`ledgers_row_v2` stores one row per ledger, which makes it a clean test:

| version_label | ledger envelope | rows |
|---|---|---|
| `c279879` | 3 – 59,100,002 | 12,100,000 |
| `fe5c426` | 14,000,003 – 43,500,002 | 16,500,000 |
| `c279879-callgraph-nilfix-20260525175306` | 294,227 – 62,799,999 | 3,949,998 |
| `071564e` | 544,227 – 62,799,999 | 30,605,800 |
| `live` | 62,802,354 – 64,412,832 | 1,609,485 |
| | **sum** | **64,765,283** |

Chain envelope is 64,412,830 ledgers, so the ratio is **1.005**. The labels'
min/max ranges overlap heavily, but those are envelopes, not coverage — each
backfill run worked a different slice. Cold holds approximately one copy of
history.

This matters because the overlapping envelopes read like 4–5× duplication if
you only look at row totals (`transactions_row_v2` sums to 10.56 billion rows,
which is ~164 tx/ledger across 64.4M ledgers — plausible, not duplicated).
There is no meaningful dedup win available. The size levers are column
encoding and payload, not deduplication.

## Producer provenance

| label | date | `stellar-extract` | evidence |
|---|---|---|---|
| `fe5c426` | 2026-04-27 | **v0.1.2** | `git show fe5c426:obsrvr-lake/stellar-history-loader/go/go.mod` |
| `c279879` | 2026-05-18 | **v0.1.2** | same |
| `c279879-callgraph-nilfix-20260525175306` | 2026-05-25 | **v0.1.2** (+ a nil fix) | label embeds base SHA `c279879` |
| `071564e` | unknown | **unknown** | commit resolves in no local repo after `git fetch --all` |
| `live` | continuous | v0.1.5 → v0.2.x → v0.3.x | the streaming ingester, bumped over time |

Two things to note.

**Every resolvable historical generation ran v0.1.2.** That is the pin from
before this month's library work. Every defect closed in v0.2.1, v0.3.0 and
v0.3.3 is therefore still present in the data those runs produced.

**`071564e` has lost provenance and is the largest generation** — 4.66 billion
transaction rows across 1,152 files, the widest ledger envelope of any label.
Its commit is not in `ttp-processor-demo`, `stellar-ingest`, `infra1`, or any
fetched remote. Its library version cannot be established from the repository,
so its correctness cannot be reasoned about from provenance — only measured
from the data. Treat it as v0.1.2-era by date bracketing (its envelope sits
between `c279879` and the `live` cutover) but do not rely on that.

## What "closed" means for data already written

The Ticket 04 findings table records defects as closed, which means closed in
current code. Rows written before the fix still carry the defect. Mapping each
finding to the producer that was wrong and the data that is still affected:

| finding | wrong side | fixed in | cold data still affected |
|---|---|---|---|
| F1 trades units (10⁷) + missing rows | historical | v0.2.1 | all historical `trades_row_v1` |
| F2 `config_settings` 13 NULL columns | historical | v0.2.1 | historical `config_settings_snapshot_v1` |
| F5 account sequence zeros | live | migration 009 era | `live` `accounts_snapshot_v1` before the fix |
| F10 ScVal rendering | historical (local converter) | v0.2.1 | ScVal fields in historical contract tables |
| F12 contract events discarded (86%) | live | migration 018, deployed 2026-10-16 | `live` `contract_events_stream_v1` below partition 6435 |
| F13 event identity | both | v0.3.0 | all event rows written before v0.3.0 |
| F14 Parquet all-NULL `tx_max_footprint_entries` | historical | fixed | historical `transactions_row_v2` |
| F15 `claimable_balances.ingested_at` never written | historical | open | all historical `claimable_balances_snapshot_v1` |
| F16 `token_transfers` has no `event_index` | both | v0.3.0 | all `token_transfers_stream_v1` before v0.3.0 |

**No cold data was produced by the current library.** The only rows in cold
written by v0.3.x are `live` rows after the 2026-10-16 ingester deploy —
partition 6436 onward. Everything else predates F1, F2, F10, F13, F14 and F16.

## The event-loss boundary, measured

F12 is the one defect with a sharp, measurable boundary in cold. Events per
ledger for `live`-labelled files:

| ledger bucket | events/ledger |
|---|---|
| 64,300,000 – 64,340,000 | 568 – 651 |
| 64,350,000 | 2,138 |
| 64,360,000 + | 4,052 – 4,636 |

~600 → ~4,300 is the 7.2× that matches the fixture-measured ~86% loss and the
production jump from 576.6 to 4,177.1 events/ledger. The fix landed inside
partition **6435**, so that partition is mixed and must be rebuilt regardless
of which side of the boundary a given ledger falls on.

## Per-table verdict

"Rebuild" means the range needs re-ingesting through the current loader at
`stellar-extract` v0.3.5. Ranges are stated as the producers' envelopes;
the unit of rebuild is the 10,000-ledger partition.

| table | authoritative today | rebuild | why |
|---|---|---|---|
| `trades_row_v1` | `live` only (62,800,000+) | **all historical** | F1: amounts off by 10⁷, real trades missing, spurious zero-amount rows. No historical range is usable at any precision. |
| `contract_events_stream_v1` | `live` partition 6436+ | **everything below partition 6436** | F12 for `live` below 6435; F13 identity for all historical. The largest rebuild in the matrix. |
| `token_transfers_stream_v1` | `live` partition 6436+ | **everything below** | F16 (no `event_index`) for all pre-v0.3.0 rows, plus the ~8% `live` shortfall from the scope report. |
| `config_settings_snapshot_v1` | `live` | historical `c279879` (40 rows) | F2. Trivially small — 40 rows. |
| `transactions_row_v2` | none fully | all historical | F14 (`tx_max_footprint_entries` all-NULL) and F6 memo policy. `live` is sound on those but predates the precondition/fee-bump column work. |
| `claimable_balances_snapshot_v1` | `live` | all historical | F15: `ingested_at` declared but never written. Open finding. |
| `accounts_snapshot_v1` | historical | `live` before the F5 fix | F5 is the one defect where **live** is the wrong side and historical is right. |
| `operations_row_v2` | neither fully | all | F4's 36 columns were one-sided; historical is v0.1.2. |
| `contract_data_snapshot_v1`, `contract_code_snapshot_v1`, `restored_keys_state_v1` | — | historical | F10 ScVal rendering differs from the library's. |
| `effects_row_v1`, `ledgers_row_v2`, `trustlines_snapshot_v1`, `native_balances_snapshot_v1`, `offers_snapshot_v1`, `liquidity_pools_snapshot_v1`, `ttl_snapshot_v1`, `evicted_keys_state_v1`, `account_signers_snapshot_v1`, `contract_creations_v1` | provisionally all | — | No finding names these as defective on either side. F7 (`created_at` split) and F8 (ledger provenance) are semantic rather than row-loss. **Unverified, not verified-good.** |

## What this says about sequencing

The honest summary is that the backfill is not a repair of a few bad
partitions. Only `live` partitions 6436+ were written by the current library,
which is roughly 1.6 million ledgers out of 64.4 million — about 2.5% of
history. The rest carries at least one closed-but-unrepaired finding.

That argues for treating the backfill as a **full re-ingest**, sequenced by
what Prism actually reads, rather than a partition-by-partition repair
campaign. Two consequences:

- The per-table authority decision mostly collapses to "rebuild". The
  exception worth remembering is `accounts_snapshot_v1`, where historical is
  right and `live` is wrong — the only table where the newest data is the
  worse copy.
- `071564e`'s lost provenance stops mattering if everything is rebuilt. It
  only matters if you try to keep some ranges, which is what this matrix was
  meant to decide.

## Still to answer

- **Is `071564e` v0.1.2?** Only measurable from the data now. A trades-units
  probe over the catalog's column statistics was attempted and is
  **inconclusive**: `selling_amount` is `varchar`, so the recorded min/max are
  lexicographic rather than numeric. The digit profile does group `071564e`
  with the historical generations and apart from `live`:

  | label | max `selling_amount` (lexicographic) |
  |---|---|
  | `071564e` | `999999999999999999` |
  | `c279879` | `99999999999999990` |
  | `fe5c426` | `9999999999999999` |
  | `live` | `99999999999999` |

  which is the direction raw stroops would point, but a string maximum is not
  a magnitude. Settling it means reading actual values out of one Parquet file
  per label through DuckDB against B2 and checking whether the amounts carry a
  decimal point. Cheap to do, not done here.
- **The ten unverified tables.** Absence of a finding is not evidence of
  correctness; Ticket 04 compared 21 tables at the extraction tier but the
  storage tier was only partly covered.
- **`live` F5 boundary.** The accounts-sequence fix date is not established
  here, so the `live` range to rebuild for `accounts_snapshot_v1` is unbounded
  below.
