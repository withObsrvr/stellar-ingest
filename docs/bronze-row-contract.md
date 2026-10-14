# Canonical Bronze row contract — candidate v1

**Status: candidate v1. All three product decisions ratified; two upstream
library changes open.** Ticket 04 deliverable 3.

This is what a Bronze row *is*, independent of which ingester produced it or
which store it landed in. It exists because two implementations wrote the
same 21 tables for months without one, and the differences that opened up —
amounts off by 10⁷, a column of zeros where data should be, 86% of a table
discarded on insert — were all cases where neither side was wrong against a
written rule, because there was no rule.

Every claim here is either verified against the committed fixture corpus or
marked as an open decision with an owner. Where a rule is machine-checked,
the test is named.

Companion documents: [`ticket-04-discrepancies.md`](ticket-04-discrepancies.md)
for the findings this is built on, [`ticket-04-progress.md`](ticket-04-progress.md)
for the harness.

## Scope

Applies to the 21 Bronze tables listed in `internal/parity.AllTables`, as
produced by `stellar-ingest` (Parquet) and `stellar-postgres-ingester`
(PostgreSQL hot buffer), both on `stellar-extract` v0.2.1.

It deliberately does **not** say that `stellar-extract`'s Go structs are the
contract. Three mapping layers sit between the library and a file, and two of
them are hand-written; the stored row is what a consumer reads, not what the
library returned.

## 1. Row identity

Every table has a **logical key**: the smallest set of columns identifying
one row within a ledger range. Without one a consumer cannot join,
deduplicate, or replay idempotently, and two stores can only be compared by
sorted rank, which is a guess.

The keys live in `internal/parity/keys.go`, not in prose, so they can be
checked. `TestLogicalKeysAreUnique` extracts all eight fixtures — about
300,000 rows across protocols 1 to 29 — and fails if a key does not identify
exactly one row.

### Event-stream tables — identity is a position in the ledger

| Table | Key | Verified |
|---|---|---|
| `ledgers` | `Sequence` | 49 rows |
| `transactions` | `LedgerSequence, TransactionHash` | 16,223 |
| `operations` | `+ OperationIndex` | 32,296 |
| `effects` | `+ OperationIndex, EffectIndex` | 38,187 |
| `trades` | `+ OperationIndex, TradeIndex` | 5,587 |
| `contract_events` | `+ OperationIndex, EventIndex` | **not unique — see below** |

### Snapshot tables — identity is the entry, scoped to a ledger

`LedgerSequence` is part of every snapshot key: Bronze keeps the history of
an entry, not only its latest state.

| Table | Key | Verified |
|---|---|---|
| `accounts`, `native_balances` | `AccountID, LedgerSequence` | 17,956 / 17,957 |
| `account_signers` | `AccountID, Signer, LedgerSequence` | 1,556 |
| `trustlines` | `AccountID, AssetCode, AssetIssuer, AssetType, LedgerSequence` | 17,824 |
| `offers` | `OfferID, LedgerSequence` | 8,662 |
| `liquidity_pools` | `LiquidityPoolID, LedgerSequence` | 1,783 |
| `claimable_balances` | `BalanceID, LedgerSequence` | 605 |
| `config_settings` | `ConfigSettingID, LedgerSequence` | 14 |
| `contract_data` | `ContractId, LedgerKeyHash, LedgerSequence` | 5,286 |
| `contract_code` | `ContractCodeHash, LedgerSequence` | 2 |
| `ttl`, `evicted_keys`, `restored_keys` | `KeyHash, LedgerSequence` | 5,981 / 12,000 / 24 |
| `contract_creations` | `ContractID` | 1 |

`contract_creations` is keyed on the contract alone: a contract is created
once, and the creating ledger is an attribute, not identity.

### Two tables without a working key

Recorded as defects, not omissions.

**`contract_events` — key in use, 4.0% collisions.** The contract and
diagnostic event streams index separately, so a contract event and a
diagnostic event from different contracts can share an operation and event
index. 4,165 of 104,730 rows collide. Those rows also share an `event_id`,
so the library cannot distinguish them either — this key is exactly the
identity `event_id` claims, no more. Tracked as **F13**; the fix is upstream,
either an index unique across streams within an operation or an `event_id`
derived from something that separates them.

**`token_transfers` — no key at all.** The row carries no event index.
Eight attributes — ledger, transaction, operation, event type, from, to,
asset, raw amount — still collide once in the corpus, and legitimately: the
same contract may transfer the same amount between the same parties twice in
one operation. The live hot buffer has no primary key on this table either.
Tracked as **F16**; needs an index field from the library.

### Rule

> A sink must not deduplicate on a key that is not in `LogicalKeys`, and must
> not deduplicate at all on a table in `KeylessTables`.

This rule is written because breaking it cost 86% of `contract_events`: the
hot buffer's primary key omitted `OperationIndex`, and the `ON CONFLICT`
built on it silently discarded 12,480 of 14,515 rows on three ledgers
(**F12**).

## 2. Null semantics

Three states must stay distinguishable, and two of them have been conflated
in production:

| State | Meaning | Encoding |
|---|---|---|
| NULL | the ledger did not provide a value | SQL NULL, Parquet null |
| Zero / empty | the ledger provided zero or an empty string | `0`, `''` |
| Absent column | this store does not carry the field | no column |

**Rule: a field the producer cannot populate is NULL, never a zero value.**

`accounts.sequence_ledger` and `.sequence_time` broke this: the columns
existed, the insert wrote them, and `convertAccount` never set them, so every
account row carried a literal `0` (**F5**). A zero reads as data. A NULL reads
as absence. The difference is not cosmetic — it is the difference between "this
account was never bumped" and "we did not look".

**Rule: a column present in a schema must be populated by some code path.**

`config_settings.tx_max_footprint_entries` shipped in the Parquet schema and
in the golden snapshots with every row NULL, because the conversion was never
wired (**F14**). A new column appearing in a diff reads as the change working.
Enforced by `TestParquetLayerWritesEveryRowField` and
`TestParquetStorageTierPreservesValues`.

**Rule: an empty list is not NULL.** `nil` means no authorization entries;
`[]` would mean an authorization with no entries. The canonical encoding in
`internal/parity/canonical.go` keeps these distinct and
`TestEncodeKeepsDistinctionsThatMatter` pins it.

## 3. Units and numeric fidelity

**Rule: amounts are decimal strings in whole units, not stroops.**

`trades` carried raw stroops on the cold side and XLM-scaled decimals on the
hot side — the same trade reading `70627` and `0.0070627` (**F1**). Resolved
in favour of decimals, matching Horizon and `amount.String`. A consumer
joining the two stores was previously off by a factor of ten million with
nothing in either schema to say which was which.

**Rule: amounts are strings, never floats.** `int64` stroop values exceed
`float64`'s exact range above 2⁵³. `token_transfers.Amount` is a `float64`
convenience column and is **not** authoritative; `AmountRaw` is. Any consumer
doing arithmetic reads `AmountRaw`.

**Rule: identifiers are rendered, not inferred.** `contract_creations`
records `executable_type` explicitly because deducing "SAC" from a NULL
`wasm_hash` broke silently when Protocol 28 added a third kind. The general
form: when a discriminant exists in the protocol, store it rather than
reconstructing it from the absence of something else.

## 4. `created_at` — the one name with two meanings

This is the contract's largest unresolved item, and it is not a difference
between the two ingesters: both do it.

Measured by extracting each fixture twice and comparing
(`internal/parity`'s non-determinism probe):

| Meaning | Tables |
|---|---|
| **Ledger close time** — reproducible from the ledger | `transactions`, `operations`, `trades`, `effects` |
| **Ingestion wall clock** — not reproducible | `accounts`, `account_signers`, `trustlines`, `offers`, `liquidity_pools`, `ttl`, `contract_data`, `contract_events`, `token_transfers` |

`accounts` also stamps `updated_at`; `ledgers` stamps `ingestion_timestamp`.

So nine tables contain a column that cannot be reproduced from the ledger,
under a name that on four other tables means event time. A consumer treating
`created_at` uniformly as event time is wrong on nine tables.

**Rule — ratified and implemented.** `closed_at` is the ledger close time and
is the only event-time column. `ingested_at` is the wall clock and is
explicitly provenance, not data. `created_at` is retired.

| Column | Meaning | Tables |
|---|---|---|
| `closed_at` | ledger close time, reproducible | 19 |
| `ingested_at` | wall clock at extraction, provenance | 15 |
| `created_at` | — | none |

Applied as a column rename in both stores: Parquet tags and
`v3_bronze_schema.sql` on the cold side, migration 019 on the hot side.
Verified with zero `created_at` columns remaining in a database built from
the full migration chain.

The rename had a side effect worth recording. The golden suite excluded
`created_at` globally as non-deterministic, which was right for the wall
clocks and wrong for the four stream tables where it was the close time — so
a deterministic event-time column on `transactions`, `operations`, `trades`
and `effects` was never being compared. With the split, `closed_at` is
compared and only `ingested_at` is excluded. That is the whole diff the
rename produced in the goldens.

Two redundancies this exposes, both left alone deliberately:
`accounts.updated_at` holds the same wall clock as `ingested_at` on every
row, and `trades.closed_at` now duplicates `trade_timestamp`. Dropping
either is a consumer-visible break and neither is wrong.

One gap it makes visible rather than introduces: `trustlines` had only the
wall clock, so after the rename it carries `ingested_at` and no event time
at all. Giving it `closed_at` needs the extractor to carry one.

This also settles **F15** the other way than expected. `claimable_balances`
declares `created_at` in `v3_bronze_schema.sql` and the Parquet writer never
produces it — the column was declared on the cold side and unwritten, which
is the same defect class as F14. Under the ratified rule it becomes
`ingested_at`, still unwritten, and the honest fix is to drop the column
declaration rather than start stamping a wall clock. Recorded as open.

## 5. Provenance

Provenance describes the run, not the ledger. It must be separable, because
a row's content should be reproducible from its input and these fields are
not.

| Column | Meaning |
|---|---|
| `era_id` | operator-supplied ingestion era |
| `version_label` | build version of the producer |
| `ingestion_timestamp`, `updated_at` | wall clocks |
| `observation_source` | `ledger_close_meta` or an RPC observation |
| `resolved_at_ledger` | ledger an RPC observation was aligned to |

**Rule: a row derived from authoritative XDR and a row observed from RPC must
be distinguishable.** `config_settings` is the only table where this arises
today: the live ingester may append rows observed from RPC that are absent
from the ledger. `observation_source` carries it.

**Rule: provenance columns are excluded from content comparison.** Enforced by
`DeclaredRunDependentColumns`.

## 6. Raw XDR fidelity

**Rule: where a row carries raw XDR, it is the unmodified base64 of the
protocol structure.** `tx_envelope`, `tx_result`, `tx_meta`, `tx_fee_meta`,
`contract_event_xdr`, `config_setting_xdr` and the rest are reproducible from
the ledger and are the fallback when a decoded column is wrong.

### Memo — ratified and implemented

**Rule: keep the raw bytes.**

| Column | Content |
|---|---|
| `memo_type` | always the protocol value: `none`, `text`, `id`, `hash`, `return` |
| `memo` | the readable rendering, **NULL** when the bytes are not valid text |
| `memo_raw` | base64 of the raw bytes, text memos only, authoritative |

`MemoText` is opaque bytes and need not be UTF-8, which a Parquet string
column rejects. The two stores had each worked around that differently and
neither kept the original: the cold store base64-encoded *every* text memo
and reported `memo_type` as `text_base64`, so plain ASCII was encoded too;
the hot store kept the text and sanitized invalid sequences on insert, which
is lossy. A consumer reading `memo` got different content depending on which
store it asked.

`memo` is now NULL rather than mangled when the bytes are not text, which is
§2's null rule: a value the producer cannot represent is absent, not
approximated. `memo_raw` always carries the truth. `id`, `hash` and `return`
memos are already lossless as decimal and hex, so `memo_raw` is NULL for
them. `text_base64` is retired as a `memo_type` value.

Implemented on the cold side in the extractor, the Parquet layer and
`v3_bronze_schema.sql`; on the hot side in the transaction-XDR overlay and
migration 020. Existing rows keep `memo_raw` NULL until their range is
re-ingested.

Two things this removed: `sanitizeTransactionMemo` was dead code — it only
fired on `memo_type == "text"`, which the cold extractor never produced
because it set `text_base64` directly — and with memo agreed, `transactions`
now differs between the stores on `envelope_size_bytes` alone, which is the
insert-time recomputation rather than an extraction difference.

## 7. Column-set scope — ratified: Bronze is one shape everywhere

**Rule: a store carries every Bronze column. A store may not declare a
subset.**

`stellar-extract` populates 36 classic operation detail fields — the asset
triples, the offer and price fields, the account thresholds and flags, the
data entry name and value — and the hot buffer's `OperationData` was declared
"simplified, core fields only" and had columns for none of them. The data
arrived and was dropped. The cold store had carried all 36 since it existed
(**F4**).

A subset was defensible as a performance choice, but it was implicit: a
consumer found columns missing with nothing to say whether that was a
decision or a defect, and a Silver transformation written against one store
broke when pointed at the other. Making the subset *explicit* would have
meant a declaration format, a way for consumers to read it, and a rule for
what happens when they need a column a store does not have — more machinery
than simply carrying the column.

Closed by migration 021 on the hot side, with column names matching
`v3_bronze_schema.sql` exactly, so `operations` is now joinable
column-for-column between the two stores. Enforced going forward by
`TestConvertFunctionsReadEveryLibraryField`, whose allowlist no longer has
anything to say about operations.

Two column-set differences remain, and the same rule decides both — they are
work, not open questions:

- **`transactions`.** The cold store carries `source_account_muxed`,
  `timebounds_min_time`, `timebounds_max_time`, `soroban_host_function_type`
  and `soroban_contract_id`; the hot store carries `contract_error_type`,
  `contract_error_code`, `soroban_footprint_read_entries` and
  `soroban_footprint_write_entries`. Under this rule both stores take all
  nine.
- **`ledgers`.** The hot store's migration-014 entry-change aggregates are
  the same case. `soroban_fee_write1kb` versus `soroban_fee_write1KB` is just
  a casing mismatch, and the library spells it `1kb`.

A narrower related case (**F11**): `operations.soroban_contract_id` is
populated by the cold store on an `InvokeHostFunction` that *creates* a
contract, and NULL in the library. Arguably the created contract; arguably
nothing, since `contract_creations` records it. One row in 2,913, and the
last value difference in `operations`.

A narrower related case (**F11**): `operations.soroban_contract_id` is
populated by the cold store on an `InvokeHostFunction` that *creates* a
contract, and NULL in the library. Arguably the created contract; arguably
nothing, since `contract_creations` records it. One row in 2,913.

## 8. Protocol eras and missing data

**Rule: a field a protocol version does not have is NULL, not zero or a
default.** Early-protocol ledgers have no Soroban fields; a protocol-23
ledger has no CAP-0085 external reference.

**Rule: a new protocol arm must not fall through silently.** Every extractor
switches on XDR union discriminants, and Go does not warn about an unhandled
arm. `stellar-extract`'s `protocol_coverage_test.go` guards the discriminants
it switches on, and `docs/SDK_UPGRADES.md` carries the upgrade procedure. An
unknown arm is recorded, not dropped — `extractAuthCredentials` emits
`UNKNOWN_<n>` rather than skipping the entry.

## 9. Ordering

**Rule: row order is not part of the contract. Element order within a row
is.**

Rows are compared as multisets: the two ingesters extract in different orders
by construction, one fanning out to 21 goroutines. But ordered arrays inside
a row — call-graph hops, auth credential pairs, claimants — carry meaning,
and reordering them is a real difference.

**Rule: output must nonetheless be byte-reproducible.** Thirteen extractors
ranged over a dedup map to build their output slice, so the same ledger
produced the same rows in a different sequence every run. No test saw it —
golden suites sort before hashing, and the parity harness compares multisets
— but it defeated content addressing and any byte-level reproducibility claim
about a backfill. Rows are now ordered by the dedup key, guarded by
`TestNoExtractorRangesOverItsDedupMap` in the library.

## 10. Versioning

This document is **candidate v1**. The canonical encoding it relies on is
versioned separately as `parity.CanonicalFormVersion`, stamped into every
comparison artifact so a change to the encoding cannot be misread as a change
to the data.

**Rule: a logical key change is a contract version bump.** Keys are the
identity downstream consumers build on, and the one time a key changed by
accident it cost most of a table.

## Open decisions

| # | Decision | Finding | Owner |
|---|---|---|---|
| ~~1~~ | ~~`created_at` split~~ — **ratified and implemented**; see §4 | F7 | — |
| ~~2~~ | ~~Memo encoding~~ — **ratified and implemented**; see §6 | F6 | — |
| ~~3~~ | ~~Declared subset of Bronze~~ — **ratified**: one shape everywhere; see §7 | F4 | — |
| 4 | `contract_events` identity — unique index or richer `event_id` | F13 | `stellar-extract` |
| 5 | `token_transfers` event index | F16 | `stellar-extract` |

All three product decisions are ratified and implemented. What remains is
4 and 5, both upstream in `stellar-extract` and both with a clear shape, plus
the `transactions` and `ledgers` column sets that decision 3 now settles as
work rather than questions.

## What is already enforced

Not aspirations — these fail a build today.

| Rule | Test | Repository |
|---|---|---|
| Logical keys identify one row | `TestLogicalKeysAreUnique` | stellar-ingest |
| Every table has a key or a recorded reason | `TestEveryTableHasAKeyDecision` | stellar-ingest |
| Null vs empty vs absent stay distinct | `TestEncodeKeepsDistinctionsThatMatter` | stellar-ingest |
| Every library field reaches the adapter | `TestConvertFunctionsReadEveryLibraryField` | live ingester |
| Every row field reaches Parquet | `TestParquetLayerWritesEveryRowField` | stellar-ingest |
| Stored values match extracted values | `TestParquetStorageTierPreservesValues` | stellar-ingest |
| Stored rows match extracted rows | `TestStorageTierRoundTrip` | live ingester |
| No field is declared and never assigned | `TestEveryRowFieldIsPopulatedSomewhere` | stellar-extract |
| Extraction is reproducible | `TestNoExtractorRangesOverItsDedupMap` | stellar-extract |
| The two ingesters agree | `make parity-report` | stellar-ingest |
