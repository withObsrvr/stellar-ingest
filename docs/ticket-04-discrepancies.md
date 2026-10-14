# Ticket 04 — historical/live Bronze discrepancy report

**Status: first complete pass. 21 of 21 tables have an extraction-tier
result; the storage tier is `unverified` for all of them.**

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
| Historical | `stellar-ingest` @ `1956dd3` | `v0.1.2` |
| Live | `ttp-processor-demo` @ `bd618bb`, `obsrvr-lake/stellar-postgres-ingester` | `v0.1.5` |

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
