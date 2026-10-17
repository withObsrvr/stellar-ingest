# Bronze storage contract — draft

**Status: draft for decision.** Companion to
[`bronze-row-contract.md`](bronze-row-contract.md), which is ratified and is
not reopened here.

The row contract says what a Bronze row *means* — its logical key, null
semantics, units, provenance, ordering. This says how a row is *stored*. They
are different axes and were conflated, which is part of how the storage layer
drifted without anyone deciding anything.

## Why this document exists

Bronze is the bottom layer. Its job is to hold every Stellar ledger once,
correctly, compactly, and to be rebuildable from the archive. Everything above
it — silver, serving, Prism — is derived and can be rebuilt from it.

Measured 2026-10-17 against the production catalog:

| | size | share |
|---|---|---|
| cold Bronze, total | **12,534 GB** | |
| identity stored as text | **4,152 GB** | **33.1%** |
| everything else | 8,382 GB | 66.9% |

A third of the bottom layer is identifiers rendered as hex and base32 strings.
None of that is required by the layer's job; it is there because the first
consumer wanted strings and the layer below accommodated. That is an inverted
dependency, and it is the single largest cost in the lake.

## The principle

> Bronze stores the most compact faithful representation. Presentation is a
> view, not a storage decision.

A consumer that wants `transaction_hash` as 64 hex characters gets it from a
view that renders `lower(hex(...))`. The stored bytes are 32. This is the same
device migration 024 used for the `created_at` split in hot PostgreSQL: change
the physical column, keep the old name readable, retire the view per consumer
later.

That matters for sequencing. There are **65 call sites across 21 files in six
services** that join on or look up `transaction_hash`. Requiring them all to
move before Bronze can shrink is what has kept Bronze from shrinking. Under
this principle they do not have to move at all until someone wants the
performance.

## 1. Identity is stored in its natural width

**Rule: an identifier is stored as the bytes or integer it is, never as its
text rendering.**

| identifier | stored today | natural form | why text costs |
|---|---|---|---|
| `transaction_hash` | 64-char hex | 32 bytes | hex is 2×, and a digest is random so it does not compress |
| `account_id`, `source_account` | 56-char strkey | 32 bytes + type | base32 of a key plus version and checksum |
| `balance_id` | hex | 32 bytes | as above |
| `transaction_id`, `operation_id` | `int64` already | — | already correct, and the evidence for this rule |

The last row is the argument. In `transactions_row_v2`, measured per row:

```
transaction_hash   66.0 bytes      64-char hex of a 32-byte digest
transaction_id      4.0 bytes      int64 TOID — the same identity
```

Both name the same transaction. One costs 16× the other. `transaction_hash`
achieves essentially **no compression** — 66 bytes for a 64-character string —
because a hash is random and a block compressor has nothing to exploit. This
is the opposite of the intuition that big columns are the problem.

Cost of the rule, across cold:

| column | size | bytes/row |
|---|---|---|
| `contract_events_stream_v1.event_id` | 800 GB | 24.1 |
| `token_transfers_stream_v1.transaction_hash` | 775 GB | 34.7 — *dropped 2026-10-17* |
| `operations_row_v2.transaction_hash` | 714 GB | 31.4 |
| `contract_events_stream_v1.transaction_hash` | 700 GB | 21.1 |
| `transactions_row_v2.transaction_hash` | 649 GB | 66.0 |
| account strkeys across five tables | ~810 GB | 3.4–15.7 |
| `balance_id` across two tables | ~145 GB | 3.1–66.0 |

Where a value repeats, compression already helps: `effects_row_v1.transaction_hash`
is 2.9 bytes/row because many effects share one transaction. The cost tracks
repetition, not declared width, which is why a column-by-column measurement
was necessary and a rule of thumb would have been wrong.

## 2. Derived values are not stored

**Rule: if a column is a pure function of other columns in the same row, it is
a view expression, not a column.**

`contract_events_stream_v1.event_id` is the case that prompted this:

```
a2069f36e0e430c44cf60d416ca2a8679155a28f695f98d959772dc103294f52:118
└─────────────── transaction_hash ───────────────────────────────┘ └ event_index
```

It is `transaction_hash || ':' || event_index`, and both operands are columns
in the same row. It costs **800 GB** — the largest single column in the lake —
to store a string the row can already produce.

With rule 1 applied as well, that table stores one transaction identity three
times: as `transaction_hash`, inside `event_id`, and implicitly via
`transaction_id` once added. Roughly **1.5 TB for one fact.**

## 3. A child row references its parent by the parent's compact key

**Rule: a table that references a transaction carries `transaction_id`, not
`transaction_hash`.**

The hash is recoverable by joining `transactions_row_v2`. Already applied to
`token_transfers_stream_v1` (775 GB, no reader). `operations_row_v2` (714 GB)
and `contract_events_stream_v1` (700 GB) remain; the latter has no
`transaction_id` column yet and needs one first.

This rule interacts with the presentation principle: a view on the child table
can join the parent and expose `transaction_hash`, so consumers see no change.
That join is not free, and whether to pay it per-read or keep the column is a
per-table judgement — stated here so it is a judgement rather than an
accident.

## 4. One producer per partition

**Rule: a partition is written by exactly one producer, and records which.**

Cold currently holds five generations — `fe5c426`, `c279879`,
`c279879-callgraph-nilfix-20260525175306`, `071564e`, `live` — with
overlapping ledger envelopes.

They are *not* duplicated: `ledgers_row_v2` has exactly one row per ledger, and
the five labels sum to 64,765,283 rows against a 64,412,830-ledger chain, a
ratio of **1.005**. Each run covered a disjoint slice. The envelopes overlap;
the coverage does not.

The problem is provenance, not volume. Every resolvable generation pins
`stellar-extract v0.1.2` — the pin from before this month's library work — so
every defect closed in v0.2.1, v0.3.0 and v0.3.3 is still present in the data
those runs wrote. `071564e`, the largest generation at 4.66 billion
transaction rows, resolves to no commit in any repository, so its library
version cannot be established at all.

Only `live` partitions 6436 and above were written by the current library:
roughly **2.5% of history**. See
[`cold-authority-matrix.md`](cold-authority-matrix.md).

## 5. Payload — open, and it touches the row contract

Row contract §6 ratifies that raw XDR is stored as **unmodified base64**, for
reproducibility and as the fallback when a decoded column is wrong. That is a
fidelity rule and this document does not overturn it.

It has a storage consequence worth putting in front of whoever owns §6:
base64 is 1.33× the bytes it encodes. Unlike hex identifiers, XDR compresses
well — it is structured and repetitive — so the real overhead is smaller than
the ratio suggests. Measured in `transactions_row_v2`:

```
tx_meta       339 GB   34.5 bytes/row
tx_envelope   141 GB   14.3
tx_fee_meta    34 GB    3.5
tx_signers     33 GB    3.3
tx_result      29 GB    3.0
              ──────
              576 GB   58.6 bytes/row combined
```

**All five payload columns together cost less per row than
`transaction_hash` alone (66.0).** This is the finding that reordered the
work: the payload is not the problem, and Item 2 of the throughput
recommendations — "stop storing transaction XDR per row" — is third in line
behind identity, not first.

Two questions for §6, neither decided here:

1. Does "unmodified base64" mean the *encoding*, or the *bytes*? Storing the
   raw XDR and rendering base64 in a view satisfies reproducibility
   identically, at 0.75× the size.
2. Should Bronze store `LedgerCloseMeta` once per ledger and derive the
   per-transaction columns, as the archive node does? Larger change, and it
   makes the five columns a derivation under rule 2.

## 6. Partitioning stays as it is

10,000-ledger `ledger_range` buckets, which the Stellar archive node arrived
at independently for its chunks. Nothing in the measurements argues against
it, and it is the unit `backfill-scope`, the range catalog and the flusher all
already speak.

## What this is worth

Rules 1–3 applied across cold, against the 4,152 GB of identity-as-text:

| change | recovers |
|---|---|
| drop `event_id` (rule 2) | ~800 GB |
| child tables carry `transaction_id` only (rule 3) | ~1.4 TB remaining |
| remaining hashes stored as bytes (rule 1) | ~650 GB |
| account strkeys stored as bytes (rule 1) | ~400 GB |
| **total** | **~3.2 TB, about a quarter of the lake** |

Payload work under §5 would add up to ~145 GB on top, which is the point about
ordering.

These are recovered **only when the files are rewritten**. A `DROP COLUMN` in
DuckLake is a metadata operation; the bytes leave when the Parquet does. The
re-ingest rewrites everything, which is why these decisions are free then and
expensive at any other time.

## Sequencing

1. **Decide this document.** Rules 1–3 are cheap to agree and large; §5 needs
   whoever owns the row contract.
2. **Write the schema into `stellar-ingest`** — it is already the single
   canonical loader, validated end to end, and resumable.
3. **Add presentation views** rendering today's shapes over the new storage,
   so none of the 65 consumer sites has to move.
4. **Re-ingest**, which applies the schema and fixes the provenance problem in
   rule 4 at the same time.
5. **Retire the views per consumer**, later, on whatever schedule suits.

Steps 3 and 5 are what make this a storage change rather than a six-service
migration.

## What is not decided here

- Whether `contract_events_stream_v1` gains `transaction_id` or keys on
  something else. It has no compact parent reference today.
- The §5 payload questions.
- Whether `version_label` survives rule 1 — it is text, it is not an
  identifier, and it is small.
- Compression codec and row-group sizing. Untouched, unmeasured, and
  plausibly worth something on the 8,382 GB that is not identity.
