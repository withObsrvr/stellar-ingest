# Bronze presentation views

Implements steps 3 and 4 of the sequencing in
[`bronze-storage-contract.md`](bronze-storage-contract.md): the storage-side
schema for rules 1–3, together with the views that render today's shapes over
it. They land together because a column removal without its view breaks cold
the moment the files are rewritten, and the re-ingest rewrites everything.

## The shape

Physical tables moved to a store schema; the views took the schema name
consumers already query.

```
bronze_store.contract_events_stream_v1   <- the loader and flusher write here
bronze.contract_events_stream_v1         <- every consumer reads here, unchanged
```

All 21 tables have a view. Nineteen are pass-throughs over a single table,
which the planner inlines; they exist so that reading is one rule rather than
a per-table exception, and so a writer flips one config value
(`--ducklake-view-schema` / `SchemaName`) instead of carrying a table list.

## What is no longer stored

| table | column | rule | recovered by | measured |
|---|---|---|---|---|
| `contract_events_stream_v1` | `event_id` | 2 | `lower(hex(parent.transaction_hash)) \|\| ':' \|\| event_index` | 800 GB |
| `contract_events_stream_v1` | `transaction_hash` | 3 | join parent on `transaction_id` | 700 GB |
| `operations_row_v2` | `transaction_hash` | 3 | join parent on `transaction_id` | 714 GB |
| `transactions_row_v2` | `transaction_hash` as text | 1 | `lower(hex(..))`, stored as 32 bytes | 649 GB → ~325 GB |

About **2.86 TB**, against the ~3.2 TB the contract projected for rules 1–3.
The gap is the deferred work below.

Both child views **LEFT** JOIN the parent on `transaction_id` **and**
`ledger_range`. Left, not inner, because the two sides do not advance
together: the flusher keeps a per-table checkpoint, so `contract_events` can
be flushed past the point `transactions_row_v2` has reached, and a child row
can genuinely have no parent for a while. Under an inner join those rows are
absent and nothing says so; under a left join they are present with a null
`transaction_hash`, which is wrong in a way somebody can see. This layer has
lost data quietly before.

The partition equality is part of the join condition.
The partition equality is not redundant, but it is also not the difference
between pruning and not: without it the parent side still prunes, because a
TOID sorts with its ledger and DuckLake keeps per-file statistics on
`transaction_id`, so the optimizer derives a dynamic range filter. Stating the
equality makes the pruning structural rather than incidental. Measured on a
50-partition fixture, with the left join in place: one partition reads 1 file
and 1,000 rows of 50,000, on both sides.

## What was verified, and how

The premises were measured rather than assumed, because the re-ingest rewrites
the files and nothing recovers a value that was never derivable.

- **`event_id` is exactly reconstructible** — all 104,730 events in the
  fixture corpus reproduce their stored `event_id` from `transaction_hash` and
  `event_index`. `event_id_reconstruction_test.go`.
- **`transaction_id` names the right transaction** — 104,730 events joined to
  16,223 transactions across 44 ledgers: no zero ids, no unjoinable events,
  none pointing at a neighbouring transaction.
  `contract_event_transaction_id_test.go`.
- **The byte encoding round-trips** — all 16,223 hashes survive text → 32
  bytes → `lower(hex(..))` unchanged. `identity_encoding_test.go`.
- **Every view reproduces the pre-change shape** — all 21 views asserted
  column-for-column, in order, against `testdata/compat/legacy_columns.json`,
  the order captured from the schema before the change. `compat_views_test.go`.
- **The removed columns come back byte-identical** through the views.

Each guard was mutated to confirm it fails: dropping the mapping assignment,
shifting the id by one, removing the partition equality, and swapping two
columns in a view all produce failures naming the specific defect.

## Required before this runs: the flusher

`v3_bronze_schema.sql` is **kept in sync with
`postgres-ducklake-flusher/v3_bronze_schema.sql`** — the comment on
`embeddedBronzeSchema` says why: whichever writer runs first defines the
schema, and the other then gets "table has N columns but M values supplied"
on insert.

So the flusher needs, before or with the re-ingest:

1. The same three column removals and the same `BLOB` type.
2. Its explicit column lists updated — the same change
   `token_transfers_stream_v1` already received when its hash was dropped.
3. To write to the store schema rather than the view schema.

Until that lands, the two writers disagree and inserts fail. This is the one
hard cross-repo dependency in the change.

## Fresh catalog only

Against an existing catalog, `CREATE OR REPLACE VIEW bronze.X` fails where
`bronze.X` is already a table. That failure is deliberate and loud —
`createViews` returns the error rather than logging it, unlike `createTables`
— because the alternative is consumers silently reading nothing. Decision 2
of the contract is a fresh catalog, which is the case this is built for.

## Deferred, with reasons

- **Account strkeys** (~810 GB stored, ~400 GB of projected recovery).
  Rendering a strkey needs base32 plus a CRC16-XModem checksum. DuckDB 1.5.5
  has neither — no base32, no CRC — so this column cannot be made
  consumer-transparent by a view at all. A Go UDF does not substitute:
  DuckLake persists views to other processes, but not function
  registrations. This needs either a DuckDB extension or consumer migration,
  and so is no longer a pure storage change.
- **`effects_row_v1` and `trades_row_v1` `transaction_hash`.** Neither table
  carries `transaction_id`, so rule 3 cannot apply until it is added
  upstream. The value is small anyway: `effects_row_v1.transaction_hash` is
  2.9 bytes/row, because many effects share one transaction and dictionary
  encoding already absorbs the repetition. Rule 1's win concentrates where
  identity is unique per row — which is why the parent table at 66.0
  bytes/row was worth converting and these are not.
- **`balance_id`** (~145 GB). Hex, so renderable in principle, but whether
  every stored value is hex rather than a `B...` strkey is unverified. Not
  worth guessing at on the only copy of the data.
- **`inner_transaction_hash`.** A digest, and in scope for rule 1, but
  optional and set only on fee-bump transactions, so the volume is small.
- **Compression codec and row-group sizing**, still untouched and unmeasured
  on the 8,382 GB that is not identity — the largest remaining unexamined
  lever.
