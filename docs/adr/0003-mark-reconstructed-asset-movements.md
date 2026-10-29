# 3. Mark reconstructed asset movements, and name CAP-67 in the schema

- Status: accepted
- Date: 2026-10-29
- Owner: Tillman Mosley III
- Affected interfaces: `bronze.token_transfers_stream_v1`,
  `bronze.contract_events_stream_v1` (presentation views)
- Rules: **STL-1**, **STL-3**

## Context

CAP-67 made events the source of truth for asset movements from protocol 23.
Before that the protocol emitted no such events, and the SDK's token-transfer
processor reconstructs them from operations, operation results and
ledger-entry changes.

`token_transfers` carries both kinds and nothing distinguished them. Measured
on the committed fixtures:

| fixture | protocol | rows |
|---|---:|---:|
| `pubnet-classic-p19` (ledger 50,000,000) | 19 | 7,822 **reconstructed** |
| `pubnet-soroban-p29` (ledger 64,938,240) | 29 | 11,736 **observed** |

A consumer querying a range that spans protocol 23 gets a mix, with no way
to tell which is which. STL-3 requires the reconstructed ones be marked,
"along with how they were derived, so users can tell observed events from
reconstructed ones".

Separately, STL-1 requires a dataset that implements a CAP or SEP to name it
in its schema metadata. Nothing did.

## Decision

**Compute `event_provenance` in the view, do not store it.**

| value | meaning |
|---|---|
| `cap67_observed` | protocol ≥ 23; events read from ledger metadata |
| `derived_from_operations` | protocol < 23; reconstructed by `go-stellar-sdk` `token_transfer` from operations, results and ledger-entry changes |
| `unknown` | the ledger row has not arrived in this partition |

Three consequences worth stating, because each was a choice:

1. **Computed, not stored.** Storage contract rule 2: a derived value is not
   stored. This is a pure function of the ledger's protocol version, so
   storing it would add a column to every row of the largest stream in the
   lake to repeat what `ledgers_row_v2` already knows. The view joins and
   derives it, exactly as `contract_events` already rebuilds `event_id` and
   `transaction_hash`.

2. **The granularity is the ledger, not the event.** The SDK normalises older
   metadata into the current shape and does not report, per event, whether it
   read or reconstructed one — the protocol version is the real determinant
   either way, and claiming finer granularity than the chain provides would
   be worse than none.

3. **An unknown protocol is `unknown`, not `observed`.** Partitions land
   independently. Defaulting a missing ledger row to "observed" would
   overstate what the data shows, and the whole point of this column is not
   overstating what the data shows.

`contract_events` gets the CAP-67 name but **no provenance column**: Soroban
events exist only from protocol 20 and are always read from transaction
metadata, never reconstructed. There is no split to expose.

The spec is named with `COMMENT ON VIEW`, so it travels with the lake and a
consumer inspecting the schema finds it, rather than it living only in prose.

## Classification (VER-3)

**Additive.** A new column on a view; consumers must ignore columns they do
not know. No stored schema changes, no backfill, no migration. Existing
queries and joins are unaffected — the legacy columns keep their order and
names, which `TestEveryViewReproducesTheLegacyColumnOrder` enforces.

## Consequences

Applying the views needed one production fix: `splitSQLStatements` was not
string-literal aware, so a semicolon inside a `COMMENT ON ... IS '...'` cut
the statement mid-literal and the parser saw an unterminated string. It
already handled semicolons in comments for the same reason; literals are the
other place prose appears in a schema file.

## Rollback

Revert. The views are recreated with `CREATE OR REPLACE` on every push, and
nothing stored changed.

## Tests

- `TestTokenTransfersMarkObservedAndDerivedApart` — a ledger either side of
  the boundary, asserted to be marked differently.
- `TestUnknownProtocolIsNotClaimedAsObserved`.
- `TestViewsNameTheSpecTheyImplement` — reads the comment back out of
  `duckdb_views()`.
- `TestSemicolonsInsideStringLiteralsDoNotSplit` and
  `TestDoubledQuotesKeepTheSplitterInSync`.
