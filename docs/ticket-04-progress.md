# Ticket 04 — progress: historical/live Bronze parity harness

Companion to [`ticket-04-developer-handoff.md`](ticket-04-developer-handoff.md)
(the brief) and [`ticket-04-discrepancies.md`](ticket-04-discrepancies.md)
(the findings). This file covers the harness: how it works, how to run it,
and what is left.

**State:** all four deliverables done. The candidate canonical row contract
is [`bronze-row-contract.md`](bronze-row-contract.md); its logical keys are
machine-verified rather than asserted, and five decisions in it remain open
for review.

## Why two processes

Both sides now pin `stellar-extract@v0.2.1`, but the harness is built for
them not to: they pinned `v0.1.2` and `v0.1.5` when it was written, and two
versions of one module cannot be linked into a single binary. Each side
canonicalizes in its own process and the comparison runs over the artifacts,
which keeps the harness usable the next time the pins diverge — and they
will, since the two bump independently.

That makes the canonical form the load-bearing part: if the two sides encode
values even slightly differently, every digest shifts and the difference
reads as a data discrepancy rather than as the harness bug it is. Three
things hold it together:

- `internal/parity/canonical.go` and `record.go` are **byte-identical** in
  both repositories. `TestCanonicalCopyInSync` fails if they drift.
- `CanonicalFormVersion` is recorded in every artifact and checked before
  any comparison, so a deliberate change to the encoding cannot be read as a
  data change.
- Both sides SHA-256 the decompressed fixture XDR against the committed
  manifest before extracting, so neither can silently consume different bytes.

## Canonical form

`encoding/json` is unusable here. It renders every number as a float64-
compatible decimal, which rounds the int64 TOIDs and stroop amounts Bronze is
made of, and it cannot separate a NULL column from an empty string. Both
distinctions are exactly what this ticket needs.

So values carry an explicit type tag and an exact textual payload:
`n` null · `s:` string · `i:` int · `u:` uint · `b:` bool · `f:` float ·
`t:` RFC3339Nano UTC · `y:` base64 bytes · `l:[…]` list · `m:{…}` map.
Delimiters inside strings are escaped so a value cannot forge a field
boundary. `TestEncodeKeepsDistinctionsThatMatter` pins the cases that matter —
nil pointer vs empty string, nil slice vs empty slice, `2^53` vs `2^53+1`,
zero time vs Unix epoch.

Row order is **not** part of the contract: historical fans out to 21
goroutines, live runs sequentially. Digests are over sorted multisets, with
length prefixes so `{"ab"}` cannot collide with `{"a","b"}`. Duplicates are
retained — a row emitted twice differs from a row emitted once.

List order **is** part of the contract. Call-graph hops, auth credentials and
claimants are ordered arrays where a reordering is a real difference.

## Detecting run-dependent columns instead of declaring them

The first full run reported 13 of 21 tables as differing, almost all on
`created_at`. Both sides stamp `time.Now()` there, so the column can never
match and the real findings were buried.

A hand-written exclusion list would have been wrong. `created_at` is the
ledger close time on `transactions`, `operations`, `trades` and `effects`,
and an ingestion wall clock on nine other tables — the name does not predict
which. Excluding it globally, as the existing golden suite does, suppresses a
genuine comparison on four tables.

So each side extracts every fixture **twice** and any column whose value
multiset moves between the two passes is recorded as non-deterministic *for
that table*. The comparison skips the union of both sides' observations and
lists them in a `Skipped` column — a column nobody compared is not a column
that matched. `values-differ` dropped from 13 tables to 3.

Two guards on the probe: a changing *row count* between passes is recorded as
nothing (it is a non-deterministic extractor, far worse than a wall clock,
and absorbing it would hide it), and `era_id` / `version_label` stay on a
declared list because they are constant within a process and observation
cannot catch them.

What the probe found is finding **F7** in the discrepancy report.

## Layout

| Path | Role |
|---|---|
| `internal/parity/canonical.go` | Encoding, digests. Stdlib only — it has to be copyable. **Mirrored in the live repo.** |
| `internal/parity/record.go` | `Record`, `TableSnapshot`, `RowSet`, the non-determinism probe. **Mirrored.** |
| `internal/parity/compare.go` | Comparison and verdicts. This side only. |
| `internal/parity/sync_test.go` | Byte-identity guard across the two repositories |
| `parity_harness.go` | Historical extraction and the markdown renderer |
| `parity_export_test.go` | Historical export driver |
| `parity_compare_test.go` | Comparison driver |
| `parity_regression_test.go` | Hermetic CI tier |
| `testdata/parity/{historical,live}/*.digest.json` | Committed baselines, 644 KB |
| `docs/ticket-04-parity-matrix.md` | Generated per-fixture matrix |

In the live repository, all test-only:
`go/internal/parity/{canonical,record}.go` (the mirrored copies) and
`go/parity_export_test.go` (the adapter). Nothing is linked into the ingester
binary; no production file was modified.

The one change to production code here was splitting `decodeLedgerMeta` out of
`(*Worker).decodeLedger`, so the harness enters the Bronze mapping through
exactly the decode the worker uses rather than reimplementing close-time
conversion and the ledger-range bucket.

## Keeping the live adapter honest

`buildLiveParityRecord` restates `WriteBatch`'s extraction sequence rather
than calling it, because `WriteBatch` opens a database transaction, writes
checkpoints and broadcasts — none of which belong in a credential-free
comparison, and splitting it apart would be a production change this ticket
does not authorize.

Restating it means it can drift. `TestParityAdapterTracksWriteBatch` reads
both call sites out of the source and compares the set of
`extract.Extract*` calls each uses; an extractor added to one and not the
other fails the test. All 20 library calls live in `writer.go` and nowhere
else, so the scan is exact.

Two deliberate departures from production, both recorded in the report rather
than reproduced:

- **Errors are returned, never skipped.** `WriteBatch` logs-and-skips most
  extractor failures. Mirroring that would let a harness fault appear as
  "the live side emitted no rows" — the worst possible failure mode for a
  parity report. The asymmetry itself is finding F9.
- **Config-settings reconciliation is off**, so the comparison stays
  raw-XDR. Its provenance question is separate.

## Running it

```sh
make parity-export    # historical: decompress fixtures, extract twice, write artifacts
make parity-live      # live: same XDR, sibling repo (LIVE_INGESTER=<path> to override)
make parity-report    # compare both sides, including the row-level drill-down
```

Artifacts land in `build/parity/` (gitignored): `input/` holds the
decompressed XDR and manifests handed to the live side, `historical/` and
`live/` hold `*.digest.json` plus gzipped `*.rows.json.gz`, and `report/`
holds the per-fixture JSON and `summary.md`.

The row sets are ~43 MB gzipped for eight fixtures and half a gigabyte
uncompressed, which is why they are build output and the digests are not.

Narrower runs: `PARITY_FIXTURES=pubnet-tiny-dump make parity-export`.

### The hermetic tier

These need no live checkout, no database, no network, and run in CI:

- `TestHistoricalParityDigestsMatchCommitted` — re-extracts every fixture and
  diffs against `testdata/parity/historical/`, naming the table and column
  that moved. Skipped under `-short`.
- `TestParityMatrixMatchesCommittedDigests` — regenerates the docs matrix
  from the committed digests so the prose cannot drift from its numbers.
- `TestCanonicalCopyInSync` — skips when the live checkout is absent.

`make parity-matrix` regenerates the matrix. `make parity-baseline-update`
promotes a fresh export — never run it to make a failing test pass; the
baselines are the reviewed record of what each side produced at a pinned
revision.

## Open work

1. **Storage tier — live: done.** `go/storage_tier_test.go` in the live
   repository builds a hot buffer, drives the real `insert*` path over a
   fixture and reads the rows back. Gated on `PARITY_PG_DSN`. It found F12
   immediately: `contract_events` stored 2,035 of 14,515 rows, a defect no
   cross-ingester comparison could see because both sides extract the same
   rows and agree. Each insert runs in its own savepoint so one failure
   does not mask the rest — the six missing columns surfaced in one run
   rather than six.

   **Storage tier — historical: still open.** The Parquet path has a
   hand-written mapping layer of its own (`parquet_writer.go`'s `Parquet*`
   structs) that no audit covers, so a field can reach `types.go` and never
   reach a file. Read the columns back through DuckDB as
   `golden_harness.go` does.
2. **Canonical row contract: drafted.** `docs/bronze-row-contract.md`.
   Logical keys live in `internal/parity/keys.go` rather than in prose, and
   `TestLogicalKeysAreUnique` verifies all 21 against ~300,000 rows across
   protocols 1 to 29. Nineteen are unique; `contract_events` collides on 4%
   of rows (F13) and `token_transfers` has no key at all (F16), both
   recorded as defects rather than papered over.

   Still open in the contract: `created_at`'s two meanings, memo encoding,
   and whether a store may carry a declared subset of Bronze. Those are
   product decisions, not harness work.
3. **Fixture gaps.** `contract_code` emits nothing on any fixture;
   `config_settings` is exercised by one.
4. **Field-name alias map.** The comparison matches columns by name, so
   `SorobanFeeWrite1kb` / `SorobanFeeWrite1KB` reports as two one-sided
   columns (F8). An explicit alias table would separate "named differently"
   from "absent".
5. **Non-pubnet coverage.**
