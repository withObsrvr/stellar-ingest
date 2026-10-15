# Phase 0 — Golden fixture and baseline test plan

Historical Phase 0 plan for TICKET 02. The plan has since been partially
executed: eight real-ledger fixtures and their goldens are committed, covering
all 21 Bronze tables. Process-crash and storage-recovery scenarios remain.
For the current status and revised fixture sizes, see `ticket-02-progress.md`.

## Original motivation

At bootstrap, integration tests skipped behind unset environment variables,
and nothing processed a real ledger. The hermetic golden suite now runs against
committed XDR fixtures; the original planning assumptions below are retained
as a historical record, not current instructions.

## Fixture selection

Pick ranges that are small enough to commit or cheaply re-fetch, and that each
exercise something specific.

| Fixture | Range | Exercises |
|---|---|---|
| `pubnet-classic` | 1,000 mainnet ledgers, pre-Soroban | Classic operations, effects, trades, trustlines. Comparable to the handoff's benchmark range. |
| `pubnet-soroban` | 1,000 mainnet ledgers, post-Protocol 20 | Contract events, contract data, contract code, TTL, restored keys, contract creations. |
| `protocol-boundary` | A range spanning a protocol-version bump | Era handling, `config_settings` changes across the boundary. |
| `state-churn` | A range with account creation, trustline changes, and offer lifecycle | Snapshot-table correctness. |
| `sparse` | A range with near-empty ledgers | Empty-table handling; every one of the 21 writers must behave on zero rows. |
| `eviction` | A range containing state eviction | `evicted_keys`, `ttl`. |

Exact sequence numbers are chosen when the fixtures are built, and pinned in
`testdata/fixtures.json` alongside each range's source archive and the ledger
hashes, so a fixture is verifiable rather than merely named.

## Storage format

Store fixtures as **raw `LedgerCloseMeta` XDR** — `nebu fetch` output consumed
through `--storage-type XDR`. This is the right level because it is exactly
what the handoff's "raw XDR bytes must remain faithful to the source" invariant
protects, it removes network dependence from the test, and it is the same bytes
every source backend converges on, so a fixture test isolates extraction from
acquisition.

Commit small fixtures directly; for anything large, commit the manifest plus a
fetch script and a checksum, and let CI cache them.

## Assertions

For each fixture, across all 21 tables:

1. **Row counts** per table.
2. **Content hash** per table — a stable hash over sorted, normalised rows, so
   the assertion is order-independent. Row ordering within a Parquet file is
   not a contract and should not be asserted.
3. **Null semantics** — explicitly assert which columns are null, not just
   that the row exists. The handoff calls out null-state enrichment cases, and
   `--enrich-contract-balances` exists precisely because `balance_holder` and
   `balance` were previously written null. A test that ignores nulls would not
   have caught that.
4. **TOIDs** — assert exact values. They encode ledger/transaction/operation
   position and are the join key for downstream consumers; an off-by-one here
   is both easy to introduce and invisible in a row-count check.
5. **Source XDR fidelity** — hash the raw bytes in and the raw bytes recorded,
   and assert they match.
6. **Exit code** and the parsed summary line.

## Recovery and integrity scenarios

From the handoff's test plan, each needs a fixture-driven test:

- Crash before sink commit; crash after sink commit; resume.
- Resume with a *different* `--workers` count than the original run. Shard
  boundaries are derived from worker count, so this is the case most likely to
  break and least likely to be exercised by accident.
- Partial writer failure.
- Corrupted XDR input.
- Empty tables.
- Re-running an already-complete range.
- Idempotent repair — `--enrich-contract-balances` twice over the same range
  with the same run ID produces the same result.

## Determinism hazards to control

Found while reading the baseline. Each of these will make a fixture test flaky
if not pinned:

- **`--workers` defaults to `runtime.NumCPU()`** — machine-dependent. Every
  fixture test must pass `--workers` explicitly.
- **`--network-passphrase` defaults to testnet** — a mainnet fixture that omits
  it is mis-labelled and may produce different derived values. Always explicit.
- **Timestamps and run IDs** in manifests and lineage records vary per run;
  normalise them out before hashing.
- **Absolute paths** appear in catalog output; normalise.
- **`--chunk-size 0` vs non-zero** are different code paths. Cover both: the
  same range processed as one chunk and as several must produce identical
  Bronze output.

## Harness shape

A table-driven Go test reading `testdata/fixtures.json`, gated behind a build
tag or an environment variable so the fast unit suite stays fast, and run as a
separate CI job. Golden files regenerate with an explicit `-update` flag, and a
regenerated golden must be reviewed as a diff — never regenerated to make a
failing test pass.
