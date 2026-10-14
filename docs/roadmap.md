# Roadmap

Phases and acceptance gates from *Stellar Ingest & Raw Ledger Origin —
Developer implementation handoff* (14 October 2026), annotated with what the
Phase 0 inventory actually found.

## Phase 0 — Baseline and inventory

**Status: complete.**

| Item | State |
|---|---|
| Record source commit, binaries, config, schema, scripts | Done — `PROVENANCE.md` |
| Inventory public flags and environment variables | Done — `docs/phase0-inventory.md` §2, §3 |
| Behaviour matrix for all source and output modes | Done — §4, §5 |
| Identify workflows requiring flowctl registration | Partly — code-level answer in §7; the deployment-manifest half needs information outside this repository |
| Baseline metrics and golden fixtures | Seven fixtures committed; 20/21 tables covered. See `docs/ticket-02-progress.md`. `restored_keys` remains open. |

The compatibility inventory and hermetic baseline suite are committed. Full
21-table baseline coverage remains open under TICKET 02. Since bootstrap,
two reproducibility fixes and XDR EOF handling have changed behavior; the
fee-bump `restored_keys` footprint fix is unit-tested but still needs a
real-ledger golden. The original "no functional changes yet" Phase 0 condition
no longer describes the current tree.

## Phase 1 — Move and stabilize

**Status: largely complete.**

| Item | State |
|---|---|
| Move loader into `stellar-ingest` with clean module path, reproducible build, Docker, README, Makefile, CI | Done |
| Keep `stellar-history-loader` invocation available; add `stellar-ingest` as primary | Done — both built from one package; `docs/compatibility.md` |
| Make control-plane registration optional, fail-closed when enabled | **Already true before the move** — see inventory §7. No code change needed. Remaining work is a regression test. |
| Cobra subcommands (`run`, `extract`, `validate`, `load`, `repair`, `inspect`) | Not started — TICKET 06 |

Exit gate — "equivalent output and return codes on golden workloads; packaged
binary/image runs independently; operational rollbacks documented":

- Equivalent return codes: verified on the three reachable
  validation-failure paths.
- Equivalent output: verified for `--help`, and now over real ledger data for
  20 of the 21 Bronze tables across 7 committed pubnet fixtures via
  `make golden`. `restored_keys` remains uncovered.
- Packaged binary runs independently: yes — standalone with `ENABLE_FLOWCTL`
  unset. **The Docker image has not been built or run.** No Docker daemon was
  available on the bootstrap host, so the `Dockerfile` is unverified; the CI
  workflow builds it and smoke-tests both executable names, and that job is the
  first thing to watch on the opening pull request.
- Rollback documented: yes — `docs/compatibility.md`.

## Phase 2 — Consolidate shared processing internals

**Status: incremental work underway.** Target layout in `docs/target-architecture.md`.
A private Parquet worker boundary and failure-injection checkpoint tests are in
place; see `docs/ticket-03-progress.md`. The full package extraction and
cross-repository Bronze parity are still outstanding. Existing goldens guard
20 tables; `restored_keys` requires its real-ledger fixture.

## Phase 3 — Source integration and replacement evaluation

**Status: further along than the handoff assumes.**

Four of six backends — `RPC`, `GCS`, `S3`, `CAPTIVE_CORE` — already delegate to
`stellar-raw-ledger-origin/source/ledgerstream` (pinned `v0.2.5`), in-process
as a Go library rather than over gRPC, which is the shape the architecture
prefers. `SDKStreamSource.StreamRange` already enforces ordering, bounded-range
end, and incomplete-range detection.

Remaining: contract tests (TICKET 05); whether `FS` and `XDR` move behind
`ledgerstream` too; ownership and release policy for `stellar.v1.RawLedger`;
and the separate question of retiring `stellar-live-source-datalake`, which
needs testnet and mainnet canaries first.

## Phase 4 — Optimize storage and managed workflows

**Status: not started.** TICKET 07.

The handoff's benchmark — 75.1 s DuckLake ingestion against 23.2 s extraction
on a 98.9 s run — is the reason this phase exists, and the reason the current
`INSERT INTO` path should be optimised independently before the storage
architecture is changed on the strength of those numbers.

---

## Tickets

| Ticket | Description | Status |
|---|---|---|
| 01 | Bootstrap `stellar-ingest`; provenance, CI, reproducible build, README, compatibility executable | **Done** |
| 02 | Golden ledger fixtures and baseline snapshot tests for all Bronze outputs | **Harness complete, coverage incomplete** — 7 fixtures, 20 of 21 tables covered; `restored_keys` still needs a real-ledger fixture. Fee-bump footprint handling now has a unit regression test. See `docs/ticket-02-progress.md`. |
| 03 | Documented module boundaries; extract reusable Bronze mapping behind tests without changing behaviour | **In progress** — private worker writer seam and fault tests; no package extraction yet. See `docs/ticket-03-progress.md`. |
| 04 | Compare `stellar-postgres-ingester` field mappings; discrepancy report | Not started |
| 05 | Source contract tests between `stellar-raw-ledger-origin` and the legacy live source | Not started |
| 06 | CLI compatibility plan and opt-in new command hierarchy | Not started |
| 07 | Benchmark identical batches through DuckLake and PostgreSQL sinks | Not started |

## Known gaps carried over unchanged

Recorded so they are not mistaken for things the bootstrap introduced:

1. **Credentials accepted as CLI arguments** — `--b2-key-secret`,
   `--pg-password`, `--ducklake-catalog`. Deviates from the handoff's
   credential invariant. Environment fallbacks would be additive and
   non-breaking. Unticketed.
2. **Range semantics are inconsistent** — `--start`/`--end` are inclusive
   (`end - start + 1`) but validation rejects `end <= start`, so a
   single-ledger range cannot be expressed. Needs a decision, not an
   accidental fix.
3. **`--network-passphrase` defaults to testnet** — a mainnet run that omits it
   is silently mis-labelled.
4. **`--workers` defaults to `runtime.NumCPU()`** — runs are not reproducible
   across machines.
5. **Console banner still reads `=== Stellar History Loader ===`** — left in
   place deliberately to hold output parity; revise with TICKET 06.
6. **Two live copies of this code** — here and at
   `ttp-processor-demo/obsrvr-lake/stellar-history-loader`. Any fix must be
   applied to both until that copy is retired.

## Delivery checklist

| | Item |
|---|---|
| ◐ | Standalone build, CLI, CI, and documentation work from the new repository — verified locally. Docker package written but **unbuilt**; pending the CI `docker` job. |
| ☐ | Existing historical workflows reproduce baseline results without flowctl — standalone operation confirmed; *baseline results* unverified pending TICKET 02 |
| ☐ | 21-table parity suite green or discrepancies catalogued |
| ☐ | Source contract verified with `stellar-raw-ledger-origin` without merging repositories |
| ☑ | No production live source or hot/cold transition changed — the bootstrap is a copy; nothing was removed or rerouted |
| ☐ | Operator docs include recovery, config migration, credentials, and data-integrity validation — config and credentials documented; recovery and integrity validation pending |
