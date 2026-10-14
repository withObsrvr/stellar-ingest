# TICKET 02 — progress

Golden ledger fixtures and baseline snapshot tests for all Bronze outputs.
**Harness complete; one coverage gap remains.** This records the decisions made, what was measured, and what
is left, so the work can be picked up without re-deriving any of it.

## Decisions

| Decision | Choice |
|---|---|
| Network | Pubnet only. Matches production ingestion and the handoff benchmark. |
| Fixture storage | Small fixtures committed to the repository, plus a separate opt-in tier that fetches large ranges from GCS. |
| Golden format | Per-table row counts and an order-independent content hash for every fixture, plus full normalised row dumps for one deliberately tiny fixture. |
| Range selection | Derived empirically by probing the archive. |

## Measured facts

All measured against
`gs://obsrvr-stellar-ledger-data-pubnet-data/landing/ledgers/pubnet`.

| Fact | Value |
|---|---|
| Archive coverage | Ledger 0 – 64,959,999; 1,015 partitions; full pubnet history |
| Archive layout | zstd, `ledgersPerBatch` 1, `batchesPerPartition` 64,000 |
| Stored object size | 106–134 KB zstd per modern ledger |
| Decoded size | ~2.2 MB per modern ledger (3 ledgers → 6,640,220 bytes) |
| Framed-stream compression | 13.0x with `zstd -19`, ~166 KB per modern ledger |
| Bronze rows produced | 29,918 rows from 3 modern ledgers — ~10 K rows per ledger, consistent with the handoff's 8.79 M / 1,000 ledgers |

**Consequence.** The fixture plan in `docs/phase0-fixtures.md` assumed
1,000-ledger fixtures. At ~166 KB compressed per ledger that is ~166 MB per
fixture, which cannot be committed. Committed fixtures are therefore 3–20
ledgers; 1,000-ledger ranges belong to the opt-in fetched tier.

## Protocol map

Coarse sweep across history (ledger → protocol version):

```
100:P1   1M:P1    5M:P2    10M:P4   15M:P8   20M:P10  25M:P11
30M:P12  35M:P16  40M:P18  45M:P19  48M:P19  50M:P19  52M:P20
55M:P22  58M:P22  60M:P24  62M:P25  64M:P27  64.9M:P29
```

**Soroban activation located by binary search: last P19 ledger is 50,457,423,
first P20 ledger is 50,457,424.**

## Planned fixtures

| Fixture | Range | Ledgers | Size | Covers |
|---|---|---|---|---|
| `pubnet-genesis-p1` | 100–104 | 5 | 2 KB | Protocol 1, zero transactions, empty-table path |
| `pubnet-classic-p19` | 50,000,000–50,000,009 | 10 | 1.16 MB | Busy pre-Soroban classic |
| `pubnet-protocol-boundary` | 50,457,419–50,457,428 | 10 | 1.59 MB | P19→P20 transition; emits the most tables (18) |
| `pubnet-soroban-p29` | 64,938,240–64,938,249 | 10 | 1.80 MB | Dense contract events and Soroban ops |
| `pubnet-tiny-dump` | 64,938,234–64,938,236 | 3 | 0.49 MB | Small enough for full row dumps; drives the determinism test |
| `pubnet-contract-creation` | 64,936,731–64,936,735 | 5 | 0.40 MB | The only fixture producing `contract_creations` — deployments are rare, 3 per 1,000 ledgers |

All committed, 5.5 MB total. Stored as `zstd -19` framed XDR except the
genesis fixture, which is small enough to commit raw.

| `pubnet-state-eviction-p23` | 59,000,625–59,000,629 | 5 | 0.71 MB | Mass Soroban state archival — 2,000 evicted keys per ledger, 10,000 `evicted_keys` rows |

**The archival window.** Eviction turned out to be bursty, not steady. Sampling
400-ledger windows at protocols 20, 22, 23, 25 and 27 found zero evictions
anywhere. A contiguous 2,000-ledger scan at 59,000,000 (protocol 23) found
**990 of 2,000 ledgers evicting, 1,970,054 keys in total**, a steady 2,000 per
ledger — a bulk state-archival event rather than routine background eviction.
That is why sparse sampling kept missing it, and it is the reason the fixture
is pinned to an exact range rather than "somewhere in protocol 23".

**Still uncovered: `restored_keys`.** It needs a successful `RestoreFootprint`
operation. None appeared in 8,000 ledgers — four windows around the archival
event (59,000,625 / 59,002,000 / 59,010,000 / 59,100,000) and four later ones
at 59.5M, 61M, 63M and 64.9M. Restores are far rarer on pubnet than evictions.

There is also a **suspected extraction gap** that may be part of the reason.
`extractRestoredKeys` unwraps fee-bump envelopes to find the operation list,
but the `switch` that resolves the footprint handles only
`EnvelopeTypeEnvelopeTypeTx`. A fee-bumped `RestoreFootprint` is therefore
located and then silently skipped at `if footprint == nil { continue }`. Fee
bumps are common, so this could be hiding real restores. It is left unfixed
deliberately: confirming the fix needs a fee-bumped restore to test against,
and changing Bronze output without one would be unverifiable.

## Table coverage gap

Extracting 3 modern ledgers produced only 15 of the 21 Bronze tables plus
`_meta_lineage`. Absent: `claimable_balances_snapshot`, `config_settings`,
`contract_code_snapshot`, `contract_creations`, `evicted_keys`,
`restored_keys`.

Two things follow. First, **an empty table produces no directory at all**, so
the golden format must distinguish "table not emitted" from "table emitted with
zero rows" — otherwise a regression that silently stops emitting a table reads
as a pass. Second, no single range covers all 21 tables, so the fixture set has
to cover them collectively, and the suite needs an assertion that the union of
fixtures touches every table.

Note also that `_meta_lineage` is a 22nd output not listed among the 21 Bronze
tables.

## Determinism hazards

Each of these makes a golden comparison flaky if not controlled. The first two
were found by reading the `ledgers` schema; the third by having a run fail on
this very host.

1. **Wall-clock fields inside Bronze rows.** `ingestion_timestamp` on
   `ledgers`, and `CreatedAt`/`UpdatedAt` set from `time.Now()` in
   `extractors_accounts.go`. These vary per run and must be normalised out
   before hashing. Worth deciding separately whether they belong in Bronze at
   all — they make a row irreproducible from the same ledger.
2. **Build and run identity.** `version_label` comes from the `-ldflags`
   version and `era_id` from `--era-id`.
3. **Ambient environment.** `ENABLE_FLOWCTL=true` is exported in at least one
   developer shell on this host, and it made a standalone run fail closed with
   `FLOWCTL_ENDPOINT is required`. The harness must scrub `ENABLE_FLOWCTL` and
   the `ledgerstream` variables (`STORAGE_TYPE`, `BUCKET_NAME`, `BUCKET_PATH`,
   `RPC_ENDPOINT`, `RPC_AUTH_HEADER`, `S3_ENDPOINT_URL`, the `STELLAR_CORE_*`
   set) rather than inherit them.
4. **Output path varies per run.** Output lands in
   `<output>/attempts/<UTC timestamp>-<id>/`, so the harness must locate the
   attempt directory dynamically rather than assume a fixed path.
5. **`--workers` defaults to `runtime.NumCPU()`** and
   **`--network-passphrase` defaults to testnet**. Both must be explicit in
   every fixture run.

## Bug found and fixed

`--storage-type XDR` was completely broken. `NewXDRStreamSource` detected end
of stream with `err == io.EOF`, but the XDR decoder wraps the error — at a
clean end of stream `errors.Is(err, io.EOF)` is true while `err == io.EOF` is
false. Every complete file therefore failed with:

```
XDR stream decode error: decoding Int: xdr:DecodeInt: EOF while decoding 4 bytes
```

This made the `nebu fetch` workflow documented in the README unusable. Fixed in
`worker.go` with `errors.Is`, and covered by
`TestXDRStreamSourceReadsCompleteFile`, which was confirmed to fail when the
comparison is reverted.

The pre-existing `TestXDRStreamSourceRejectsTruncatedFrame` exercised only the
*error* path and never a successful complete read, which is how the bug
survived. That shape — testing rejection without testing acceptance — is worth
checking for elsewhere.

## Remaining work

1. Fetch and commit the four outstanding fixtures.
2. Locate an eviction-bearing range with a wider scan.
3. Build the normalisation and hashing harness: strip the non-deterministic
   fields, sort rows, hash per table, record counts, and distinguish an absent
   table from an empty one.
4. Generate and commit goldens; add the `-update` regeneration flag.
5. Add the full row dump for `pubnet-tiny-dump`.
6. Assert the fixture set collectively covers all 21 tables.
7. Add the opt-in large-range tier and wire both into CI as a separate job.
8. Recovery and integrity scenarios from `docs/phase0-fixtures.md`, especially
   resume with a different `--workers` count and `--chunk-size 0` versus
   non-zero producing identical output.

## The harness

`golden_harness.go` and `golden_test.go`. Hermetic — fixtures are committed, so
no network access and no cloud credentials are needed. Runs in ~9 seconds and
is wired into `make golden` and the CI workflow.

### How a digest is computed

Per table: the row count, the surviving column list, and an order-independent
content hash. The hash is an MD5 per row of the row rendered as JSON,
aggregated over the row hashes in sorted order.

Sorting the hashes rather than the rows makes the result independent of Parquet
row ordering, which is not a contract and varies with worker count and shard
boundaries. Rendering as JSON rather than concatenating values means a column
rename or type change also moves the hash, so schema drift is caught alongside
value drift.

DuckDB does the reading, which avoids writing a bespoke Parquet reader for 21
different schemas.

### Absent versus empty

Digests are keyed by table. A table missing from the map was not emitted at
all, which is deliberately distinct from one present with `row_count` 0 —
the writer creates no directory for a table with no rows, so without this
distinction a regression that silently stopped emitting a table would read as
a pass. `compareDigests` reports set differences separately from content
differences for the same reason.

### What is excluded, and why that is a workaround

`nonDeterministicColumns` drops `ingestion_timestamp`, `created_at`,
`updated_at`, `version_label`, and `era_id`. The `_meta_lineage` output is
excluded wholesale as pure provenance.

Excluding these is a workaround, not an endorsement. A Bronze row that cannot
be reproduced from the same ledger is a data-model problem. Whether wall-clock
stamps belong in Bronze at all is worth deciding separately.

### Tests

| Test | What it pins |
|---|---|
| `TestGoldenBronzeOutputs` | Every fixture's every table against a committed digest. `-update` regenerates. |
| `TestExtractionIsDeterministic` | The same fixture extracted twice must produce identical digests. |
| `TestFixturesCoverAllBronzeTables` | The fixture set collectively covers all 21 tables, minus recorded gaps. |
| `TestXDRStreamSourceReadsCompleteFile` | A complete XDR stream reads without error. |
| `TestFixtureIntegrity` | Fixture bytes match their manifest SHA-256. |

## Second bug found: `operations.contracts_involved` was not reproducible

`TestExtractionIsDeterministic` caught it immediately. `call_graph.go` built
the `contracts_involved` slice by ranging over a Go map, whose iteration order
is randomised. That slice is serialised into the `contracts_involved` JSON
array, so **the same ledger produced a different value on every run**.

Row counts matched, so nothing short of a content comparison would have caught
it — and it only affects Soroban-era data, so the classic fixtures were clean.
Fixed by sorting the slice before returning it.

This one matters beyond the test suite: any downstream consumer comparing or
deduplicating on `contracts_involved` was getting inconsistent results, and the
historical and live ingestion paths could disagree on identical input. It is
exactly the class of divergence TICKET 04 is meant to detect between the two
extractors.

## Remaining work

1. Locate an archival window covering `evicted_keys` and `restored_keys`.
2. Full normalised row dumps for `pubnet-tiny-dump` — the hybrid golden format
   calls for them and only hashes exist so far.
3. The opt-in large-range tier (1,000-ledger ranges fetched from GCS).
4. Recovery and integrity scenarios from `docs/phase0-fixtures.md`: crash
   before and after sink commit, resume with a different `--workers` count,
   partial writer failure, corrupted XDR, re-running a complete range,
   idempotent repair.
5. `--chunk-size 0` versus non-zero producing identical Bronze output.
6. Decide whether the wall-clock columns belong in Bronze at all.

## Nix flake

`flake.nix` provides a reproducible build, a dev shell, a container image, and
`nix flake check`.

| Output | What it is |
|---|---|
| `packages.default` / `packages.stellar-ingest` | The binary, with `stellar-history-loader` symlinked beside it and `v3_bronze_schema.sql` installed under `share/` |
| `packages.docker` | Layered image, entrypoint `stellar-ingest`, non-root |
| `apps.default`, `apps.stellar-history-loader` | `nix run` targets for both names |
| `checks.stellar-ingest` | What `nix flake check` builds |
| `devShells.default` | Go, CGO toolchain, `duckdb`, `zstd`, `google-cloud-sdk`, golangci-lint |

Unlike the other Obsrvr services, this cannot be built with `CGO_ENABLED=0` —
`duckdb-go/v2` is cgo and bundles its own DuckDB. The flake sets
`CGO_ENABLED=1` accordingly.

`doCheck = true`, so the hermetic test suite — golden Bronze snapshots
included — runs inside the nix sandbox on every build. `preCheck` unsets
`ENABLE_FLOWCTL` and the `ledgerstream` variables so nothing leaks in. The
check timeout is raised to 20 minutes because linking DuckDB is slow.

### The one thing that will trip you up

**Flakes only see git-tracked files.** A new or edited file must be at least
`git add`-ed before nix will build it, otherwise you get
`Path 'flake.nix' ... is not tracked by Git`. This bit during setup. The
`Makefile` nix targets carry a comment saying so.

The `vendorHash` is pinned in `flake.nix`. It changes whenever dependencies
change: build once, take the `got:` hash from the mismatch error, paste it in.
