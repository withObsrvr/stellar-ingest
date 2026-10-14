# TICKET 02 — progress

Golden ledger fixtures and baseline snapshot tests for all Bronze outputs.
**In progress.** This records the decisions made, what was measured, and what
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

| Fixture | Range | Covers | Status |
|---|---|---|---|
| `pubnet-genesis-p1` | 100–104 | Protocol 1, zero transactions, empty-table path | **Committed** (2,060 bytes) |
| `pubnet-classic-p19` | around 50,000,000 | Busy pre-Soroban classic | Not fetched |
| `pubnet-protocol-boundary` | 50,457,419–50,457,428 | P19→P20 transition | Not fetched |
| `pubnet-soroban-p29` | 64,938,234–64,938,243 | Dense contract events and Soroban ops | Not fetched |
| `pubnet-tiny-dump` | 64,938,234–64,938,236 | The fixture that gets full row dumps | Not fetched |
| eviction | unlocated | `evicted_keys`, state archival | **Not found** — a 401-ledger scan at the chain tip returned zero ledgers with `evicted_keys_count > 0`. Needs a wider search; no fixture will be fabricated for it. |

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
