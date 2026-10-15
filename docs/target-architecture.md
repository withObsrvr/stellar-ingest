# Target architecture

The agreed destination for the incremental refactor. Recorded at bootstrap
so Phase 2 has a decided endpoint. The CLI and most application code remain a
flat `package main`; a private `internal/bronze/restored` package and a
worker-independent mapping entry point are now implemented. See
[`module-boundaries.md`](module-boundaries.md) for the **current** layout.
The rest of the tree below is a target, not a description of today's code.

## The principle

One canonical Stellar ingestion implementation with multiple execution modes —
not a collection of small utilities that force you to reassemble the workflow
each time.

`stellar-ingest` stays a complete, standalone application. A full historical
backfill remains one command, with no gRPC service and no control plane
required. What changes is internal organisation: the reusable engine becomes
importable without dragging in the CLI, the DuckLake configuration, or the
backfill scheduler.

No capability is removed to achieve this.

## Ownership boundaries

| Owner | Owns | Does not own |
|---|---|---|
| `stellar-raw-ledger-origin` | Reading ledgers from RPC, archive, Captive Core; faithful `RawLedger`/`LedgerCloseMeta` representation; Nebu and flowctl source adapters; acquisition benchmarks | Bronze schemas, Parquet layout, DuckLake loading, Lake lifecycle |
| `stellar-ingest` | Decoding raw ledgers into the 21 Bronze datasets; validation, batching, checkpoint/replay; Parquet, DuckLake, and PostgreSQL destinations; the standalone CLI | A second general-purpose orchestration runtime; Silver/Gold transforms; the explorer serving API |
| Obsrvr Lake | Managed ingestion jobs; hot/cold storage policy; flusher, retention; Silver and serving projections; deployment | Duplicate ledger-to-Bronze mapping rules |
| flowctl / Nebu | flowctl: deployed components and long-lived workflows. Nebu: composable local pipelines | Domain-specific 21-table Bronze semantics |

`stellar-raw-ledger-origin` stays a separate repository. Its usefulness
independent of the Bronze schema is the point — other Stellar applications can
consume raw ledgers without adopting DuckLake or PostgreSQL.

## The problem this is actually solving

`stellar-history-loader` and `stellar-postgres-ingester` each implement
extraction of the same 21 Bronze tables. They may agree today; nothing keeps
them agreeing.

Fix how `contract_data_snapshot_v1` interprets a Soroban balance entry in the
historical loader, forget the live ingester, and the two paths now disagree
about the same ledger. For a platform that has to serve historical and current
data as one dataset, that is worse than maintaining several executables.

The target:

```
                    Canonical Bronze extractor
                              |
                  +-----------+-----------+
                  |                       |
           Historical worker         Live worker
                  |                       |
             DuckLake                  PostgreSQL
```

Shared extraction; independent batching, workers, destinations, and deployment.

## Target package layout

```
stellar-ingest/
├── cmd/
│   └── stellar-ingest/        # CLI entry point
├── internal/
│   ├── cli/                   # Flag and config parsing
│   ├── engine/                # Execution, scheduling, batching
│   └── workflow/              # Backfill, replay, repair modes
├── bronze/
│   ├── extractors/            # The 21 canonical extractors
│   ├── schemas/               # Versioned row contracts
│   └── validation/
├── source/
│   └── adapters/              # FS, XDR, and ledgerstream delegation
├── sink/
│   ├── parquet/
│   ├── ducklake/
│   └── postgres/
├── checkpoint/
└── tests/
    ├── fixtures/
    ├── parity/
    └── benchmarks/
```

`bronze/`, `source/`, `sink/`, and `checkpoint/` are importable because
`stellar-postgres-ingester` is a real second consumer. Everything else stays
`internal/` until something outside this repository actually needs it — do not
export an interface before a second consumer exists for it.

## Interface sketch

Illustrative of responsibilities, not a finished API:

```go
type LedgerSource interface {
    Stream(ctx context.Context, from uint32) (LedgerStream, error)
}

type BronzeExtractor interface {
    Extract(ctx context.Context, ledger RawLedger) (BronzeBatch, error)
}

type BronzeSink interface {
    Write(ctx context.Context, batch BronzeBatch) error
}

type CheckpointStore interface {
    Load(ctx context.Context, jobID string) (Checkpoint, error)
    Save(ctx context.Context, checkpoint Checkpoint) error
}
```

Two cautions that have to survive into the real implementation:

**Acknowledgement boundaries are not shown here.** Saving a checkpoint after
writing data is only safe if replay is idempotent, or if the write and the
checkpoint commit atomically. The invariant is that a durable checkpoint never
advances ahead of committed destination data.

**`BronzeSink` is not the same abstraction as a generic Parquet writer.**
DuckLake has catalog transactions and metadata requirements PostgreSQL does
not. Share the Bronze *data model*; let each destination keep its own writing
strategy. Over-generalising here hides exactly the semantics that matter during
recovery.

## Execution modes

The same engine serves one-shot backfills and continuous processing, so there
is never a second implementation of the 21 extractors:

```sh
# Local, standalone — no daemon, no control plane
stellar-ingest run --config mainnet-backfill.yaml
stellar-ingest inspect --config mainnet-backfill.yaml

# Managed, through the Obsrvr CLI
obsrvr lake ingest create --config mainnet-backfill.yaml
obsrvr lake ingest inspect <job-id>
obsrvr lake ingest events <job-id> --follow
obsrvr lake ingest cancel <job-id>
```

flowctl orchestrates. `stellar-ingest` knows how to process Stellar ledgers.
Obsrvr Lake owns the datasets and their lifecycle.

Durable job state belongs to the orchestrator. Per the handoff, do not add
job `wait`/`events` subcommands to `stellar-ingest` until a durable job
registry exists to back them.

## Storage paths stay plural for now

Keep DuckLake-direct and PostgreSQL-hot-then-flush both available. The choice
between them gets made from measurement on identical decoded Bronze batches
(TICKET 07), not from architectural preference.

Three concerns that are easy to conflate and should not be:

1. Where the complete history is durably stored.
2. Where ingestion is buffered for downstream transformation.
3. Where low-latency queries are served.

Those can be three different systems. PostgreSQL may remain the right hot
buffer or serving index even if DuckLake becomes the authoritative historical
store — historical serving tables consuming too much disk is not by itself an
argument against the ingestion buffer.

The handoff's benchmark is the reason this stays open: DuckLake ingestion took
75.1 s of a 98.9 s run against 23.2 s of extraction. The current `INSERT INTO`
path warrants independent optimisation — batch sizing, Parquet-to-DuckLake
write amplification, file layout, catalog commits — before the storage
architecture is changed on the strength of those numbers.

## Ordering

1. Bronze parity test suite — feed identical fixtures to the history loader and
   the PostgreSQL ingester, compare normalised output across all 21 tables.
   This establishes whether they already disagree, before any consolidation.
2. Extract the canonical Bronze decoder. Choose correct behaviour field by
   field; do not assume the history loader is authoritative. Both
   implementations may hold fixes the other lacks.
3. Move this application onto the shared packages, preserving CLI and
   operational behaviour. Re-run benchmarks for regressions.
4. Move `stellar-postgres-ingester` onto them, replacing only the decode and
   extract layer. Keep its gRPC source, PostgreSQL destination, reconnection
   handling, and checkpoints. Validate replay, restart recovery, and ordering.
5. Evaluate storage paths on identical input.

This is consolidation, not a greenfield rewrite. The existing components work
and the recovery logic in them is valuable.

## Open decisions

Carried from the handoff, unresolved:

- (a) Authoritative schema package and versioning, including the messages-only
  `RawLedger` copy versus `flow-proto`. The current guard test detects drift
  between the two copies; the preference is to generate adapters from one
  authoritative schema rather than rely indefinitely on two copies staying in
  sync.
- (b) Whether externally importable Bronze libraries live here or in a later
  dedicated module.
- (c) Checkpoint format migration and versioning.
- (d) Which source adapters stay embedded versus service-only.
- (e) Whether contract-balance enrichment belongs in a general repair package
  or a Lake-specific maintenance module.
- (f) Which deployment paths currently depend on mandatory flowctl
  registration. Note that *code-level* registration is already opt-in — see
  `docs/phase0-inventory.md` §7 — so this is purely a question about
  deployment manifests.
