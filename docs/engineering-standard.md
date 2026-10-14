# Conformance to the Obsrvr Quality CLI Engineering Standard

Source: *Obsrvr Quality CLI Engineering Standard*, v1.0, OBSRVR Engineering,
14 October 2026. It applies to `stellar-ingest`, `stellar-raw-ledger-origin`,
`nebu`, flowctl integrations and the proposed `obsrvr` umbrella CLI.

This file records where this repository stands against it and which roadmap
ticket closes each gap. It is a gate, not a wish list: a change that moves a
row here should say so in its commit.

The standard's own adoption plan puts this repository first — *"Phase 1:
publish this convention and retrofit one tool (stellar-ingest) **without
behavioral changes**"*. That constraint matters more than the speed of
adoption: the Phase 1 exit gate already verified output parity over real
ledger data for all 21 Bronze tables, and a restructure that quietly changed
behaviour would invalidate it. §8 says the same thing in plainer words —
*"Refactor internals, do not dismantle working end-to-end workflows."*

## Summary

| § | Area | State |
|---|---|---|
| 1 | Principles from the Rex demonstration | Mostly N/A — a batch tool, not a stateful service |
| 2 | Core first, interfaces second | ◐ Partial — mapping extracted behind boundaries, but the composition root is still one `package main` |
| 3 | Command design contract | ☐ Gap — flat flags, no `--output`, no subcommands (Ticket 06) |
| 4 | Resources, jobs, queries, events | ◐ Partial — attempts and manifests exist; no job ID surface or events |
| 5 | Reliability and operational correctness | ◐ Partial — strong on data guarantees and checkpointing; **credentials in argv is an open violation** |
| 6 | Go implementation guide | ☐ Gap — no Cobra, no `slog`, flat layout |
| 7 | Umbrella and specialized tools | ☑ Conforms — independent, usable without the platform |
| 8 | Application to Stellar ingestion tools | ☑ **Canonical Bronze condition met** for the extraction tier (Ticket 04) |
| 9 | Example command vocabulary | ☐ Gap (Ticket 06) |
| 10 | Release gate / PR checklist | ◐ Partial — see below |

## §10 release gate, item by item

| Item | State |
|---|---|
| Core business logic testable without terminal or network transport | ☑ `extractBronzeLedger` runs from a committed fixture with no process, no network and no database; the Ticket 04 harness is built on exactly that |
| `--help`, `--version`, completion, examples reviewed | ◐ `--help` is pinned by `make parity-baseline` against a committed surface; no completion, no examples |
| JSON responses and NDJSON event format covered by golden tests | ☐ No JSON output mode exists (Ticket 06) |
| stdout/stderr separation; noninteractive behaviour verified | ◐ Noninteractive and verified hermetically; stdout/stderr separation is not enforced — progress and results both go to the default logger |
| Cancellation, timeout, retries, interrupted runs exercised | ◐ Checkpoint fault injection and a real-Parquet restart simulation exist (Ticket 03); no SIGINT/SIGTERM or context-cancellation test |
| No secrets in arguments, diagnostics, logs, or inspect output | ☐ **Violated** — `--b2-key-secret`, `--pg-password`, `--ducklake-catalog` are CLI arguments, visible in `ps`. Roadmap known gap 1 |
| Resource IDs, event types, state transitions, error codes documented | ◐ Attempt IDs and manifests documented; no error-code taxonomy |
| Compatibility and migration documented for changed flag/config/schema | ☑ `docs/compatibility.md`, and the CLI surface is diffed against a committed baseline in CI |
| CLI and API/UI adapters invoke the same underlying operation | N/A — no second adapter yet |
| Benchmarks include end-to-end throughput and per-stage timings | ☐ One ad-hoc measurement (75.1 s DuckLake vs 23.2 s extraction) and no harness. Ticket 07 |

## §8 — Canonical Bronze

> *"Historical and live ingestion must share canonical Bronze
> transformations; compare all 21 table outputs before removing duplicate
> implementations."*

The comparison is done for the extraction tier and is reproducible:
[`ticket-04-discrepancies.md`](ticket-04-discrepancies.md) carries the
21-table matrix, [`ticket-04-progress.md`](ticket-04-progress.md) the method.
Eleven tables are equal, four carry defects, one is an intended semantic
difference, one is provenance-only, and `contract_code` is unexercised by any
fixture.

So the precondition for consolidation is satisfied — but the result is that
the duplicate implementations **cannot yet be collapsed**. A shared mapper
would have to pick a side on four open defects (`trades` units and coverage,
`config_settings` NULLs, the live call graph, `accounts.sequence_*`) and on
`created_at`'s two incompatible meanings. Those are rows 1–6 of the ticket's
follow-up list.

The storage tier is unverified for all 21 tables, and finding F6 shows why
that is not a formality: `envelope_size_bytes` differs at extraction and is
repaired at insert.

## §5 — the credential violation

> *"Avoid secrets in process arguments, logs, event payloads, or JSON inspect
> output."*

`--b2-key-secret`, `--pg-password` and `--ducklake-catalog` are accepted as
CLI arguments and are therefore readable by any local process via `ps`. This
predates the move into this repository (roadmap known gap 1) and is the only
item on the release gate that is an outright violation rather than an absence.

Environment-variable fallbacks would be additive and non-breaking, which
makes this the cheapest conformance win available and the one with the
clearest security argument. It is currently unticketed.

## §2 and §6 — layout

The standard's target layout is:

```
cmd/stellar-ingest/main.go   # composition root
internal/cli/                # flags, parsing, rendering only
internal/app/                # use cases: extract, load, validate, repair
internal/domain/             # value objects and invariant checks
internal/jobs/               # execution, status, checkpoint coordination
internal/events/             # typed events, subscriptions
internal/adapters/           # source/sink/remote integration
api/                         # optional versioned shared contracts
```

Today everything but `internal/bronze/restored` and `internal/parity` is one
flat `package main`. Movement so far has been toward this shape rather than
away from it — the Parquet worker boundary, the worker-independent mapping
entry point, the private restored-keys package — but the composition root,
the CLI layer and the use-case layer are not separated.

The honest sequencing: the layout is not the binding constraint. §8's
canonical-Bronze question is, and Ticket 06 is where the CLI surface changes
anyway. Moving files first would churn every path in the Ticket 04 harness
and the golden suite for no behavioural gain, against the adoption plan's
own "without behavioral changes" instruction. Fold the restructure into
Ticket 06, when the Cobra surface lands and the files have to move regardless.

## Gaps mapped to tickets

| Gap | § | Ticket |
|---|---|---|
| Four Bronze defects block a shared mapper | 8 | Ticket 04 follow-ups 1–4 |
| Storage tier unverified | 8, 10 | Ticket 04 follow-up 5 |
| Canonical row contract undecided | 8 | Ticket 04 follow-up 6 |
| Credentials in argv | 5, 10 | **Unticketed — should be, it is a live violation** |
| No subcommands, `--output json`, completion, examples | 3, 6, 9 | Ticket 06 |
| No stdout/stderr discipline, no `slog` | 3, 6 | Ticket 06 |
| Flat package layout | 2, 6 | Ticket 06 |
| No error-code taxonomy | 5, 10 | Ticket 06 |
| No job/event surface | 4 | Deferred — standard's adoption Phase 3, and a batch tool may not need one |
| No cancellation/signal test | 5, 10 | Ticket 02 remainder |
| No throughput/per-stage benchmark harness | 10 | Ticket 07 |
| Source contract tests | 8 | Ticket 05 |
