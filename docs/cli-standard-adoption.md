# Adopting the Obsrvr CLI Engineering Standard

Gap analysis of `stellar-ingest` against *Quality CLI Engineering Standard
v1.0* (14 October 2026), and the staged plan to close it.

The standard's own adoption plan names this repository as the Phase 1
retrofit, **"without behavioral changes"**, and says of this tool
specifically: *"Refactor internals, do not dismantle working end-to-end
workflows."* Both constraints are load-bearing here, for a concrete reason
given at the end.

## Where the tool stands

Measured, not estimated.

| Standard §3 requirement | Today |
|---|---|
| Resource nouns and verbs | **No.** One flat command, 45 flags, no subcommands |
| Stable IDs, `--context`/`--network` targeting | Partial. `--era-id` and `--network-passphrase` exist; no `--context` |
| `--output`, `--config`, `--timeout`, `--context`, `--quiet`, `--verbose`, `--no-color` | `--output` now selects the format; the other five are missing |
| stdout results / stderr diagnostics | Partial. Progress and logs are on stderr; the banner and summaries are on stdout |
| Human tables default, `--output json`, `--output ndjson` | Flag accepts all three; only `table` is implemented — Stage 3 |
| Noninteractive, `--yes` for destructive ops | Already noninteractive; nothing prompts |
| `--` passthrough, stdin `-` | Not applicable yet; nothing is forwarded |
| `--help` with examples, completion, `version`, `doctor` | `--help` lists flags with no examples; no completion, no `--version`, no `doctor` |
| Documented nonzero exit classes | **No.** Every failure is `log.Fatal`, so exit 1 for everything |

Against §4 (resources, jobs, events): there is no job ID, no `inspect`, no
`events --follow`, no `wait --for`, no `cancel`. A run is a process; its state
is its stdout.

Against §5 (reliability): better than the surface suggests. `context.Context`
is threaded, DuckLake pushes retry on commit conflict, pushes are idempotent
by ledger range, checkpoints are written after the sink commits, and the range
catalog makes a multi-day backfill resumable. What is missing is **structured
errors with stable codes**, and `classifyFlowctlFailure` is the obvious seed
for them — it already classifies failures, but only for flowctl reporting.

Against §5 security: **closed.** All four credentials now resolve from the
environment or a file, the flags warn, and the warning never echoes a value.
See *Done* below.

Against §6 (layout): **done.**

```
cmd/stellar-ingest/   composition root and surface dispatch
internal/cli/         flags, resolution, Cobra commands
internal/app/         the operations
internal/domain/      value objects, configuration, validation
internal/adapters/    sources and sinks: Parquet, DuckLake, hot buffer
internal/jobs/        orchestration: worker, checkpoint, chunks, lineage
internal/fixtures/    the committed fixtures, shared by every layer's tests
internal/credential/  where a secret came from
internal/status/      OSC 7501 program status
```

The dependency direction is now enforced by the compiler rather than by
intention:

```
domain    -> status
adapters  -> domain
jobs      -> adapters, domain, parity, status
```

`internal/events` is still empty, because it has nothing to hold until
Stage 6 gives jobs an event stream.

Three cycles had to be broken first, each caused by a value object sitting in
the wrong file rather than by real entanglement. `LedgerRange` and
`RangeState` lived in the range catalog, `OrchestratorConfig` in the
orchestrator, `LineageRecord` in the lineage writer, `BatchData` and
`PrefetchedLedger` in the worker, `DefaultPrefetchBuffer` in the worker, and
the DuckLake table-name map in the pusher. All of them are shapes two layers
agree on, so all of them are domain.

Two findings the compiler produced that no amount of reading would have:

- `jobs -> adapters` is the **correct** direction. The first draft of this
  layering had it backwards, on the assumption that a sink is lower than an
  orchestrator. A worker writes through a writer; that is the shape.
- `BatchData` is not a pure value object. It has a method, `MergeLedger`,
  which was defined in `worker.go` -- and a type cannot have methods outside
  its own package, so the split made that visible immediately.

`XDRStreamSource` moved out of `worker.go` as well. A source living in the
orchestration layer was invisible while everything shared a package; once an
adapters test could not reach it without importing jobs, it was obvious.

### What it cost, honestly

Eleven symbols needed exporting, not the seventy-five estimated before the
work. That earlier figure counted cross-layer *references*; the distinct
symbols were forty-seven, and thirty-six were already exported.

The real cost was test placement. A Go test can only reach a package's
unexported internals from inside that package, so each of the forty-four test
files had to go where its subject lives -- and three files tested internals of
two packages at once and had to be split. The shared fixture helpers could not
move at all, because a test helper cannot cross a package boundary either;
they became `internal/fixtures`, which is a normal package rather than a test
file so that any layer can import it.

`testdata` moved to the repository root, above all three layers, because all
three read the same eight fixtures.

## The `--output` collision — resolved

`--output` meant **the output directory**, and the deployed Nomad jobs pass
`--output /data/output-validation`. The standard reserves `--output` for the
response format.

**Decided: rename the directory flag to `--output-dir` and take `--output` for
the format**, with the old meaning accepted for one release. This is the only
option that ends with the standard satisfied and every Obsrvr tool agreeing,
and the deprecation window costs one release.

The transition is resolved by *value* rather than by a flag day, because every
running job passes a path today:

```
--output table|json|ndjson   a format, the new meaning
--output <anything else>     a directory, the old meaning, with a warning
```

Deterministic and documented rather than clever. The only input it gets wrong
is an output directory named exactly `table`, `json` or `ndjson`, which fails
immediately with `--output-dir` unset rather than writing somewhere
surprising. Two different directories, one to each flag, is an error rather
than a guess.

The whole rule lives in `resolveOutput`, away from the flag parsing, so it can
be read and tested on its own — it is the part that either breaks a running
job or writes Parquet into a directory called "json".

One sequencing note: the validation Nomad job still passes `--output`, on
purpose. Its pinned image predates `--output-dir`, so switching the job first
would fail on an unknown flag. The job moves when the next image lands; until
then it takes the deprecated path and logs the warning.

## Done

- **Program status (OSC 7501).** A run reports `working` with progress and a
  terminal `done` or `error`, on stderr, so a terminal or supervising tool
  follows the program's own account of its state instead of scraping output.
  `--program-status=auto|always|never`, auto meaning only when stderr is a
  terminal. This is the standard's first principle — one implementation
  serving a command line and a UI — at the smallest useful scale.
- **Progress moved to stderr**, per §3's I/O row. It was on stdout, which
  competes with results in a pipeline.
- **`--output` freed for the response format**, directory moved to
  `--output-dir`, old meaning accepted for one release. See above.
- **Stage 2: secrets out of argv.** `DUCKLAKE_CATALOG_DSN`, `B2_KEY_ID`,
  `B2_KEY_SECRET` and `PG_PASSWORD`, each with a `_FILE` form that wins over
  the plain variable. The flags still work and warn, naming the flag and the
  variables but never the value. A named-but-unreadable or empty file is an
  error rather than a fallthrough, so a bad secret mount fails where it is
  wrong instead of becoming an authentication failure somewhere further away.

  Worth recording why this mattered beyond `ps`: the loader re-invokes itself
  per chunk, so a secret on the parent's command line was on every child's
  too. The environment route fixes the children for free, because a child
  inherits an environment rather than an argument list.

  Checked rather than assumed: pgx already redacts the DSN from its dial
  errors (`user=user database=db`), and the flowctl metadata payload carries
  no credential, so argv really was the only exposure. `RedactDSN` exists for
  any future diagnostic that wants to print one.

  This entry was lost when the Stage 2 change merged: it was anchored on text
  that existed only on the `--output` branch, so the edit silently did
  nothing and `main` recorded the gate as satisfied without saying how. Added
  back here.

## Staged plan

Each stage is independently shippable and leaves the tool working.

**Stage 1 — conformance that breaks nothing.** `--version`; `--quiet` and
`--verbose` over `slog`; `--no-color`; documented exit classes built on
`classifyFlowctlFailure`; `doctor` as a flag-compatible mode switch
(`--doctor`) that checks credentials, extensions, catalog reachability and
schema agreement; examples in `--help`. All additive. The CLI baseline records
each flag as it lands.

**Stage 3 — machine-readable output.** A typed run summary rendered as a human
table by default and as JSON on request, with a versioned schema and golden
tests. The flag already accepts `json` and `ndjson`; this is what makes them
mean something.

**Stage 4 — the composition root.** `cmd/stellar-ingest/main.go` plus
`internal/app` holding extract/load/validate/repair as callable operations
with typed requests and responses. Mechanical and large: it moves code without
changing it, and it is what makes a UI or an API possible without shelling
out, per §2.

**Stage 5 — done.** `extract`, `validate`, `load`, `repair`, `run` and
`version`, each calling an operation in `internal/app` and owning no
ingestion logic. The flat flag form is retained and warns once. Completion
comes from Cobra.

The two surfaces are parsed by different libraries on purpose: pflag reads a
single dash followed by several characters as a cluster of shorthands, so
`-output-dir x` — which the standard library accepts and which `--help` has
always rendered — would fail under it. One flag table feeds both
registrations, and three tests keep them from drifting: same names, same
defaults, same resulting request from the same arguments.

Subcommands are presets over `RunRequest` rather than separate pipelines.
Narrowing each verb's accepted flags is deliberately not done: deciding which
of 45 flags belongs to which verb is a compatibility decision, and getting it
wrong silently ignores a flag a deployed job passes.

**Stage 6 — jobs and events.** Operation IDs, the `queued → running →
succeeded | failed | cancelled` lifecycle, `inspect`, `events --follow` as
NDJSON, `wait --for`. Worth doing when something other than a person is
driving the tool; §11 puts it at Phase 3 for the same reason.

## Sequencing against the backfill

One scheduling point that matters more than the order above.

The artifact validated end to end — compact identity, views, partitioning,
UTC-pinned sessions, range state, concurrency — is the **flat-flag loader**,
exercised through a Nomad job that passes about twenty flags. Stages 4 and 5
restructure the surface that validation ran through, and would invalidate it.

So: Stages 1 to 3 are safe to take at any time, being additive. Stages 4 and 5
belong either well before a backfill, with a re-validation round after them, or
after the backfill has run. Taking them *during* the window where a validated
image is waiting to be used is the one sequencing that buys nothing and risks
the thing that already works.

## Release gate, §10

Current status, so a reader can see what a PR here still has to earn.

| Gate | Status |
|---|---|
| Core logic testable without terminal or network | Partial — extractors yes, the run path no |
| `--help`, `--version`, completion, examples reviewed | No — only `--help`, without examples |
| JSON and NDJSON covered by golden tests | No — no JSON output yet |
| stdout/stderr separation; noninteractive verified | Partial — progress fixed, results still mixed |
| Cancellation, timeout, retries, interrupted runs exercised | Partial — retries and resume are tested; no timeout flag |
| No secrets in arguments, diagnostics, logs | Yes — environment and file routes, flags warn |
| Resource IDs, event types, states, error codes documented | No — Stage 6 |
| Compatibility and migration documented for flag/config/schema changes | Yes — `docs/compatibility.md` and the CLI baseline |
| CLI and API/UI adapters invoke the same operation | Not applicable yet — Stage 4 creates the operation |
| Benchmarks include end-to-end throughput and per-stage timings | Partial — throughput is reported, per-stage is not |
