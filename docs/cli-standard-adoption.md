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

Against §6 (layout): **done for the layers that matter now.**

```
cmd/stellar-ingest/     composition root
internal/cli/           flags, resolution, rendering
internal/app/           the operations
internal/ingest/        the engine, importable
```

`internal/domain`, `internal/jobs` and `internal/adapters` are not split out
yet. `internal/events` has nothing to hold until Stage 6.

The split has been analysed rather than assumed, and a clean layering does
exist — but not the obvious one. Counting every cross-file reference in the
engine against a proposed assignment gives:

```
jobs      -> domain    34      the expected direction
adapters  -> jobs      28      a back-edge
jobs      -> adapters   7      the expected direction
adapters  -> domain     6      the expected direction
domain    -> jobs       1      a back-edge
domain    -> adapters   1      a back-edge
```

Three cycles, each caused by a value object sitting in the wrong file rather
than by genuine entanglement:

| back-edge | cause | where it belongs |
|---|---|---|
| `domain -> jobs` | `types.go` uses `LedgerRange`, defined in `range_state.go` | `LedgerRange` is a value object: domain |
| `domain -> adapters` | `table_selection.go` uses `mapToDuckLakeTable` from `ducklake.go` | the 21-table name map is pure data: domain |
| `adapters -> jobs` | `ducklake.go` and others use the attempt manifest | `AttemptManifest` is a value object: domain |

And one finding worth keeping: `jobs -> adapters` is the *correct* direction —
an orchestrator writes through a sink — so the first draft of the layering had
it backwards. `worker.go` needing the Parquet writer and the stream source is
not a cycle, it is the shape.

What makes this its own change rather than part of Stage 4: the 75 cross-layer
references are all lowercase today, because they are within one package.
Splitting means exporting most of them, which is a large API churn on top of
the 128 files Stage 4 already moved — and it would rewrite the same golden and
parity files whose `R100` rename status is the evidence that Stage 4 changed
nothing. Doing both at once would destroy that proof.

Two places this tool is **ahead** of the standard, worth not regressing:

- `docs/baseline/cli-help.txt` is a reviewed, CI-enforced record of the flag
  surface. A flag change that does not update it fails the build. The standard
  asks for compatibility regression tests; this is one, and it already works.
- Golden and parity suites pin extractor output per column, which is the §6
  testing pyramid's "golden JSON/NDJSON and exit-code tests" rung for the data
  rather than the CLI.

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
