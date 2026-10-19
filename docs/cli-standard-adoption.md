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
| `--output`, `--config`, `--timeout`, `--context`, `--quiet`, `--verbose`, `--no-color` | **None of them**, and `--output` is taken — see below |
| stdout results / stderr diagnostics | Partial. Progress and logs are on stderr; the banner and summaries are on stdout |
| Human tables default, `--output json`, `--output ndjson` | **No machine-readable output at all** |
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

Against §6 (layout): the standard's layout is `cmd/`, `internal/cli`,
`internal/app`, `internal/domain`, `internal/jobs`, `internal/events`,
`internal/adapters`. Today 69 files sit in `package main` at the root, with
`internal/bronze/restored`, `internal/parity` and `internal/status` carved out.

Two places this tool is **ahead** of the standard, worth not regressing:

- `docs/baseline/cli-help.txt` is a reviewed, CI-enforced record of the flag
  surface. A flag change that does not update it fails the build. The standard
  asks for compatibility regression tests; this is one, and it already works.
- Golden and parity suites pin extractor output per column, which is the §6
  testing pyramid's "golden JSON/NDJSON and exit-code tests" rung for the data
  rather than the CLI.

## The `--output` collision

`--output` means **the output directory** in this tool, and the running Nomad
jobs pass `--output /data/output-validation`. The standard reserves `--output`
for the response format (`--output json`).

They cannot both be `--output`. Three ways out:

1. **Rename the directory flag** to `--output-dir`, accept `--output` as a
   deprecated alias for one release, then take `--output` for format. Honours
   the standard; needs a deprecation window, which §5 explicitly allows.
2. **Keep `--output` as the directory** and use `--format json`. Deviates from
   the standard in a visible way, and every other Obsrvr tool would have to
   deviate identically or be inconsistent.
3. **Let the subcommand grammar settle it** — `stellar-ingest extract --out
   ./bronze --output json` — since the flat form is retained for
   compatibility anyway.

**Recommendation: 1.** It is the only option that ends with the standard
satisfied and all tools agreeing, the deprecation window costs one release,
and the alias means no deployment breaks on the day it lands. This is a
decision for the maintainer, not an implementation detail, which is why it is
recorded here rather than resolved quietly.

## Done

- **Program status (OSC 7501).** A run reports `working` with progress and a
  terminal `done` or `error`, on stderr, so a terminal or supervising tool
  follows the program's own account of its state instead of scraping output.
  `--program-status=auto|always|never`, auto meaning only when stderr is a
  terminal. This is the standard's first principle — one implementation
  serving a command line and a UI — at the smallest useful scale.
- **Progress moved to stderr**, per §3's I/O row. It was on stdout, which
  competes with results in a pipeline.

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
tests. Resolves the `--output` question above, because it is the first thing
that needs the flag.

**Stage 4 — the composition root.** `cmd/stellar-ingest/main.go` plus
`internal/app` holding extract/load/validate/repair as callable operations
with typed requests and responses. Mechanical and large: it moves code without
changing it, and it is what makes a UI or an API possible without shelling
out, per §2.

**Stage 5 — subcommands over the same operations.** `extract`, `validate`,
`load`, `run` as the standard's §9 vocabulary, with the flat flag form
retained as a deprecated path so existing jobs keep working. Completion falls
out of Cobra once this lands.

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
