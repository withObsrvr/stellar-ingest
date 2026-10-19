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
| Resource nouns and verbs | **Yes.** `extract`, `validate`, `load`, `repair`, `run`, `doctor`, `inspect`, `wait`, `version`, `completion` |
| Stable IDs, `--context`/`--network` targeting | **Yes.** `--context` selects a config context, `--network mainnet\|testnet\|futurenet` sets the passphrase, `--era-id` partitions |
| `--output`, `--config`, `--timeout`, `--context`, `--quiet`, `--verbose`, `--no-color` | **All seven present** |
| stdout results / stderr diagnostics | **Yes.** Only the rendered result, `--version` and `--plan` reach stdout; all narration goes through `internal/logging` to stderr |
| Human tables default, `--output json`, `--output ndjson` | **Yes**, over a versioned `stellar-ingest.run/v1` result, with golden files for all three |
| Noninteractive, `--yes` for destructive ops | Already noninteractive; nothing prompts, so `--yes` has nothing to guard |
| `--` passthrough, stdin `-` | `-` is accepted by `--events`; nothing else forwards arguments, so `--` has nothing to separate |
| `--help` with examples, completion, `version`, `doctor` | **Yes.** Root and every verb carry worked examples; completion for bash/zsh/fish; `version`; `doctor` |
| Documented nonzero exit classes | **Yes.** `internal/exit`: 0 ok, 1 internal, 2 usage, 3 not-found, 4 permission, 5 timeout, 6 cancelled |

Against §4 (resources, jobs, events): every run carries a job ID, which is
the same value the range catalog stores as `attempt_id`, so an event stream
and the durable record join on one identifier. `--events` writes an
append-only NDJSON stream with a sequence cursor. `inspect` reports each
range as `queued | running | succeeded | failed`, and `wait --for
succeeded|settled` blocks on it.

Two things from §4 are deliberately **not** implemented, and naming them is
better than implying otherwise:

- **`events --follow` across processes.** Following a live run's stream from
  another process needs somewhere both can see it. That is a daemon or
  another table, and §2 says not to require a daemon because another product
  has one.
- **`cancel` across processes.** Same reason. A run is cancelled with SIGINT
  or SIGTERM, which it handles: the context reaches every blocking layer, so
  the allocation unwinds its defers instead of being killed mid-push.

`cancelled` therefore never appears as a lifecycle state. Nothing records a
deliberate stop — a cancelled run's claim expires and the range is
re-derived, which is safe because the push deletes before inserting.

## Staged plan

Each stage is independently shippable and leaves the tool working.

**Stage 1 — done.** `--version`; `--quiet`, `--verbose` and `--no-color`
over `slog`; `--timeout`; documented exit classes in `internal/exit`;
`doctor`, both as a verb and as the flag-compatible `--doctor`; examples in
`--help` on the root and every verb.

Writing the exit classes forced the thing they depended on: `app.Run` called
`log.Fatal` 39 times, so every failure was exit 1 and none of it was
testable. Converting those to returned errors is what made the 20 input
rules assertable — and the first assertion found two of them enforced after
the work rather than before. `--ducklake` checked its credentials AFTER
extraction, so a run missing `B2_KEY_SECRET` extracted a whole range to
Parquet and then discovered it could not push.

**Stage 3 — done.** `app.RunResult` is a typed, versioned result —
`stellar-ingest.run/v1` — rendered as a table, JSON or NDJSON by
`internal/cli`, with golden files for each. `Validator.RunAll` and
`RunChunkedCommand` had to start returning their counts rather than only
narrating them; "2 passed, 1 failed" as prose cannot be read by anything but
a person.

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

**Stage 6 — done, with two stated gaps.** Job IDs, the `queued → running →
succeeded | failed` lifecycle derived from the range catalog, `inspect`,
`wait --for`, and an append-only NDJSON event stream via `--events`.
`events --follow` and cross-process `cancel` are not implemented; see the §4
note above for why.

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
| Core logic testable without terminal or network | **Yes.** `app.Run` returns errors instead of calling `log.Fatal`; its 20 input rules, the doctor and the lifecycle mapping all run in-process |
| `--help`, `--version`, completion, examples reviewed | **Yes.** Root and every verb have examples; completion verified for bash, zsh and fish |
| JSON and NDJSON covered by golden tests | **Yes.** `testdata/golden/render/`, five fixtures across three formats |
| stdout/stderr separation; noninteractive verified | **Yes.** Asserted in `internal/app/streams_test.go`; nothing prompts |
| Cancellation, timeout, retries, interrupted runs exercised | **Yes.** SIGINT/SIGTERM cancel the context; `--timeout` bounds every operation; the freeze window deliberately survives cancellation and is tested from both sides |
| No secrets in arguments, diagnostics, logs | **Yes.** Environment and file routes; flags warn; the doctor is driven with known secrets and all three of its output surfaces are grepped for them |
| Resource IDs, event types, states, error codes documented | **Yes.** Job IDs join the event stream to the catalog; event types are constants; the lifecycle and the exit classes are both documented here and in code |
| Compatibility and migration documented for flag/config/schema changes | **Yes.** `docs/compatibility.md`, including the exit-code change |
| CLI and API/UI adapters invoke the same operation | **Yes.** Both surfaces call `internal/app`; `app.Deps` supplies streams so an API or UI can call the same operation with its own |
| Benchmarks include end-to-end throughput and per-stage timings | **Partial.** The result carries per-phase `duration_ms` for extract, push and hot buffer, plus throughput. There is no committed benchmark suite |

## What is still open

One row above is partial and one thing is deliberately absent:

- **No committed benchmark suite.** Per-stage timings are now in the result,
  which is the input a benchmark needs, but nothing asserts on them over
  time.
- **Per-verb flag narrowing.** Every verb still accepts all 50-odd flags.
  Deciding which belongs to which is a compatibility decision, and getting
  it wrong silently ignores a flag a deployed job passes. The verbs stay
  presets over `RunRequest` until each has a reason to reject one.
