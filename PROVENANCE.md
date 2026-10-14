# Provenance

`stellar-ingest` was bootstrapped by promoting the existing
`stellar-history-loader` application in its entirety, per the decision recorded
in *Stellar Ingest & Raw Ledger Origin — Developer implementation handoff*
(14 October 2026), TICKET 01.

## Source

| Field | Value |
|---|---|
| Source repository | `github.com/withObsrvr/ttp-processor-demo` |
| Source path | `obsrvr-lake/stellar-history-loader/go` |
| Source commit | `bd618bb4706e983eb305bc43bb5fc90c9f6c6fb5` |
| Source commit date | 2026-10-14 07:41:11 -0400 |
| Source commit subject | Stellar history loader updates (#106) |
| Source working tree | clean at time of copy (`git status --porcelain` empty for the path) |
| Original module path | `github.com/withObsrvr/stellar-history-loader` |
| Files copied | 50 (46 `.go`, `go.mod`, `go.sum`, `v3_bronze_schema.sql`) |
| Lines of Go copied | 13,427 |

Git history was **not** grafted. The move is a single bootstrap commit, and
per-file blame for work predating this repository lives in `ttp-processor-demo`
at the commit pinned above. Eighteen commits touch the source path there,
the earliest dated 2026-03-28 (`5edbea12`).

To read the pre-migration history of any file:

```sh
git -C <path-to>/ttp-processor-demo log --follow \
  -- obsrvr-lake/stellar-history-loader/go/<file>.go
```

## Changes made during the move

The bootstrap is deliberately behaviour-preserving. Exactly three things
changed, none of them in program logic:

1. **Module path** — `github.com/withObsrvr/stellar-history-loader` →
   `github.com/withObsrvr/stellar-ingest`. This is the only edit to a `.go`
   or `.mod` file. No Go source imported the old module path (the program is a
   single flat `package main`), so no import rewriting was required.

2. **Module moved from `go/` to the repository root.** In `ttp-processor-demo`
   the module lived in a `go/` subdirectory because the repository held many
   services. As a standalone repository, a root module is required for
   `github.com/withObsrvr/stellar-ingest` to be resolvable by `go get` — which
   matters because the handoff anticipates other repositories importing the
   Bronze packages once they are factored out (open decision (b)). Package
   structure inside the module is untouched.

3. **Build scaffolding** — `Makefile`, `Dockerfile`, `.gitignore`, CI workflow,
   `README.md`, and `docs/` are new or rewritten for the standalone repository.

### Explicitly *not* changed

- Package layout: still one flat `package main`. Decomposition is TICKET 03.
- CLI flags, defaults, parsing, and validation order.
- Console output, including the `=== Stellar History Loader ===` banner.
- Exit codes.
- Environment variable names and semantics.
- Bronze schemas, extractor behaviour, and `v3_bronze_schema.sql`.

## Verified parity

Measured against a binary built from the pinned source commit:

| Check | Result |
|---|---|
| `go build` | exit 0, both before and after |
| `go test ./...` | pass, both before and after |
| `--help` output | byte-identical after normalising the program-name line |
| Exit code, no flags | 1 before, 1 after |
| Exit code, `--start 1` | 1 before, 1 after |
| Exit code, `--start 1 --end 2` | 1 before, 1 after |

The normalised `--help` output is committed at `docs/baseline/cli-help.txt` and
is enforced in CI by `make parity-baseline`. `go vet` is clean, `gofmt -l`
reports nothing, and `go mod tidy` produced no change to `go.mod` or `go.sum`
beyond the module line.

The Docker image was **not** built — no Docker daemon was available on the
bootstrap host. The CI workflow builds it and smoke-tests both executable
names.

### What this parity evidence does *not* cover

No ledger data was processed as part of this bootstrap. Output-level parity —
Parquet bytes, DuckLake rows, PostgreSQL rows over real ledger ranges — is
TICKET 02 and is not yet established. See `docs/phase0-fixtures.md`.
