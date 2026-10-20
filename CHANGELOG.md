# Changelog

Format: [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).
Versioning: [SemVer 2.0.0](https://semver.org/spec/v2.0.0.html). The tool is
pre-1.0, so a minor bump may break (VER-2).

## [Unreleased]

### Fixed

- **SIGINT and SIGTERM now stop a run.** The orchestrator created its own
  root context, so the signal context the composition root threads through
  the application layer died at the call site and every worker got a context
  nothing could cancel. Extraction — the long part of any backfill — ignored
  cancellation until the range finished. Measured against the live archive:
  a run survived 60 seconds of SIGINT and had to be killed, losing the
  checkpoint and holding the range claim until its lease expired. It now
  stops in under half a second, writes its checkpoint and publishes no
  manifest.
- A run stopped by its caller is reported as a cancellation rather than as
  "N worker(s) failed", so a scheduler does not retry a deliberate shutdown.
- A cancelled run no longer publishes an attempt manifest. The manifest is
  the claim that an attempt is complete, and a resumed backfill reads it as
  one.

### Changed

- **BREAKING: exit codes are realigned with CLI-15** of the OBSRVR
  Engineering Standard v2.0. See
  [ADR 0001](docs/adr/0001-align-exit-codes-with-cli-15.md).

  | Code | Was | Is now |
  |---|---|---|
  | 5 | timeout | conflict / failed precondition |
  | 6 | cancelled | timed out waiting |
  | 7 | — | dependency unavailable |
  | 8 | — | data validation failed |
  | 130 / 143 | — | interrupted (SIGINT / SIGTERM) |

  Codes 0–4 are unchanged, and class 1 keeps its historical meaning of
  "something went wrong". A dispatch loop matching specifically on `5` or
  `6` must be updated. The chunk runner and Nomad both read only
  "nonzero" and are unaffected.

- A `validate` failure now exits **8** instead of **1**. The standard marks
  1 as retryable and 8 as "No; investigate": a quality failure is the one
  outcome where re-running the identical command is guaranteed to spend the
  same money and reach the same answer.

- An unreachable dependency — refused connection, unresolvable host,
  network timeout — now exits **7**, which a scheduler may retry with
  backoff. The detection is deliberately narrow; anything broader would turn
  a permanent misconfiguration into an infinite retry loop.

### Security

- Seven reachable vulnerabilities closed by dependency bumps (SEC-9):
  `pgx` v5.9.1 → v5.9.2 (**SQL injection via placeholder confusion with
  dollar-quoted string literals**, `GO-2026-5004`, reachable from the range
  catalog's `Load`), `golang.org/x/net` → v0.60.0, `golang.org/x/text` →
  v0.42.0, `google.golang.org/grpc` → v1.83.1, `go.opentelemetry.io/otel/sdk`
  → v1.45.0.

  Nine reachable vulnerabilities remain, all in the Go standard library and
  all fixed in go1.26.9. The flake pins an older toolchain, so that bump and
  the blocking `govulncheck` CI gate go together in a follow-up.

### Added

- CI now runs the **race detector** over the concurrent code in a dedicated
  parallel job (TEST-4 — the Makefile target existed and nothing called it),
  a **gofmt** check, and
  **secret scanning** with gitleaks (SEC-6). `.gitleaks.toml` allowlists
  `testdata/` by path: committed chain fixtures are public ledger data, and
  283 high-entropy strings in golden files otherwise trip the
  generic-api-key rule.
- `-trimpath` on every build, so the same commit produces the same bytes
  regardless of where it was built (BUILD-3). OCI annotations on the image,
  so a running container names the commit it came from (BUILD-4).
- `AGENTS.md` (DOC-1).
- `docs/adr/`, with the two decision records this release required (DOC-4).
- `docs/engineering-profile.md`, the §13 profile, carrying the contract
  list, the one recorded exception and the ordered gap list.
- This changelog (VER-11).

### Deprecated

- `--b2-key-id`, `--b2-key-secret`, `--pg-password` and `--ducklake-catalog`
  as flag values. Use the environment variable or its `_FILE` form. The
  flags are accepted until **2027-04-20** under a recorded exception
  ([ADR 0002](docs/adr/0002-exception-credential-flags.md)); they are
  removed in the first release after that date.
