# Changelog

Format: [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).
Versioning: [SemVer 2.0.0](https://semver.org/spec/v2.0.0.html). The tool is
pre-1.0, so a minor bump may break (VER-2).

## [Unreleased]

### Added

- **Reconstructed asset movements are now marked as such** (STL-3).
  `bronze.token_transfers_stream_v1` gains `event_provenance`:
  `cap67_observed` for protocol 23 and later, where CAP-67 events are read
  from ledger metadata, and `derived_from_operations` before that, where the
  SDK reconstructs them from operations, results and ledger-entry changes.

  The table has always carried both and nothing told them apart — measured
  on the committed fixtures, 7,822 reconstructed rows at protocol 19 against
  11,736 observed at protocol 29. A consumer querying across the protocol 23
  boundary got a mix with no way to separate them.

  Computed in the view, not stored (storage contract rule 2): it is a pure
  function of the ledger's protocol version, so storing it would add a
  column to every row of the largest stream in the lake to repeat what the
  ledgers table already knows. A ledger row that has not arrived yields
  `unknown` rather than being claimed as observed.

- **The views name the spec they implement** (STL-1). `COMMENT ON VIEW`
  records CAP-67 Unified Asset Events (Final, protocol 23) on both
  `token_transfers_stream_v1` and `contract_events_stream_v1`, so it travels
  with the lake instead of living only in prose. See
  [ADR 0003](docs/adr/0003-mark-reconstructed-asset-movements.md).

### Fixed

- `splitSQLStatements` was not string-literal aware, so a semicolon inside a
  `COMMENT ON ... IS '...'` cut the statement mid-literal and the whole
  schema file failed to apply. It already handled semicolons in comments for
  the same reason; literals are the other place prose appears in a schema
  file.

- **The flowctl integration is now tested against a stub control plane.**
  Previously the only coverage was a pure-function check of failure
  classification; that the component registers at all, registers as a
  SOURCE, reports each extraction phase, and classifies failures was true
  by reading the code and asserted nowhere. The stub implements the three
  RPCs the reporter uses and embeds `UnimplementedControlPlaneServer`, so a
  fourth would fail loudly rather than go unnoticed.

  This also pins the documented fail-closed behaviour: with
  `ENABLE_FLOWCTL=true` and an unreachable control plane, a run stops
  before doing any work. That is a real operational trade and should be a
  decision somebody made, not something rediscovered by running the binary.

### Changed

- flowctl bumped v0.0.10 → v0.0.12. The `ControlPlaneServer` interface is
  byte-identical between the two, so nothing in the integration changed.

## [0.1.2] - 2026-10-21

Makes the released tarball actually runnable on the distributions most
people deploy on. No contract changed.

### Fixed

- **The portable build failed in CI** with `error obtaining VCS status`.
  Go stamps VCS data by running git, and git refuses a repository owned by
  another user — which is every CI checkout seen from inside the build
  container, though not a local rootless one, so it passed locally and
  failed there. `-buildvcs=false`; nothing is lost, since the commit and
  build date are stamped explicitly through LDFLAGS.
- **The SBOM step could not write its output.** The portable build runs in
  a container, and under docker that container is root, so `dist/` came out
  root-owned and the SBOM action — which runs as the runner user — got
  `EACCES` creating a file there. The release set is now assembled into a
  directory the runner owns, which also keeps the published files explicit
  rather than implied by globs over a build directory.
- **The image job published without the artifacts.** It ran in parallel
  with `binaries`, so when the artifact build failed for v0.1.2, ghcr got
  `:0.1.2` and `:latest` for a release that did not exist — no tarball, no
  SBOM, no GitHub Release. The image now waits on the artifacts. A registry
  tag is the most public thing the workflow writes; it should be the last.

- **The v0.1.1 tarball required GLIBC 2.38** and so did not run on Debian
  12, Ubuntu 22.04 or Amazon Linux 2023 — the floor was whatever GitHub's
  `ubuntu-latest` happened to be. Release binaries are now built inside
  `golang:1.26-bookworm`, pinned by digest, putting the glibc floor at
  **2.34**. A gate runs the artifact on the oldest supported base, so this
  cannot regress quietly.

  Verified by running it: Debian 12 and 13, Ubuntu 22.04 and 24.04, and
  Amazon Linux 2023 all work. **RHEL 9 and its rebuilds still do not** —
  that floor is `libstdc++` (GLIBCXX 3.4.30), not glibc, and static-linking
  the C++ runtime is overridden by duckdb-go's own cgo directives. Carried
  as a known gap in [`docs/releasing.md`](docs/releasing.md); those users
  can build from source or use the container image.

  Not solved with Nix, though it was considered: a Nix-built binary
  hard-codes an interpreter path into the store and will not execute
  anywhere else at all. Nix stays the reproducibility story; the container
  is the distribution one. Reasoning in [`docs/releasing.md`](docs/releasing.md).

- **The release workflow rejected its own signed tag.** `actions/checkout`
  fetches a tag push as `+<sha>:refs/tags/<name>`, recreating it locally as
  a lightweight ref — the annotation object, and with it the signature, is
  never fetched — so `git cat-file -t` reported `commit` for a tag that is
  annotated and signed on the remote. The check now asks the API what the
  tag actually is.

## [0.1.1] - 2026-10-21

The release machinery itself, and the first advisory it caught. No contract
changed: the CLI surface, exit codes, result envelope and Bronze schema are
identical to 0.1.0, which is why this is a patch.

### Fixed

- **The release SBOM was empty.** The first dry run produced a well-formed
  CycloneDX document listing **zero components**: `syft` was pointed at
  `dist/`, which by then held only a `.tar.gz` it does not open. It now
  scans the binary, where Go build info names every module compiled in —
  118 components instead of 0 — and a gate fails the release if the count
  is zero. An empty SBOM is valid CycloneDX and passes every schema check,
  so nothing else would have caught it.

### Security

- **Go toolchain pinned to 1.26.10**, closing `GO-2026-6809` in `crypto/x509`
  — published after the 1.26.9 pin and caught by the `govulncheck` gate on
  its first day of existence, which is the gate working as designed.

  The pinned nixpkgs still carries 1.26.9, and nixpkgs sets
  `GOTOOLCHAIN=local`, so the flake build ignores the `toolchain` directive
  and quietly builds with the older Go. Released artifacts come from
  `make dist` under `setup-go` and are unaffected, but `nix build` output
  diverges until nixpkgs catches up. CI now warns when the two disagree
  rather than letting it pass unnoticed.

### Added

- **A release pipeline** (SEC-10, VER-12). A signed tag now builds artifacts
  in CI, attaches a CycloneDX SBOM and SLSA build provenance signed through
  Sigstore, pushes an attested image to `ghcr.io/withobsrvr/stellar-ingest`,
  and opens a GitHub Release whose notes are this file's section for that
  version. `make dist` builds the same artifacts locally, so what CI
  published can be reproduced without reading workflow YAML.
- `make release-check` refuses to build a release whose tag disagrees with
  `internal/domain.SemVer`. Tagging v0.2.0 while the source says 0.1.0 would
  otherwise ship a binary reporting a release it is not, into every manifest
  and lineage row downstream.
- [`docs/releasing.md`](docs/releasing.md), the release runbook (DOC-6),
  including how to rehearse the pipeline without publishing.

### Changed

- **Go toolchain pinned to 1.26.9** (BUILD-2), via `flake.lock` and a
  `toolchain` directive in `go.mod`. DuckDB is unchanged at v1.5.5.
- **`govulncheck` is now a blocking CI gate** (SEC-9). It was held back
  because the old toolchain carried nine reachable standard-library
  advisories — net/http, net/textproto, crypto/tls — and a permanently red
  gate teaches people to ignore red gates. **Reachable vulnerabilities: 0**,
  down from 16 at the start of this work.

## [0.1.0] - 2026-10-20

First tagged release. The tool predates it by some months; this is the point
at which its contracts — the CLI surface, the exit codes, the result
envelope, the Bronze schema — became things with a version number attached
rather than whatever `main` happened to be.

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
- **A stopped run now gives its range claim back.** `Claim` only takes over a
  `freezing` row once `DefaultClaimLease` has expired, so a Ctrl-C used to
  leave every range it was working blocked from redispatch for two hours —
  making SIGINT stop promptly was worth little while the ranges stayed
  locked. `RangeStateStore.Release` deletes the row, guarded so a run can
  only release its own claim and a frozen range can never be released. Emits
  `range.released`.

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

### Fixed

- **The release SBOM was empty.** The first dry run produced a well-formed
  CycloneDX document listing **zero components**: `syft` was pointed at
  `dist/`, which by then held only a `.tar.gz` it does not open. It now
  scans the binary, where Go build info names every module compiled in —
  118 components instead of 0 — and a gate fails the release if the count
  is zero. An empty SBOM is valid CycloneDX and passes every schema check,
  so nothing else would have caught it.

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
- **`LICENSE` (Apache-2.0)** and `NOTICE`. The project had no licence file,
  which left every consumer without permission to use it (DOC-1).
- **`SECURITY.md`** (SEC-12), naming `security@withobsrvr.com`, a
  three-business-day acknowledgement, what is in scope — decoding
  attacker-influenced ledger input is the interesting part — and the one
  accepted exposure with its expiry date.
- **`version` now reports a release, a commit and a build date**, with
  `--output json` carrying the `stellar-ingest.version/v1` schema
  (CLI-13, CLI-6, VER-2). The first release is **v0.1.0**.

  `domain.Version` keeps its meaning: it is the **commit**, and manifests,
  lineage rows and checkpoints go on recording it, because two builds of one
  release can decode differently. The release number is reported beside it,
  not instead of it. The flat `--version` is unchanged — it prints the
  commit, which is the contract `make version-stamp` checks.
- Builds are byte-identical across runs (BUILD-3): `-trimpath`, plus a build
  date taken from the commit rather than the wall clock. Not
  `SOURCE_DATE_EPOCH`, despite the convention — nix-shell exports it as
  1980-01-01 to pin file mtimes, which would stamp every developer build
  with a date sixteen years before Stellar existed.
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

[unreleased]: https://github.com/withObsrvr/stellar-ingest/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/withObsrvr/stellar-ingest/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/withObsrvr/stellar-ingest/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/withObsrvr/stellar-ingest/releases/tag/v0.1.0
