# stellar-ingest — engineering profile

Against the *OBSRVR Engineering Standard* v2.0 (20 October 2026). The
template is §13's.

Supersedes the v1.0 gap analysis in [`cli-standard-adoption.md`](cli-standard-adoption.md)
and the conformance record in [`engineering-standard.md`](engineering-standard.md),
both of which track v1.0 and are kept for history. Where they disagree with
this file, this file is current.

## 1. System class and applicable sections

**CLI tool.** Universal §2, §7, §9, §12, plus §3, §4, §6, §8, §10, §11.
§5 does not apply: the tool exposes no network API.

## 2. Contracts owned

| Contract | Version | Owner |
|---|---|---|
| CLI flag surface | pinned in `docs/baseline/cli-help.txt`, diffed in CI | Tillman Mosley III |
| `version` output | `stellar-ingest.version/v1`; release line is SemVer | Tillman Mosley III |
| Release artifacts | tarball + SBOM + SHA256SUMS + SLSA provenance, per [`releasing.md`](releasing.md) | Tillman Mosley III |
| `--output json` / `ndjson` result | `stellar-ingest.run/v1` | Tillman Mosley III |
| Exit codes | CLI-15 table, [ADR 0001](adr/0001-align-exit-codes-with-cli-15.md) | Tillman Mosley III |
| Bronze Parquet schema (21 tables) | [`bronze-row-contract.md`](bronze-row-contract.md), [`bronze-storage-contract.md`](bronze-storage-contract.md) | Tillman Mosley III |
| Config file format | `docs/compatibility.md` | Tillman Mosley III |

## 3. Product-specific rules

- Stays a complete application: acquire → decode Bronze → validate →
  Parquet, DuckLake or Postgres → checkpoint and repair. Usable with no
  hosted platform (P-8).
- The flat flag form (`stellar-ingest --start …`, no verb) keeps working
  alongside the verbs. Deployed jobs pass about twenty flags with no verb,
  and breaking them to gain a noun is the dismantling the standard warns
  against for this tool. One flag table feeds both registrations so they
  cannot drift; a test asserts it.
- Memory is a published contract, not an implementation detail: peak is
  `workers × (batch-size × extracted-row bytes + prefetch-buffer × raw LCM
  bytes)`, and the README carries measured per-era figures.

## 4. Exceptions

| Rule | ADR | Expires |
|---|---|---|
| CLI-10 — credential flags still accepted | [0002](adr/0002-exception-credential-flags.md) | 2027-04-20 |

Reviewed quarterly (CONF-2). An expired exception is non-conformance.

## 5. Conformance status

Closed in the v2.0 conformance pass:

- **CLI-11** — SIGINT/SIGTERM now stop a run and exit 130/143. The
  orchestrator built its own root context, so the signal context died at the
  call site and extraction ignored cancellation entirely; measured against
  the live archive, a run survived 60 s of SIGINT and had to be killed.
  Now 0.2 s, with the checkpoint written, no manifest published, and the
  range claim given back so a rerun can take it immediately rather than
  waiting out a two-hour lease.
- **CLI-15** — exit codes realigned, codes 7 and 8 added. ADR 0001.
- **CONF-1 / DOC-4** — `docs/adr/` exists; the CLI-10 exposure is recorded
  with an expiry instead of standing unacknowledged.
- **SEC-9** — **zero reachable vulnerabilities**, down from 16. Seven closed
  by dependency bumps, including a SQL-injection in `pgx` reachable from the
  range catalog (`GO-2026-5004`); the remaining nine were standard-library
  advisories closed by the go1.26.9 toolchain pin. `govulncheck` is now a
  blocking CI gate.
- **BUILD-2** — toolchain pinned in both `flake.lock` and `go.mod`.
- **STL-1 / STL-3** — the Bronze views name CAP-67 in their schema
  metadata, and `token_transfers` marks reconstructed asset movements
  apart from observed ones
  ([ADR 0003](adr/0003-mark-reconstructed-asset-movements.md)).
- **SEC-10 / VER-12 (to SLSA L2)** — releases build in CI from a signed tag,
  with a CycloneDX SBOM and Sigstore-signed SLSA provenance, an attested
  image on ghcr, and notes taken from the changelog. Runbook in
  [`releasing.md`](releasing.md).
- **TEST-4, SEC-6, §9 merge gate (partly)** — CI now runs the race detector,
  a gofmt check and secret scanning. Two things the measurement decided:
  the race job is scoped to the concurrent tests, because the full module
  under `-race` takes 402 s on 16 cores and exceeds Go's 600 s per-package
  timeout on a 2-core runner — the extra time is golden and parity fixtures
  replaying, not concurrency. And committed chain fixtures are allowlisted
  by path in `.gitleaks.toml`; without that, 283 high-entropy strings in
  golden files trip the generic-api-key rule.
- **BUILD-3 / BUILD-4** — `-trimpath` on every build, OCI annotations on the
  image.
- **DOC-1** — `LICENSE` (Apache-2.0), `NOTICE`, `SECURITY.md`, `AGENTS.md`
  and `CHANGELOG.md` added.
- **SEC-12** — `SECURITY.md` names the reporting address, the response time,
  what is in scope, and the one accepted exposure with its expiry.
- **CLI-13 / VER-2** — `version` reports the release, the commit and the
  build date, with `--output json` carrying `stellar-ingest.version/v1`.
  First release is **v0.1.0**.
- **BUILD-3** — builds are byte-identical across runs: `-trimpath`, and a
  build date taken from the commit rather than the wall clock.

Strongest areas: §9 (real pubnet fixtures with recorded ranges, golden
suite, 21-table parity harness, CLI surface pinned in CI) and §2 (the layer
boundaries are enforced by a test, not by convention).

### Next gaps to close, in order

1. **Reconverge the flake toolchain.** `go.mod` pins go1.26.10 (for
   `GO-2026-6809`); the pinned nixpkgs carries 1.26.9 and sets
   `GOTOOLCHAIN=local`, so the flake build silently uses the older Go. CI
   warns. Bump `flake.lock` as soon as nixpkgs carries 1.26.10. Recurring by
   nature: every standard-library advisory needs a toolchain bump on both
   sides, and nixpkgs lags by days.
2. **`golangci-lint` in the merge gate** (§9). Measured: 47 issues today —
   41 `errcheck` (mostly unchecked `fmt.Fprintf` in tests) and 6
   `staticcheck`. Enabling it means fixing those first, which is unrelated
   churn and wants its own PR.
3. **§12 repo hygiene** — `CONTRIBUTING` is the remaining one. The repo is
   public, so DOC-1 asks for it.
4. **RHEL 9 cannot run the release tarball.** The floor is `libstdc++`
   (GLIBCXX 3.4.30, from bookworm's GCC 12), not glibc. Closing it means a
   GCC 11 build base or changing how duckdb-go links -- `-static-libstdc++`
   and `CGO_LDFLAGS` are both overridden by its cgo directives, verified
   with `ldd`. The container image and source builds are unaffected.
5. **SLSA Build Level 3** (SEC-10). The GitHub-native attestation puts
   releases at L2; L3 for public releases needs a hardened isolated builder.
   Also linux/amd64 only — the cgo DuckDB bundle makes an arm64 artifact a
   native-runner question, not a flag.
6. **DATA-6 / DATA-11** — the result envelope and lineage records carry no
   `network`; lineage carries no protocol version.
7. **TEST-3** — no fuzz tests, and the tool's primary input is XDR.
8. **CLI-9** — no `STELLAR_INGEST_*` environment layer and no XDG user
   config; precedence is flags › config file › defaults.
9. **CLI-12** — no `events --follow` verb (there is an `--events` flag) and
   no `cancel`.
10. **§10** — no justfile, and no `SOURCE_DATE_EPOCH` in release builds.
11. **CLI-22** — no control-character stripping for chain-derived text.
    Latent today: no command renders a memo or asset code to the terminal,
    and nothing pins that it stays that way.
