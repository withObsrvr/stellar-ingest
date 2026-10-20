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
  Now 0.2 s, with the checkpoint written and no manifest published.
- **CLI-15** — exit codes realigned, codes 7 and 8 added. ADR 0001.
- **CONF-1 / DOC-4** — `docs/adr/` exists; the CLI-10 exposure is recorded
  with an expiry instead of standing unacknowledged.
- **SEC-9 (partly)** — seven reachable vulnerabilities closed by dependency
  bumps, including a SQL-injection in `pgx` reachable from the range
  catalog (`GO-2026-5004`). Nine remain, all in the Go standard library and
  all fixed in go1.26.9; see gap 1 below.
- **TEST-4, SEC-6, §9 merge gate (partly)** — CI now runs the race detector,
  a gofmt check and secret scanning. Committed chain fixtures are allowlisted
  by path in `.gitleaks.toml`; without that, 283 high-entropy strings in
  golden files trip the generic-api-key rule.
- **BUILD-3 / BUILD-4** — `-trimpath` on every build, OCI annotations on the
  image.
- **DOC-1 (partly)** — `AGENTS.md` and `CHANGELOG.md` added.

Strongest areas: §9 (real pubnet fixtures with recorded ranges, golden
suite, 21-table parity harness, CLI surface pinned in CI) and §2 (the layer
boundaries are enforced by a test, not by convention).

### Next gaps to close, in order

1. **Toolchain bump, and then the govulncheck gate** (SEC-9, BUILD-2). Nine
   reachable vulnerabilities remain, all in the Go standard library and all
   fixed in **go1.26.9**. The flake pins an older Go, so the bump means
   updating `flake.lock` — which can cascade into the cgo DuckDB build and
   so belongs in its own change. Once the toolchain is current, add
   `toolchain` to `go.mod` and make `govulncheck` a blocking CI gate. These
   are one piece of work, not three.
2. **`golangci-lint` in the merge gate** (§9). Measured: 47 issues today —
   41 `errcheck` (mostly unchecked `fmt.Fprintf` in tests) and 6
   `staticcheck`. Enabling it means fixing those first, which is unrelated
   churn and wants its own PR.
3. **§12 repo hygiene** — no `LICENSE`, `SECURITY.md`, `CONTRIBUTING`.
   `LICENSE` and the `SECURITY.md` contact address are decisions, not
   drafting work.
4. **CLI-13 / VER-2** — `version` prints a bare commit. No SemVer, no build
   date, no `--output json`. There are no git tags, so the starting version
   is a decision.
5. **DATA-6 / DATA-11** — the result envelope and lineage records carry no
   `network`; lineage carries no protocol version.
6. **TEST-3** — no fuzz tests, and the tool's primary input is XDR.
7. **CLI-9** — no `STELLAR_INGEST_*` environment layer and no XDG user
   config; precedence is flags › config file › defaults.
8. **CLI-12** — no `events --follow` verb (there is an `--events` flag) and
   no `cancel`.
9. **§10** — no justfile, and no `SOURCE_DATE_EPOCH` in release builds.
10. **STL-1 / STL-3** — no CAP or SEP named in schema metadata; records
    reconstructed from operations and effects are not marked as derived.
11. **CLI-22** — no control-character stripping for chain-derived text.
    Latent today: no command renders a memo or asset code to the terminal,
    and nothing pins that it stays that way.
