# Security policy

## Reporting a vulnerability

Email **security@withobsrvr.com**. You will get an acknowledgement within
**three business days**.

Please include enough to reproduce it: the command or input, the version
(`stellar-ingest version`), and what you expected instead. If you have a
proof of concept, say so rather than attaching it to the first mail, and we
will agree a channel.

Please do not open a public issue for a vulnerability, and please do not test
against Obsrvr's hosted infrastructure or the validator fleet — a local run
against a public archive reproduces almost everything this tool does.

## Scope

This repository is `stellar-ingest`: ledger acquisition, Bronze decoding,
validation, and publication to Parquet, DuckLake or PostgreSQL.

Especially interesting:

- **Decoding attacker-influenced input.** Everything this tool reads is
  written by somebody else — ledger XDR, transaction memos, asset codes,
  contract symbols and event data. A crafted ledger that causes a panic, an
  out-of-bounds read, unbounded memory growth, or output that escapes its
  encoding is in scope.
- **Terminal escape injection.** Chain-derived text reaching a terminal
  without its control characters stripped (CLI-22). The tool does not
  currently render memos or asset codes to a terminal, so this is latent
  rather than live — a path that makes it live is a finding.
- **Credential handling.** Credentials resolve from the environment or a
  `_FILE` path. A credential reaching `argv`, a log line, an event payload,
  the result envelope or `doctor` output is a finding. Note the known,
  recorded exception below.
- **Data integrity.** A path that lets a run publish a manifest for an
  incomplete attempt, skip ledgers silently, or stamp a lineage row that does
  not match what was written.

Out of scope: vulnerabilities in Stellar Core or the Stellar Go SDK, which
should go to the Stellar Development Foundation; and findings that require
an attacker who already has the host credentials.

## Known accepted exposure

`--b2-key-id`, `--b2-key-secret`, `--pg-password` and `--ducklake-catalog`
still accept credentials as command-line arguments, which are readable by any
local process through `ps`. This is a recorded exception with an expiry date
of **2027-04-20** — see
[ADR 0002](docs/adr/0002-exception-credential-flags.md). Use the environment
variable or its `_FILE` form instead; the flags warn when used.

Reports of this specific exposure are welcome but already known.

## Supported versions

Pre-1.0: fixes land on `main` and in the next release. There are no
maintained release branches yet.

## Disclosure

We will agree a disclosure date with you. Fixes ship with an advisory naming
the affected versions, and the changelog records them under **Security**.
