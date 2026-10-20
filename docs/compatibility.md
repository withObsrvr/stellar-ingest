# Compatibility and migration

## Executable names

`stellar-ingest` is the primary executable. `stellar-history-loader` is built
from the same package and is byte-for-byte the same program; it exists so
existing operational scripts and deployment manifests keep working unchanged
during the migration.

```sh
make build-all   # produces bin/stellar-ingest and bin/stellar-history-loader
```

The Docker image installs both on `PATH`, with `stellar-ingest` as the
entrypoint:

```sh
docker run --rm withobsrvr/stellar-ingest:latest --start ... --end ...
docker run --rm --entrypoint stellar-history-loader withobsrvr/stellar-ingest:latest --start ...
```

The legacy name carries no deprecation warning yet. Adding one would change
stderr output and break the Phase 1 "equivalent output" exit gate, so it waits
until that gate is signed off.

## Flag compatibility

**Additive only.** Every pre-existing flag keeps its name, type, default,
validation order and error message. The standard-conformance work added ten:
`--config`, `--context`, `--network`, `--events`, `--for`, `--doctor`,
`--quiet`, `--verbose`, `--no-color`, `--timeout`. The baseline diff for that
change is 20 insertions and 0 deletions, which is the machine-checkable form
of that claim.

`docs/baseline/cli-help.txt` records the surface and `make parity-baseline`
fails CI on any drift — including drift from an unrelated change. An
intentional flag change must update that file in the same commit, which makes
every CLI contract change visible in review.

## Exit codes — CHANGED

Previously every failure was `log.Fatal`, so every failure was exit **1**.
Failures now carry a documented class:

| Code | Class |
|---|---|
| 0 | success |
| 1 | internal failure |
| 2 | invalid input |
| 3 | missing resource |
| 4 | permission |
| 5 | conflict / failed precondition |
| 6 | timed out waiting |
| 7 | dependency unavailable |
| 8 | data validation failed |
| 130 / 143 | interrupted (SIGINT / SIGTERM) |

**Superseded for codes 5 and up.** The table above is the current one,
aligned with CLI-15 of the OBSRVR Engineering Standard v2.0. It replaces an
earlier table in which `5` meant timeout and `6` meant cancelled; see
[ADR 0001](adr/0001-align-exit-codes-with-cli-15.md).

**What this breaks:** nothing we can find, and the two consumers were
checked rather than assumed. The chunk runner tests `err != nil` from
`exec.Cmd.Run` and never reads the code. Nomad treats any nonzero as a
failed allocation. Class 1 deliberately keeps the historical meaning of
"something went wrong", so anything matching specifically on 1 still sees 1
for a genuine internal failure.

**What it gains:** a scheduler can tell "retry this" from "stop and ask
someone". Retrying invalid input forever is how a broken flag becomes a
quota bill.

## Output streams — CHANGED

The banner, phase headers and summaries used to go to **stdout**. They now
go to stderr along with all other narration, and stdout carries only the
rendered result, `--plan` output, or `--version`.

**What this breaks:** anything parsing this tool's stdout for prose. We know
of nothing that does; the Nomad jobs capture both streams and the validation
checks read the catalog, not the logs.

**What it gains:** `--output json | jq` works, which it could not before.

A related fix: the progress line previously wrote its text to stderr and its
terminating newline to stdout, so consecutive progress lines ran together on
stderr. That is visible in the v4 mainnet logs and is now one write.

## Environment compatibility

Unchanged. See `docs/phase0-inventory.md` §3. Note in particular that
`CAPTIVE_CORE` is configured **entirely** through environment variables with no
flag coverage, so any future config-file interface has to reach them.

## What has not changed, deliberately

- Console output, including the `=== Stellar History Loader ===` banner. It is
  wrong for the new name, but changing it breaks output parity. It changes when
  TICKET 06 introduces the subcommand hierarchy and the output contract is
  revised as a whole.
- Exit codes.
- Package structure — still one flat `package main` (TICKET 03).
- Bronze schemas and `v3_bronze_schema.sql`.

## Rollback

The bootstrap is a copy; nothing was removed from `ttp-processor-demo`.
`obsrvr-lake/stellar-history-loader` remains buildable and deployable at commit
`bd618bb4` and later. Rolling back means continuing to build from there — no
revert in this repository is required, and no production route changes as a
result of this bootstrap.

The two trees will diverge from the first change made here. Until
`ttp-processor-demo`'s copy is retired, any fix applied in one must be applied
to the other, and that duplication is itself a reason to keep the transition
short.
