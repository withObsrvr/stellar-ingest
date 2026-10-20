# Agent instructions

For any agent or human working in this repository. Required by DOC-1 of the
OBSRVR Engineering Standard; see [`docs/engineering-profile.md`](docs/engineering-profile.md)
for which rules apply here and what is still open.

## What this is

`stellar-ingest` acquires Stellar ledgers, decodes them into 21 Bronze
tables, validates the output and publishes it to Parquet, DuckLake or
PostgreSQL. It is a complete application: it works with no hosted platform
behind it, and must stay that way (P-8).

## Getting to a passing test suite

```sh
nix develop          # or: direnv allow
make build           # both executable names
go test ./...        # hermetic: no network, no credentials
```

Fixtures are committed, so the suite needs no cloud access. Anything that
does need credentials is in a test that skips without them.

## Rules that are load-bearing

These are not style preferences. Each one has a test that fails if you break
it, and each exists because breaking it cost something.

1. **Do not dismantle the flat flag form.** `stellar-ingest --start …` with
   no verb is what deployed jobs use; the verbs are additive. One flag table
   in `internal/cli/flags.go` feeds both registrations, and
   `TestBothRegistrationsRegisterTheSameFlags` fails if they drift. Add a
   flag in one place, not two.

2. **Respect the layering.** `domain → adapters → jobs → app → cli`, enforced
   by `internal/architecture/layers_test.go` against an allowlist. A new
   package starts with no import permissions and has to be given them there,
   on purpose.

3. **The CLI surface is pinned.** `make parity-baseline` diffs `--help`
   against `docs/baseline/cli-help.txt`. An intentional flag change updates
   that file in the same commit.

4. **Exit codes are a contract.** The CLI-15 table in `internal/exit`, with
   the integers pinned by `TestCodesMatchTheStandardTable`. A scheduler's
   retry policy is written against them; changing one is a Breaking change
   needing an ADR.

5. **Output contracts are versioned and golden-tested.** The result envelope
   is `stellar-ingest.run/v1`. Adding a field is additive; changing what one
   means is Breaking, even when the wire format is unchanged (VER-4).

6. **Never put a secret in argv.** Credentials resolve from `NAME` or
   `NAME_FILE` in the environment. The legacy flags are deprecated under a
   dated exception ([ADR 0002](docs/adr/0002-exception-credential-flags.md));
   do not add new ones.

7. **Tests use real chain data** (TEST-1). Fixtures under `testdata/fixtures`
   are captured pubnet ledgers with their ranges recorded. Invented data is
   for injecting failures only, never for showing a feature works. A bug fix
   adds a test that fails without it, and for a data bug that means the
   ledger that exposed it (TEST-5).

## Memory

Peak is `workers × (batch-size × extracted-row bytes + prefetch-buffer × raw
LCM bytes)`. Ledger weight varies by era — 412 B at genesis, ~32 MB of
extracted rows at the Soroban end — so one setting does not fit the whole
chain. The README carries measured figures. A change that increases
per-worker buffering needs a number, not an assumption.

## Decisions

Anything that changes a contract, the architecture or the security posture
is an ADR in `docs/adr/`, MADR format, never edited after acceptance
(DOC-4). Excepting a rule in the standard is also an ADR, with an owner and
an expiry date no more than six months out (CONF-1).

## Commits

Conventional Commits where it fits. Say *why*, not *what* — the diff already
says what. If a change is a decision, the commit links the ADR.
