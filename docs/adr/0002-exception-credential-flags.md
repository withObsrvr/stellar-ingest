# 2. Exception: credential flags remain accepted until 2027-04-20

- Status: accepted
- Date: 2026-10-20
- Owner: Tillman Mosley III
- Rule excepted: **CLI-10** — "Secrets MUST NOT be accepted as argument
  values, because process arguments are visible to every user on the host."
- Expires: **2027-04-20** (six months, the CONF-1 maximum)

## Context

`--b2-key-id`, `--b2-key-secret`, `--pg-password` and `--ducklake-catalog`
accept credentials as command-line arguments. Anything in `argv` is readable
by any local process through `ps`, recorded in shell history, and inherited
by children — which for this tool means every chunk child.

The supported path already exists and is the documented one: each credential
resolves from `NAME` or `NAME_FILE` in the environment, and passing the flag
prints a warning naming the environment variable to use instead.

Removing the flags outright is the conforming fix. It is not taken yet for
one reason: deployed Nomad job files pass them, and this repository cannot
see those files. Removing them before the jobs are migrated turns a warned
deprecation into a failed mainnet allocation.

## Decision

Keep accepting the flags, with the warning, until **2027-04-20**. Remove them
in the first release after that date, as a Breaking change under VER-3 with
its own ADR.

## Risk

A credential passed as a flag on a shared host is readable by any local user
for the lifetime of the process. The exposure is unchanged from today; this
exception records it rather than creating it.

## Compensating controls

- Environment and `_FILE` resolution is implemented and documented, so no
  deployment needs the flags.
- Every flag use prints a warning to stderr naming the replacement.
- `doctor` reports where each credential was resolved from, so an operator
  can confirm a migration landed before the flags go.
- Resolved secrets never reach logs, events, `--output json` or the result
  envelope.

## Exit criteria

1. Every deployed job file sets the environment variable or its `_FILE` form.
2. `doctor` on each deployment reports no credential resolved from a flag.
3. The flags are removed, with an ADR and a changelog entry.

Tracked in the repo profile (`docs/engineering-profile.md`, §13) and reviewed
quarterly per CONF-2.
