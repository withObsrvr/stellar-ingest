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

Unchanged. All 41 flags keep their names, types, defaults, validation order,
and error messages. `docs/baseline/cli-help.txt` records the surface and
`make parity-baseline` fails CI on any drift — including drift from an
unrelated change. An intentional flag change must update that file in the same
commit, which makes every CLI contract change visible in review.

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
