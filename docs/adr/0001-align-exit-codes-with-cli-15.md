# 1. Align exit codes with CLI-15

- Status: accepted
- Date: 2026-10-20
- Owner: Tillman Mosley III
- Affected interfaces: process exit codes (a contract under VER-3)
- Supersedes: the table in `docs/compatibility.md`

## Context

`internal/exit` was written against *Quality CLI Engineering Standard* v1.0,
which asked for "documented nonzero classes" without fixing the integers.
The OBSRVR Engineering Standard v2.0 fixes them in CLI-15, and two of ours
collide with it:

| Code | stellar-ingest (v1.0 era) | CLI-15 (v2.0) |
|---|---|---|
| 5 | timeout | conflict / failed precondition |
| 6 | cancelled | timed out waiting |
| 7 | — | dependency unavailable |
| 8 | — | data validation failed |
| 130 / 143 | — | interrupted after a clean shutdown |

A collision is worse than a gap. A scheduler written against v2.0 reads our
`6` as "timed out waiting, retry" when we mean "somebody stopped this on
purpose", and reads our `5` as "resolve the conflict first" when we mean a
deadline elapsed.

The missing code that costs money is **8**. A `validate` failure returned an
unclassified error, so it fell through to `1`. CLI-15 marks `1` retryable
and `8` "No; investigate" — so a scheduler retried a genuine data-quality
failure indefinitely, re-reading the same ledgers to reach the same answer.
That is exactly the runaway `internal/exit` was created to prevent, reached
by a different route.

## Decision

Adopt the CLI-15 table verbatim:

| Code | Class | Retry? |
|---|---|---|
| 0 | ok | — |
| 1 | internal | Depends |
| 2 | usage | No |
| 3 | not-found | No |
| 4 | permission | No |
| 5 | conflict | After resolving |
| 6 | timeout | Yes |
| 7 | unavailable | Yes, with backoff |
| 8 | data-validation | No; investigate |
| 130 / 143 | cancelled | Yes, resumes from checkpoint |

Three consequences worth stating, because each is a decision rather than a
transcription:

1. **Cancellation leaves the 0–8 range.** CLI-15 puts an interrupted run at
   128+n. `signal.NotifyContext` discards which signal arrived, so the
   composition root now installs its own handler and attaches the signal to
   the error; `exit.Code` reads it. 130 means a person pressed Ctrl-C, 143
   means a supervisor reclaimed the allocation, and a scheduler treats those
   differently.

2. **Code 7 is kept narrow.** It is the one class where retrying the
   identical invocation is correct, so only an unambiguous unreachable
   dependency qualifies: a refused connection, an unresolvable host, a
   network timeout. Classifying every storage-client error as `7` would turn
   a permanent misconfiguration into an infinite retry loop.

3. **Class 1 keeps its historical meaning.** Anything matching specifically
   on 1 still sees 1 for a genuine internal failure.

## Classification (VER-3)

**Breaking.** Codes 5 and 6 change meaning and cancellation moves from 6 to
130/143.

## Migration

Known consumers were checked rather than assumed, as they were for the
original change:

- The chunk runner tests `err != nil` from `exec.Cmd.Run` and never reads the
  code. Unaffected.
- Nomad treats any nonzero as a failed allocation. Unaffected.
- A dispatch loop matching on `5` or `6` specifically would change behaviour.
  No such matching exists in this repository; anything outside it must be
  checked before upgrading.

Because the tool is pre-1.0, this ships as a minor bump with the break called
out in `CHANGELOG.md` (VER-2, VER-11), not as a major.

## Rollback

Revert this commit. The table is one `const` block, one `String` method and
one `Code` function; nothing persists an exit code.

## Tests

- `TestCodesMatchTheStandardTable` pins every integer against the standard's
  table, so a reordered `const` block cannot silently change a retry policy.
- `TestSignalBeatsClass` covers 130, 143 and a wrapped signal.
- `TestUnreachableIsNarrow` covers both directions of the code 7 boundary.
