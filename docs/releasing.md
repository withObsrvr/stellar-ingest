# Cutting a release

A runbook (DOC-6). Everything below is `make` or `git`; the workflow calls
the same targets, so a release can be reproduced locally.

## Before the tag

1. **Decide the number.** SemVer (VER-2). Pre-1.0, so a minor bump may break;
   after 1.0.0, only a major may. Classify every contract change in the
   release against VER-3's table first — a Semantic change (same wire format,
   different meaning) counts as breaking even when nothing fails to compile.

2. **Set it in the source.** `SemVer` in `internal/domain/version.go`. This is
   the fact; the tag is the assertion, and `make release-check` refuses to
   build artifacts when they disagree.

3. **Write the changelog section.** Rename `## [Unreleased]` to
   `## [X.Y.Z] - YYYY-MM-DD` and add the comparison links at the bottom. The
   release notes are extracted from this section, so an empty one fails the
   publish job rather than shipping a release with no notes.

4. **Check the gates locally**, which is cheaper than finding out in CI:

   ```sh
   make ci-build          # build, vet, tests, golden, CLI surface
   make dist              # the artifacts themselves
   govulncheck ./...      # must be zero reachable (SEC-9)
   ```

## The tag

Annotated and signed — VER-12, and the release workflow refuses anything
else:

```sh
git tag -s vX.Y.Z -m "stellar-ingest vX.Y.Z"
git tag -v vX.Y.Z          # must print a Good signature
git push origin vX.Y.Z
```

Signing uses SSH, configured per-repo:

```sh
git config --local gpg.format ssh
git config --local user.signingkey ~/.ssh/id_ed25519.pub
git config --local tag.gpgsign true
git config --local gpg.ssh.allowedSignersFile ~/.config/git/allowed_signers
```

For GitHub to show the tag as **Verified** rather than merely signed, the
same public key must be registered at github.com/settings/keys as a
**Signing Key** — a separate entry from the authentication key. Without it
the release still builds; the workflow warns.

## What the tag triggers

`.github/workflows/release.yml`:

| Job | Does |
| --- | --- |
| `verify` | Tag is annotated and signed; `SemVer` matches the tag |
| `binaries` | `make dist`, CycloneDX SBOM, SLSA provenance via Sigstore |
| `image` | Builds and pushes `ghcr.io/withobsrvr/stellar-ingest`, attests the digest |
| `publish` | GitHub Release with the tarball, SBOM, `SHA256SUMS` and the changelog section |

## Verifying a release

What a consumer runs, and therefore what to check once before announcing:

```sh
gh attestation verify stellar-ingest_X.Y.Z_linux_amd64.tar.gz -R withObsrvr/stellar-ingest
sha256sum -c SHA256SUMS
```

## Rehearsing without releasing

The workflow takes `workflow_dispatch` with `dry_run` (default true):
everything builds, the SBOM and attestations are produced, nothing is
published.

```sh
gh workflow run release.yml -f dry_run=true
```

## Known gaps

- **linux/amd64 only.** The binary is cgo and bundles DuckDB, so
  cross-compiling is not a flag change. An arm64 artifact needs a native
  runner in the matrix.
- **Docker Hub is not published to.** ghcr needs no stored credential; a
  mirror would need one (SEC-1 wants it scoped and rotatable).
- **SLSA Build Level.** The GitHub-native attestation puts this at L2.
  SEC-10 wants L3 for public releases, which needs a hardened, isolated
  builder.
