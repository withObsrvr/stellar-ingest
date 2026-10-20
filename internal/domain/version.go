package domain

// Version is the build stamp, set at link time and recorded in every
// manifest, lineage row and parity record this package writes.
//
// It lives here rather than in the command because the engine is what stamps
// it. That moves the linker path with it: every build must pass
//
//	-X github.com/withObsrvr/stellar-ingest/internal/domain.Version=<sha>
//
// and a build that misses it stamps "dev" into the records instead of a
// commit, which is exactly the provenance gap the cold-authority work had to
// untangle. The Makefile, the Dockerfile and flake.nix all set it, and
// TestVersionIsStampedByTheBuild checks the built binary rather than trusting
// that they agree.
//
// It stays the COMMIT, not the release. Checkpoints compare it to decide
// whether an interrupted attempt may resume, and lineage rows carry it as
// pipeline_version; a release number is too coarse for either -- two builds
// of v0.1.0 can decode differently. SemVer below is the release, and the two
// are reported side by side rather than one replacing the other.
var Version = "dev"

// SemVer is the release this build belongs to (VER-2).
//
// Baked rather than linked, because it is a property of the source tree: the
// commit says which build, this says which release it is part of. A release
// build may override it at link time from the tag, which is what makes a
// tagged artifact say v0.1.0 and an untagged developer build say the version
// the tree is working towards.
var SemVer = "0.1.0"

// BuildDate is when the binary was built, RFC 3339 in UTC (CLI-13, DATA-3).
//
// Empty in a developer build, where there is no meaningful answer and a
// fabricated one would be worse than none: a reproducible release sets it
// from SOURCE_DATE_EPOCH, so the same commit gives the same timestamp.
var BuildDate = ""
