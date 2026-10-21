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
var SemVer = "0.1.2"

// BuildDate is when the binary was built, RFC 3339 in UTC (CLI-13, DATA-3).
//
// The Makefile sets it from the COMMIT's date, not from the wall clock and
// not from SOURCE_DATE_EPOCH, so two builds of one commit are byte-identical
// (BUILD-3). SOURCE_DATE_EPOCH is the usual convention and is deliberately
// not used: nix-shell exports it as 315532800 to pin file mtimes, which
// would stamp every developer build 1980-01-01. Override with BUILD_DATE=...
// when a release pipeline needs a different answer.
//
// Empty is a valid answer and omits the field. The flake build leaves it
// unset because a Nix store path carries no commit date, and inventing one
// would have `version` claim a precision the build does not have.
var BuildDate = ""
