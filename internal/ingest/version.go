package ingest

// Version is the build stamp, set at link time and recorded in every
// manifest, lineage row and parity record this package writes.
//
// It lives here rather than in the command because the engine is what stamps
// it. That moves the linker path with it: every build must pass
//
//	-X github.com/withObsrvr/stellar-ingest/internal/ingest.Version=<sha>
//
// and a build that misses it stamps "dev" into the records instead of a
// commit, which is exactly the provenance gap the cold-authority work had to
// untangle. The Makefile, the Dockerfile and flake.nix all set it, and
// TestVersionIsStampedByTheBuild checks the built binary rather than trusting
// that they agree.
var Version = "dev"
