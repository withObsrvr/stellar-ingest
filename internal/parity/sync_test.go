package parity

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// liveParityPackageDir is where the live ingester's copy of this package
// lives, relative to this repository.
//
// A relative path across repositories is fragile, which is why the test skips
// rather than fails when it is absent: a CI checkout of stellar-ingest alone
// has nothing to compare against, and failing there would make the suite
// depend on a sibling checkout. It still guards the case that matters — a
// developer with both trees who edits one copy.
const liveParityPackageDir = "../../../ttp-processor-demo/obsrvr-lake/stellar-postgres-ingester/go/internal/parity"

// syncedFiles must be byte-identical in both repositories.
//
// compare.go is deliberately not in the list: only this side runs the
// comparison, and copying it would be dead code in the live tree.
var syncedFiles = []string{"canonical.go", "keys.go", "record.go"}

// TestCanonicalCopyInSync is the drift guard for the duplicated canonical
// form.
//
// The two codebases pin different versions of stellar-extract — v0.1.2 here,
// v0.1.5 there — so they cannot be linked into one binary and cannot share a
// package the ordinary way. Each canonicalizes in its own process over its
// own copy of these files. If the copies drift, every digest on one side
// shifts and the difference reads as a data discrepancy rather than as the
// harness bug it is. CanonicalFormVersion catches a deliberate, versioned
// change; this test catches an undeclared one.
func TestCanonicalCopyInSync(t *testing.T) {
	if _, err := os.Stat(liveParityPackageDir); err != nil {
		t.Skipf("live ingester checkout not present at %s; nothing to compare", liveParityPackageDir)
	}

	for _, name := range syncedFiles {
		t.Run(name, func(t *testing.T) {
			local := hashFile(t, name)
			remote := hashFile(t, filepath.Join(liveParityPackageDir, name))
			if local != remote {
				t.Fatalf("internal/parity/%s differs between the two repositories.\n"+
					"  this repo: %s\n"+
					"  live repo: %s\n"+
					"Copy the authoritative version across; the parity digests are "+
					"only comparable while these files are byte-identical.",
					name, local, remote)
			}
		})
	}
}

// TestLiveCopyHasNoExtraFiles catches a file added on one side only — a
// helper added to the live copy would compile there and break the next sync.
func TestLiveCopyHasNoExtraFiles(t *testing.T) {
	if _, err := os.Stat(liveParityPackageDir); err != nil {
		t.Skipf("live ingester checkout not present at %s", liveParityPackageDir)
	}

	entries, err := os.ReadDir(liveParityPackageDir)
	if err != nil {
		t.Fatalf("read live parity package: %v", err)
	}
	allowed := map[string]bool{}
	for _, f := range syncedFiles {
		allowed[f] = true
	}
	for _, e := range entries {
		if !allowed[e.Name()] {
			t.Errorf("live copy has %s, which is not one of the synced files %v", e.Name(), syncedFiles)
		}
	}
}

func hashFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
