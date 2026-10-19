package jobs

// These two reach into the orchestrator's own internals -- clearing a shard's
// incomplete files, and the checkpoint's identity rotation -- so they can
// only live here. They shared a file with the manifest tests, which need the
// Parquet writer's internals instead, and one file cannot be inside two
// packages.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

func TestClearIncompleteShardFilesIsScopedToWorker(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "bronze", "ledgers", "range_0000000")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "shard_0001_old.parquet")
	keep := filepath.Join(dir, "shard_0002_keep.parquet")
	for _, path := range []string{target, keep} {
		if err := os.WriteFile(path, []byte("fixture"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := clearIncompleteShardFiles(root, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("incomplete shard file was not removed: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("other shard file was removed: %v", err)
	}
}

func TestCheckpointPersistsAttemptIdentityAndRotatesOnConfigChange(t *testing.T) {
	originalVersion := domain.Version
	domain.Version = "checkpoint-test-v1"
	t.Cleanup(func() { domain.Version = originalVersion })

	root := t.TempDir()
	config := domain.OrchestratorConfig{StartLedger: 10, EndLedger: 20, NumWorkers: 2, OutputDir: root, StorageType: "FS", Bucket: "/archive"}
	first, err := NewCheckpointManager(root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Save(); err != nil {
		t.Fatal(err)
	}

	resumed, err := NewCheckpointManager(root, config)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.AttemptID() != first.AttemptID() {
		t.Fatalf("resume changed attempt ID: got %s, want %s", resumed.AttemptID(), first.AttemptID())
	}

	config.EndLedger++
	changed, err := NewCheckpointManager(root, config)
	if err != nil {
		t.Fatal(err)
	}
	if changed.AttemptID() == first.AttemptID() {
		t.Fatal("configuration change reused the previous attempt ID")
	}

	domain.Version = "checkpoint-test-v2"
	versionChanged, err := NewCheckpointManager(root, domain.OrchestratorConfig{StartLedger: 10, EndLedger: 20, NumWorkers: 2, OutputDir: root, StorageType: "FS", Bucket: "/archive"})
	if err != nil {
		t.Fatal(err)
	}
	if versionChanged.AttemptID() == first.AttemptID() {
		t.Fatal("pipeline version change reused the previous attempt ID")
	}

	checkpointPath := filepath.Join(root, "checkpoint.json")
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := strings.Replace(string(data), "  \"pipeline_version\": \"checkpoint-test-v1\",\n", "", 1)
	if legacy == string(data) {
		t.Fatal("test fixture did not remove pipeline_version")
	}
	if err := os.WriteFile(checkpointPath, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	domain.Version = "checkpoint-test-v1"
	legacyRotated, err := NewCheckpointManager(root, domain.OrchestratorConfig{StartLedger: 10, EndLedger: 20, NumWorkers: 2, OutputDir: root, StorageType: "FS", Bucket: "/archive"})
	if err != nil {
		t.Fatal(err)
	}
	if legacyRotated.AttemptID() == first.AttemptID() {
		t.Fatal("legacy checkpoint without pipeline_version reused the previous attempt ID")
	}
}
