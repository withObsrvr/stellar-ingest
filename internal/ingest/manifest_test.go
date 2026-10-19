package ingest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteAttemptManifestUsesClosedParquetArtifacts(t *testing.T) {
	root := t.TempDir()
	writer := NewParquetTableWriter[ParquetLedger](root, "ledgers", 0)
	rows := []ParquetLedger{
		{Sequence: 10000, LedgerRange: 10000},
		{Sequence: 10001, LedgerRange: 10000},
	}
	if err := writer.Write(rows, func(row ParquetLedger) uint32 { return row.LedgerRange }); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "manifest.json")
	config := OrchestratorConfig{StartLedger: 10000, EndLedger: 10001, OnlyTables: map[string]bool{"ledgers": true}}
	if err := WriteAttemptManifest(path, "attempt-1", config, nil); err != nil {
		t.Fatal(err)
	}
	manifest, err := ReadAttemptManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	table := manifest.Tables["ledgers"]
	if table.Status != "success_nonzero" || table.Rows != 2 || len(table.Files) != 1 {
		t.Fatalf("unexpected ledger manifest: %+v", table)
	}
	if table.Files[0].Checksum == "" || table.Files[0].Bytes == 0 {
		t.Fatalf("manifest did not fingerprint parquet: %+v", table.Files[0])
	}
}

func TestWriteAttemptManifestRecordsSuccessfulZeroRows(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "manifest.json")
	config := OrchestratorConfig{StartLedger: 10000, EndLedger: 10001, OnlyTables: map[string]bool{"trades": true}}
	if err := WriteAttemptManifest(path, "attempt-1", config, nil); err != nil {
		t.Fatal(err)
	}
	manifest, err := ReadAttemptManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	table := manifest.Tables["trades"]
	if table.Status != "success_zero" || table.Rows != 0 || len(table.Files) != 0 {
		t.Fatalf("unexpected zero-row manifest: %+v", table)
	}
}

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

func TestParquetFileListSQLEscapesPaths(t *testing.T) {
	got := parquetFileListSQL([]string{"/tmp/a.parquet", "/tmp/it's.parquet"})
	if !strings.Contains(got, "'/tmp/a.parquet'") || !strings.Contains(got, "'/tmp/it''s.parquet'") {
		t.Fatalf("unexpected SQL list: %s", got)
	}
}

func TestCheckpointPersistsAttemptIdentityAndRotatesOnConfigChange(t *testing.T) {
	originalVersion := Version
	Version = "checkpoint-test-v1"
	t.Cleanup(func() { Version = originalVersion })

	root := t.TempDir()
	config := OrchestratorConfig{StartLedger: 10, EndLedger: 20, NumWorkers: 2, OutputDir: root, StorageType: "FS", Bucket: "/archive"}
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

	Version = "checkpoint-test-v2"
	versionChanged, err := NewCheckpointManager(root, OrchestratorConfig{StartLedger: 10, EndLedger: 20, NumWorkers: 2, OutputDir: root, StorageType: "FS", Bucket: "/archive"})
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
	Version = "checkpoint-test-v1"
	legacyRotated, err := NewCheckpointManager(root, OrchestratorConfig{StartLedger: 10, EndLedger: 20, NumWorkers: 2, OutputDir: root, StorageType: "FS", Bucket: "/archive"})
	if err != nil {
		t.Fatal(err)
	}
	if legacyRotated.AttemptID() == first.AttemptID() {
		t.Fatal("legacy checkpoint without pipeline_version reused the previous attempt ID")
	}
}
