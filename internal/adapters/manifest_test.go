package adapters

import (
	"github.com/withObsrvr/stellar-ingest/internal/domain"
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
	config := domain.OrchestratorConfig{StartLedger: 10000, EndLedger: 10001, OnlyTables: map[string]bool{"ledgers": true}}
	if err := domain.WriteAttemptManifest(path, "attempt-1", config, nil); err != nil {
		t.Fatal(err)
	}
	manifest, err := domain.ReadAttemptManifest(path)
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
	config := domain.OrchestratorConfig{StartLedger: 10000, EndLedger: 10001, OnlyTables: map[string]bool{"trades": true}}
	if err := domain.WriteAttemptManifest(path, "attempt-1", config, nil); err != nil {
		t.Fatal(err)
	}
	manifest, err := domain.ReadAttemptManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	table := manifest.Tables["trades"]
	if table.Status != "success_zero" || table.Rows != 0 || len(table.Files) != 0 {
		t.Fatalf("unexpected zero-row manifest: %+v", table)
	}
}

func TestParquetFileListSQLEscapesPaths(t *testing.T) {
	got := parquetFileListSQL([]string{"/tmp/a.parquet", "/tmp/it's.parquet"})
	if !strings.Contains(got, "'/tmp/a.parquet'") || !strings.Contains(got, "'/tmp/it''s.parquet'") {
		t.Fatalf("unexpected SQL list: %s", got)
	}
}
