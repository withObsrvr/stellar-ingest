package ingest

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestDuckLakeManifestIngestion is opt-in because it requires a PostgreSQL
// catalog and locally installed DuckDB extensions. It exercises the real
// DuckLakePusher, including manifest verification and INSERT ingestion.
func TestDuckLakeManifestIngestion(t *testing.T) {
	catalog := os.Getenv("DUCKLAKE_INGESTION_TEST_POSTGRES")
	extensions := os.Getenv("DUCKLAKE_INGESTION_TEST_EXTENSION_DIR")
	dataPath := os.Getenv("DUCKLAKE_INGESTION_TEST_DATA_PATH")
	if catalog == "" || extensions == "" || dataPath == "" {
		t.Skip("set DUCKLAKE_INGESTION_TEST_POSTGRES, DUCKLAKE_INGESTION_TEST_EXTENSION_DIR, and DUCKLAKE_INGESTION_TEST_DATA_PATH")
	}
	keyID := os.Getenv("DUCKLAKE_INGESTION_TEST_S3_KEY_ID")
	keySecret := os.Getenv("DUCKLAKE_INGESTION_TEST_S3_KEY_SECRET")
	endpoint := os.Getenv("DUCKLAKE_INGESTION_TEST_S3_ENDPOINT")
	region := os.Getenv("DUCKLAKE_INGESTION_TEST_S3_REGION")
	if keyID == "" {
		keyID = "integration-test"
	}
	if keySecret == "" {
		keySecret = "integration-test"
	}
	if endpoint == "" {
		endpoint = "localhost:9000"
	}
	if region == "" {
		region = "us-east-1"
	}
	rowCount := 2
	if value := os.Getenv("DUCKLAKE_INGESTION_TEST_ROWS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			t.Fatalf("invalid DUCKLAKE_INGESTION_TEST_ROWS %q", value)
		}
		rowCount = parsed
	}
	const startLedger uint32 = 10000
	endLedger := startLedger + uint32(rowCount) - 1

	root := t.TempDir()
	writer := NewParquetTableWriter[ParquetLedger](root, "ledgers", 0)
	rows := make([]ParquetLedger, rowCount)
	for i := range rows {
		sequence := startLedger + uint32(i)
		rows[i] = ParquetLedger{Sequence: sequence, LedgerRange: (sequence / 10000) * 10000, PipelineVersion: "integration-test"}
	}
	if err := writer.Write(rows, func(row ParquetLedger) uint32 { return row.LedgerRange }); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "manifest.json")
	config := OrchestratorConfig{StartLedger: startLedger, EndLedger: endLedger, OnlyTables: map[string]bool{"ledgers": true}}
	if err := WriteAttemptManifest(manifestPath, "integration-test", config, nil); err != nil {
		t.Fatal(err)
	}

	pusher, err := NewDuckLakePusher(DuckLakeConfig{
		CatalogDSN: catalog, DataPath: dataPath,
		MetadataSchema: "bronze_ingestion_test_meta", CatalogName: "lake", SchemaName: "bronze_ingestion_test",
		S3KeyID: keyID, S3KeySecret: keySecret, S3Endpoint: endpoint, S3Region: region,
		StartLedger: startLedger, EndLedger: endLedger, OnlyTables: map[string]bool{"ledgers": true}, ManifestPath: manifestPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pusher.Close()
	if _, err := pusher.db.Exec("SET extension_directory = '" + extensions + "'"); err != nil {
		t.Fatal(err)
	}
	result, err := pusher.Push(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowCounts["ledgers_row_v2"] != int64(rowCount) {
		t.Fatalf("ingested rows = %d, want %d", result.RowCounts["ledgers_row_v2"], rowCount)
	}
	var count, distinct int64
	if err := pusher.db.QueryRow("SELECT count(*), count(DISTINCT sequence) FROM lake.bronze_ingestion_test.ledgers_row_v2 WHERE sequence BETWEEN ? AND ?", startLedger, endLedger).Scan(&count, &distinct); err != nil {
		t.Fatal(err)
	}
	if count != int64(rowCount) || distinct != int64(rowCount) {
		t.Fatalf("DuckLake rows=%d distinct=%d, want %d/%d", count, distinct, rowCount, rowCount)
	}
}

func TestDuckLakePublishedLedgerRange(t *testing.T) {
	catalog := os.Getenv("DUCKLAKE_VERIFY_POSTGRES")
	dataPath := os.Getenv("DUCKLAKE_VERIFY_DATA_PATH")
	metadataSchema := os.Getenv("DUCKLAKE_VERIFY_METADATA_SCHEMA")
	extensions := os.Getenv("DUCKLAKE_VERIFY_EXTENSION_DIR")
	if catalog == "" || dataPath == "" || metadataSchema == "" || extensions == "" {
		t.Skip("set DUCKLAKE_VERIFY_POSTGRES, DUCKLAKE_VERIFY_DATA_PATH, DUCKLAKE_VERIFY_METADATA_SCHEMA, and DUCKLAKE_VERIFY_EXTENSION_DIR")
	}
	start, err := strconv.ParseUint(os.Getenv("DUCKLAKE_VERIFY_START"), 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	end, err := strconv.ParseUint(os.Getenv("DUCKLAKE_VERIFY_END"), 10, 32)
	if err != nil {
		t.Fatal(err)
	}

	pusher, err := NewDuckLakePusher(DuckLakeConfig{
		CatalogDSN: catalog, DataPath: dataPath, MetadataSchema: metadataSchema,
		S3KeyID: os.Getenv("DUCKLAKE_VERIFY_S3_KEY_ID"), S3KeySecret: os.Getenv("DUCKLAKE_VERIFY_S3_KEY_SECRET"),
		S3Endpoint: os.Getenv("DUCKLAKE_VERIFY_S3_ENDPOINT"), S3Region: os.Getenv("DUCKLAKE_VERIFY_S3_REGION"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pusher.Close()
	if _, err := pusher.db.Exec("SET extension_directory = '" + extensions + "'"); err != nil {
		t.Fatal(err)
	}
	if err := pusher.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	var rows, distinct, minimum, maximum int64
	if err := pusher.db.QueryRow(
		"SELECT count(*), count(DISTINCT sequence), min(sequence), max(sequence) FROM lake.bronze.ledgers_row_v2 WHERE sequence BETWEEN ? AND ?",
		start, end,
	).Scan(&rows, &distinct, &minimum, &maximum); err != nil {
		t.Fatal(err)
	}
	expected := int64(end-start) + 1
	if rows != expected || distinct != expected || minimum != int64(start) || maximum != int64(end) {
		t.Fatalf("rows=%d distinct=%d range=%d-%d, want %d distinct ledgers %d-%d", rows, distinct, minimum, maximum, expected, start, end)
	}
}
