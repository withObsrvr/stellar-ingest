package adapters

// The end-to-end check for the compact-identity storage change: a real
// transaction hash written as real Parquet by the production writer, inserted
// into a real DuckLake catalog, and read back out through the presentation
// views.
//
// Everything else about this change is covered without a database -- the
// encoding round-trips in Go, the views reproduce the pre-change column order
// in plain DuckDB, event_id reconstructs from the fixture corpus. What none of
// that exercises is the path the re-ingest actually runs: []byte through a
// Parquet file, through read_parquet into a BLOB column, through a view that
// renders lower(hex(..)) and joins a parent table.
//
// That path has one silent failure mode. DuckDB casts a VARCHAR into a BLOB
// column without complaint and stores the 64 ASCII bytes of the hex text, so
// the view renders 128 characters and the column costs twice the text rather
// than half. Nothing errors. This test measures the stored width.
//
// Opt-in, same as TestDuckLakeManifestIngestion:
//
//	DUCKLAKE_INGESTION_TEST_POSTGRES=postgresql://...
//	DUCKLAKE_INGESTION_TEST_EXTENSION_DIR=$HOME/.duckdb/extensions
//	DUCKLAKE_INGESTION_TEST_DATA_PATH=/some/dir/

import (
	"context"
	"fmt"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	compatTestHash = "a2069f36e0e430c44cf60d416ca2a8679155a28f695f98d959772dc103294f52"
	compatTestTxID = int64(264141029869883392)
	// An event whose parent transaction is deliberately absent, to prove the
	// views left-join rather than dropping the row.
	compatOrphanTxID = int64(999999999999999)
)

func TestCompatViewsRenderIdentityFromRealDuckLake(t *testing.T) {
	catalog := os.Getenv("DUCKLAKE_INGESTION_TEST_POSTGRES")
	extensions := os.Getenv("DUCKLAKE_INGESTION_TEST_EXTENSION_DIR")
	dataPath := os.Getenv("DUCKLAKE_INGESTION_TEST_DATA_PATH")
	if catalog == "" || extensions == "" || dataPath == "" {
		t.Skip("set DUCKLAKE_INGESTION_TEST_POSTGRES, DUCKLAKE_INGESTION_TEST_EXTENSION_DIR, and DUCKLAKE_INGESTION_TEST_DATA_PATH")
	}

	const seq uint32 = 60000
	const partition uint32 = 60000

	root := t.TempDir()
	only := map[string]bool{"transactions": true, "operations": true, "contract_events": true}

	// The parent, with the hash encoded exactly as the loader encodes it.
	txWriter := NewParquetTableWriter[ParquetTransaction](root, "transactions", 0)
	if err := txWriter.Write([]ParquetTransaction{{
		LedgerSequence:  seq,
		TransactionHash: domain.DecodeHashBytes(compatTestHash),
		TransactionID:   compatTestTxID,
		LedgerRange:     partition,
	}}, func(r ParquetTransaction) uint32 { return r.LedgerRange }); err != nil {
		t.Fatal(err)
	}
	if err := txWriter.Close(); err != nil {
		t.Fatal(err)
	}

	opWriter := NewParquetTableWriter[ParquetOperation](root, "operations", 0)
	if err := opWriter.Write([]ParquetOperation{{
		LedgerSequence: seq,
		TransactionID:  compatTestTxID,
		LedgerRange:    partition,
	}}, func(r ParquetOperation) uint32 { return r.LedgerRange }); err != nil {
		t.Fatal(err)
	}
	if err := opWriter.Close(); err != nil {
		t.Fatal(err)
	}

	evWriter := NewParquetTableWriter[ParquetContractEvent](root, "contract_events", 0)
	if err := evWriter.Write([]ParquetContractEvent{
		{LedgerSequence: seq, TransactionID: compatTestTxID, EventIndex: 118, LedgerRange: partition},
		{LedgerSequence: seq, TransactionID: compatOrphanTxID, EventIndex: 0, LedgerRange: partition},
	}, func(r ParquetContractEvent) uint32 { return r.LedgerRange }); err != nil {
		t.Fatal(err)
	}
	if err := evWriter.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(root, "manifest.json")
	config := domain.OrchestratorConfig{StartLedger: seq, EndLedger: seq, OnlyTables: only}
	if err := domain.WriteAttemptManifest(manifestPath, "compat-view-test", config, nil); err != nil {
		t.Fatal(err)
	}

	// Its own metadata schema, so this never touches another catalog.
	pusher, err := NewDuckLakePusher(DuckLakeConfig{
		CatalogDSN: catalog, DataPath: dataPath,
		// A metadata schema per run, because each one is a separate DuckLake
		// catalog and so cannot inherit the previous run's tables. Reusing a
		// fixed name made the partition assertion pass with the partitioning
		// code removed: CREATE TABLE IF NOT EXISTS left the already-partitioned
		// tables from the run before, and the test asserted against those.
		MetadataSchema: fmt.Sprintf("bronze_compat_%d_meta", time.Now().UnixNano()),
		CatalogName:    "lake",
		SchemaName:     "bronze_compat_store", ViewSchemaName: "bronze_compat",
		S3KeyID: "integration-test", S3KeySecret: "integration-test",
		S3Endpoint: "localhost:9000", S3Region: "us-east-1",
		StartLedger: seq, EndLedger: seq, OnlyTables: only, ManifestPath: manifestPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pusher.Close()
	if _, err := pusher.db.Exec("SET extension_directory = '" + extensions + "'"); err != nil {
		t.Fatal(err)
	}

	if _, err := pusher.Push(context.Background(), root); err != nil {
		t.Fatal(err)
	}

	// The stored width. 32 means the bytes; 64 means the hex text was cast in
	// and the saving became a cost.
	t.Run("stored as 32 bytes", func(t *testing.T) {
		var stored int
		if err := pusher.db.QueryRow(
			`SELECT octet_length(transaction_hash) FROM lake.bronze_compat_store.transactions_row_v2`,
		).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if stored != domain.HashBytesLen {
			t.Errorf("stored %d bytes, want %d. 64 means a VARCHAR was cast into the BLOB column "+
				"and the hex text is being stored", stored, domain.HashBytesLen)
		}
	})

	t.Run("parent view renders the hash", func(t *testing.T) {
		var got string
		if err := pusher.db.QueryRow(
			`SELECT transaction_hash FROM lake.bronze_compat.transactions_row_v2`,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != compatTestHash {
			t.Errorf("view renders %q, want %q", got, compatTestHash)
		}
	})

	t.Run("child view recovers the hash by joining", func(t *testing.T) {
		var got string
		if err := pusher.db.QueryRow(
			`SELECT transaction_hash FROM lake.bronze_compat.operations_row_v2`,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != compatTestHash {
			t.Errorf("view renders %q, want %q", got, compatTestHash)
		}
	})

	t.Run("event view rebuilds event_id", func(t *testing.T) {
		var eventID, hash string
		if err := pusher.db.QueryRow(
			`SELECT event_id, transaction_hash FROM lake.bronze_compat.contract_events_stream_v1
			 WHERE transaction_id = ?`, compatTestTxID,
		).Scan(&eventID, &hash); err != nil {
			t.Fatal(err)
		}
		// Exactly the string stellar-extract used to store.
		if want := compatTestHash + ":118"; eventID != want {
			t.Errorf("event_id = %q, want %q", eventID, want)
		}
		if hash != compatTestHash {
			t.Errorf("transaction_hash = %q, want %q", hash, compatTestHash)
		}
	})

	// Rule 6: the files the loader writes must be laid out by ledger_range.
	// This is what the validation run showed missing -- distinct_partitions was
	// 0 on every table, because the loader never set a partition layout and
	// pruning was falling back to per-file statistics.
	t.Run("files are laid out by ledger_range", func(t *testing.T) {
		// DuckLake inlines small writes into the catalog rather than writing
		// Parquet, and a test range is small. Flush first, so this asserts
		// rather than skipping -- a skip here would have hidden exactly the
		// gap this test exists for.
		if _, err := pusher.db.Exec(`CALL ducklake_flush_inlined_data('lake')`); err != nil {
			t.Fatalf("flush inlined Data: %v", err)
		}

		for _, table := range []string{"transactions_row_v2", "operations_row_v2", "contract_events_stream_v1"} {
			// The schema argument is required: these tables live in the store
			// schema, and without it the function looks in main and reports
			// the table as nonexistent.
			rows, err := pusher.db.Query(
				`SELECT data_file FROM ducklake_list_files('lake', ?, schema => ?)`,
				table, pusher.config.SchemaName)
			if err != nil {
				t.Fatalf("%s: list files: %v", table, err)
			}
			var files, partitioned int
			for rows.Next() {
				var path string
				if err := rows.Scan(&path); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				files++
				// DuckLake writes a Hive-style directory per partition value.
				if strings.Contains(path, duckLakePartitionColumn+"=") {
					partitioned++
				}
			}
			rows.Close()

			if files == 0 {
				t.Errorf("%s: no data files after flushing; nothing was verified", table)
				continue
			}
			if partitioned != files {
				t.Errorf("%s: %d of %d files carry a %s= directory; unpartitioned files prune "+
					"only by statistics", table, partitioned, files, duckLakePartitionColumn)
			}
		}
	})

	// The reason the views left-join. A child flushed ahead of its parent must
	// stay visible with a null hash rather than disappearing silently.
	t.Run("orphaned child stays visible", func(t *testing.T) {
		var rows int
		if err := pusher.db.QueryRow(
			`SELECT count(*) FROM lake.bronze_compat.contract_events_stream_v1`,
		).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 2 {
			t.Fatalf("view returns %d of 2 events; a child without a parent was dropped", rows)
		}

		var hash, eventID *string
		if err := pusher.db.QueryRow(
			`SELECT transaction_hash, event_id FROM lake.bronze_compat.contract_events_stream_v1
			 WHERE transaction_id = ?`, compatOrphanTxID,
		).Scan(&hash, &eventID); err != nil {
			t.Fatal(err)
		}
		if hash != nil || eventID != nil {
			t.Errorf("orphan rendered hash=%v event_id=%v, want both null", hash, eventID)
		}
	})
}
