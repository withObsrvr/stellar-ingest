package jobs

import (
	"database/sql"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// extractWith runs a fixture through the orchestrator with an explicit worker
// count and chunk size, returning the digest of its Bronze output.
func extractWith(t *testing.T, db *sql.DB, meta fixtures.Meta, ledgerPath, outputDir string, workers int, batchSize int) FixtureDigest {
	t.Helper()

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	cfg := domain.OrchestratorConfig{
		StartLedger:       meta.StartLedger,
		EndLedger:         meta.EndLedger,
		NumWorkers:        workers,
		OutputDir:         outputDir,
		BatchSize:         batchSize,
		NetworkPassphrase: meta.NetworkPassphrase,
		StorageType:       "XDR",
		Bucket:            ledgerPath,
	}
	if err := NewOrchestrator(cfg).Run(); err != nil {
		t.Fatalf("extract with workers=%d batch=%d: %v", workers, batchSize, err)
	}

	bronze, err := findAttemptDir(outputDir)
	if err != nil {
		t.Fatalf("locate attempt dir: %v", err)
	}
	d, err := digestFixtureOutput(db, bronze, meta.Name)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return d
}

// requireSameDigest asserts two runs produced identical Bronze content,
// naming the drifted columns when they did not.
func requireSameDigest(t *testing.T, label string, want, got FixtureDigest) {
	t.Helper()

	for _, table := range sortedKeys(want.Tables) {
		w := want.Tables[table]
		g, ok := got.Tables[table]
		if !ok {
			t.Errorf("%s: table %q emitted by the baseline run but not the variant", label, table)
			continue
		}
		if w.RowCount != g.RowCount {
			t.Errorf("%s: table %q row count %d, baseline %d", label, table, g.RowCount, w.RowCount)
			continue
		}
		if w.ContentHash != g.ContentHash {
			drifted := driftedColumns(w.ColumnHashes, g.ColumnHashes)
			t.Errorf("%s: table %q content differs; drifted columns: %v", label, table, drifted)
		}
	}
	for _, table := range sortedKeys(got.Tables) {
		if _, ok := want.Tables[table]; !ok {
			t.Errorf("%s: table %q emitted by the variant but not the baseline", label, table)
		}
	}
}

// TestWorkerCountInvariance pins that Bronze content does not depend on how
// many workers produced it.
//
// Worker count determines shard boundaries, so this is the knob most likely to
// change output by accident, and the one the handoff singles out: a resume
// with a different worker count must not corrupt or duplicate rows. Row
// ordering inside Parquet legitimately varies here, which is why the digest
// sorts row hashes rather than rows.
func TestWorkerCountInvariance(t *testing.T) {
	scrubEnv(t)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	const name = "pubnet-soroban-p29"
	meta, _ := fixtures.LoadMeta(t, name)
	work := t.TempDir()
	ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), work)

	baseline := extractWith(t, db, meta, ledgerPath, filepath.Join(work, "w1"), 1, 1000)

	for _, workers := range []int{2, 3, 8} {
		got := extractWith(t, db, meta, ledgerPath,
			filepath.Join(work, "w"+itoa(workers)), workers, 1000)
		requireSameDigest(t, "workers="+itoa(workers)+" vs workers=1", baseline, got)
	}
}

// TestBatchSizeInvariance pins that Bronze content does not depend on the
// extraction batch size.
//
// Batch size controls when accumulated rows are flushed to the Parquet writer.
// A batch smaller than the range forces multiple flushes per table, which is
// where a writer that carried state across flushes would show up.
func TestBatchSizeInvariance(t *testing.T) {
	scrubEnv(t)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	const name = "pubnet-soroban-p29"
	meta, _ := fixtures.LoadMeta(t, name)
	work := t.TempDir()
	ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), work)

	baseline := extractWith(t, db, meta, ledgerPath, filepath.Join(work, "b1000"), 2, 1000)

	for _, batch := range []int{1, 3} {
		got := extractWith(t, db, meta, ledgerPath,
			filepath.Join(work, "b"+itoa(batch)), 2, batch)
		requireSameDigest(t, "batch-size="+itoa(batch)+" vs 1000", baseline, got)
	}
}

// TestRerunCompleteRangeIsIdempotent pins that extracting the same range twice
// into two clean output directories yields identical Bronze content.
//
// This is the "re-run range" recovery scenario. It is distinct from
// TestExtractionIsDeterministic only in intent — that one guards
// reproducibility of a single range, this one guards that re-running a
// completed range is a safe operational act rather than something that
// accumulates or duplicates rows.
func TestRerunCompleteRangeIsIdempotent(t *testing.T) {
	scrubEnv(t)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	const name = "pubnet-state-eviction-p23"
	meta, _ := fixtures.LoadMeta(t, name)
	work := t.TempDir()
	ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), work)

	first := extractWith(t, db, meta, ledgerPath, filepath.Join(work, "run1"), 2, 1000)
	second := extractWith(t, db, meta, ledgerPath, filepath.Join(work, "run2"), 2, 1000)
	requireSameDigest(t, "re-run of a complete range", first, second)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestCorruptedXDRIsRejected pins that a damaged ledger stream fails loudly
// rather than yielding a short but plausible-looking extraction.
//
// The handoff's invariant is that data errors must not be retried
// indefinitely, which presupposes they are detected at all. The failure mode
// worth guarding is silent truncation: a corrupt byte midway through a stream
// producing a clean exit over fewer ledgers than asked for, which downstream
// would read as a complete range.
func TestCorruptedXDRIsRejected(t *testing.T) {
	scrubEnv(t)

	const name = "pubnet-genesis-p1"
	meta, path := fixtures.LoadMeta(t, name)

	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	cases := []struct {
		label  string
		mutate func([]byte) []byte
	}{
		{"truncated mid-record", func(b []byte) []byte {
			return b[:len(b)/2]
		}},
		{"corrupt length field", func(b []byte) []byte {
			out := append([]byte(nil), b...)
			// Smash the union discriminant of the first record.
			out[0], out[1], out[2], out[3] = 0x7f, 0xff, 0xff, 0xff
			return out
		}},
		{"garbage appended after a valid stream", func(b []byte) []byte {
			return append(append([]byte(nil), b...), 0x01, 0x02, 0x03)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			work := t.TempDir()
			bad := filepath.Join(work, "ledgers.xdr")
			if err := os.WriteFile(bad, tc.mutate(good), 0o644); err != nil {
				t.Fatalf("write corrupted fixture: %v", err)
			}

			out := filepath.Join(work, "out")
			if err := os.MkdirAll(out, 0o755); err != nil {
				t.Fatalf("create output dir: %v", err)
			}

			cfg := domain.OrchestratorConfig{
				StartLedger:       meta.StartLedger,
				EndLedger:         meta.EndLedger,
				NumWorkers:        1,
				OutputDir:         out,
				BatchSize:         1000,
				NetworkPassphrase: meta.NetworkPassphrase,
				StorageType:       "XDR",
				Bucket:            bad,
			}

			err := NewOrchestrator(cfg).Run()
			if err == nil {
				t.Errorf("corrupted stream (%s) extracted without error; "+
					"a damaged range must not look like a successful one", tc.label)
			} else {
				t.Logf("rejected as expected: %v", err)
			}
		})
	}
}
