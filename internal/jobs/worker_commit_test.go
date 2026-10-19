package jobs

import (
	"context"
	"errors"
	"github.com/withObsrvr/stellar-ingest/internal/adapters"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"path/filepath"
	"testing"
)

// This is deliberately not a general destination abstraction: Close models
// the Parquet footer/finalization boundary used by Worker.Run.
type faultShardWriter struct {
	writeErr error
	closeErr error
	writes   int
	closes   int
}

func (w *faultShardWriter) WriteBatch(*domain.BatchData) error {
	w.writes++
	return w.writeErr
}
func (w *faultShardWriter) Close() error {
	w.closes++
	return w.closeErr
}

func TestWorkerCheckpointWaitsForParquetFinalization(t *testing.T) {
	ledgerPath := filepath.Join("..", "..", "testdata", "fixtures", "pubnet-genesis-p1", "ledgers.xdr")
	failure := errors.New("injected writer failure")
	for _, tc := range []struct {
		name     string
		writeErr error
		closeErr error
	}{
		{"batch write", failure, nil},
		{"footer finalization", nil, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := domain.OrchestratorConfig{
				StartLedger: 100, EndLedger: 104, NumWorkers: 1,
				BatchSize: 2, OutputDir: t.TempDir(), StorageType: "XDR",
				Bucket:            ledgerPath,
				NetworkPassphrase: "Public Global Stellar Network ; September 2015",
			}
			checkpoint, err := NewCheckpointManager(cfg.OutputDir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkpoint.Save(); err != nil {
				t.Fatal(err)
			}
			source, err := adapters.NewXDRStreamSource(ledgerPath)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			shard := Shard{ID: 0, StartLedger: 100, EndLedger: 104}
			newWorker := func(writer shardWriter) *Worker {
				return NewWorker(0, shard, cfg,
					&ProgressTracker{processedByWorker: make(map[int]uint64)},
					writer, source, checkpoint, nil)
			}

			faulty := &faultShardWriter{writeErr: tc.writeErr, closeErr: tc.closeErr}
			if err := newWorker(faulty).Run(context.Background()); !errors.Is(err, failure) {
				t.Fatalf("run error = %v, want injected failure", err)
			}
			if faulty.writes == 0 {
				t.Fatal("writer was not exercised")
			}
			if tc.closeErr != nil && faulty.closes != 1 {
				t.Fatalf("Close called %d times", faulty.closes)
			}
			assertNotCompleted := func() {
				t.Helper()
				loaded, err := NewCheckpointManager(cfg.OutputDir, cfg)
				if err != nil {
					t.Fatal(err)
				}
				if loaded.IsShardComplete(0) {
					t.Fatal("checkpoint advanced before successful finalization")
				}
				from, skip := loaded.GetShardResumePoint(0, shard)
				if skip || from != shard.StartLedger {
					t.Fatalf("resume = %d, skip=%v; want shard start", from, skip)
				}
			}
			assertNotCompleted()

			// A retry must re-read the full shard; the orchestrator deletes any
			// incomplete Parquet shard files before invoking this worker.
			retry := &faultShardWriter{}
			if err := newWorker(retry).Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if retry.writes == 0 || retry.closes != 1 {
				t.Fatalf("retry: %d writes, %d closes", retry.writes, retry.closes)
			}
			loaded, err := NewCheckpointManager(cfg.OutputDir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !loaded.IsShardComplete(0) {
				t.Fatal("checkpoint did not advance after finalization")
			}
			if _, skip := loaded.GetShardResumePoint(0, shard); !skip {
				t.Fatal("completed shard was not skipped")
			}
		})
	}
}
