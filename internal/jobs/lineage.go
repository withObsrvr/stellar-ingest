package jobs

import (
	"fmt"

	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/logging"

	"github.com/parquet-go/parquet-go"
)

// LineageWriter collects lineage records and writes them to Parquet.
type LineageWriter struct {
	log             *logging.Logger
	mu              sync.Mutex
	records         []domain.LineageRecord
	outputDir       string
	pipelineVersion string
	startedAt       time.Time
}

func NewLineageWriter(outputDir, pipelineVersion string, log *logging.Logger) *LineageWriter {
	return &LineageWriter{
		log:             log,
		outputDir:       outputDir,
		pipelineVersion: pipelineVersion,
		startedAt:       time.Now(),
	}
}

// RecordBatch logs a batch write for a specific table.
func (lw *LineageWriter) RecordBatch(workerID int, dataset string, ledgerStart, ledgerEnd uint32, rowCount int, duration time.Duration) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	now := time.Now()
	lw.records = append(lw.records, domain.LineageRecord{
		LineageID:         fmt.Sprintf("w%d-%s-%d-%d", workerID, dataset, ledgerStart, ledgerEnd),
		Dataset:           dataset,
		SourceLedgerStart: ledgerStart,
		SourceLedgerEnd:   ledgerEnd,
		PipelineName:      "stellar-history-loader",
		PipelineVersion:   lw.pipelineVersion,
		RecordsProcessed:  int64(rowCount),
		StartedAt:         now.Add(-duration).UnixMicro(),
		CompletedAt:       now.UnixMicro(),
		DurationMs:        duration.Milliseconds(),
		WorkerID:          int32(workerID),
	})
}

// Flush writes all collected lineage records to a Parquet file.
func (lw *LineageWriter) Flush() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	if len(lw.records) == 0 {
		return nil
	}

	dir := filepath.Join(lw.outputDir, "bronze", "_meta_lineage")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create lineage dir: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("lineage_%d.parquet", time.Now().UnixNano()))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create lineage file: %w", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[domain.LineageRecord](f,
		parquet.Compression(&parquet.Snappy),
	)

	if _, err := writer.Write(lw.records); err != nil {
		return fmt.Errorf("write lineage records: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close lineage writer: %w", err)
	}

	lw.log.Detail("[Lineage] Wrote %d lineage records to %s", len(lw.records), path)
	return nil
}

func (lw *LineageWriter) Records() []domain.LineageRecord {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	records := make([]domain.LineageRecord, len(lw.records))
	copy(records, lw.records)
	return records
}
