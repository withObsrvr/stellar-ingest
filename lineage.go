package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// LineageRecord tracks one pipeline run's output for a specific table.
type LineageRecord struct {
	LineageID         string `parquet:"lineage_id"`
	Dataset           string `parquet:"dataset"`
	SourceLedgerStart uint32 `parquet:"source_ledger_start"`
	SourceLedgerEnd   uint32 `parquet:"source_ledger_end"`
	PipelineName      string `parquet:"pipeline_name"`
	PipelineVersion   string `parquet:"pipeline_version"`
	RecordsProcessed  int64  `parquet:"records_processed"`
	StartedAt         int64  `parquet:"started_at,timestamp(microsecond)"`
	CompletedAt       int64  `parquet:"completed_at,timestamp(microsecond)"`
	DurationMs        int64  `parquet:"duration_ms"`
	WorkerID          int32  `parquet:"worker_id"`
}

// LineageWriter collects lineage records and writes them to Parquet.
type LineageWriter struct {
	mu              sync.Mutex
	records         []LineageRecord
	outputDir       string
	pipelineVersion string
	startedAt       time.Time
}

func NewLineageWriter(outputDir, pipelineVersion string) *LineageWriter {
	return &LineageWriter{
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
	lw.records = append(lw.records, LineageRecord{
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

	writer := parquet.NewGenericWriter[LineageRecord](f,
		parquet.Compression(&parquet.Snappy),
	)

	if _, err := writer.Write(lw.records); err != nil {
		return fmt.Errorf("write lineage records: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close lineage writer: %w", err)
	}

	fmt.Printf("[Lineage] Wrote %d lineage records to %s\n", len(lw.records), path)
	return nil
}

func (lw *LineageWriter) Records() []LineageRecord {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	records := make([]LineageRecord, len(lw.records))
	copy(records, lw.records)
	return records
}
