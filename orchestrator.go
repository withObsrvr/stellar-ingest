package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OrchestratorConfig holds configuration for the orchestrator.
type OrchestratorConfig struct {
	StartLedger       uint32
	EndLedger         uint32
	NumWorkers        int
	OutputDir         string
	BatchSize         int
	NetworkPassphrase string
	StorageType       string
	Bucket            string
	LedgersPerFile    uint32
	FilesPerPartition uint32
	EraID             string
	OnlyTables        map[string]bool
}

// Orchestrator manages parallel workers for history loading.
type Orchestrator struct {
	config        OrchestratorConfig
	baseOutputDir string
	attemptID     string
	progress      *ProgressTracker
	checkpoint    *CheckpointManager
	checkpointErr error
	lineage       *LineageWriter
}

// Shard represents a range of ledgers assigned to a worker.
type Shard struct {
	ID          int
	StartLedger uint32
	EndLedger   uint32
}

// ProgressTracker tracks overall and per-worker progress.
type ProgressTracker struct {
	mu                sync.Mutex
	totalLedgers      uint64
	processedByWorker map[int]uint64
	startTime         time.Time
	lastReport        time.Time
}

// NewOrchestrator creates a new Orchestrator with the given configuration.
func NewOrchestrator(config OrchestratorConfig) *Orchestrator {
	checkpoint, checkpointErr := NewCheckpointManager(config.OutputDir, config)
	baseOutputDir := config.OutputDir
	attemptID := "invalid"
	if checkpoint != nil {
		attemptID = checkpoint.AttemptID()
		config.OutputDir = filepath.Join(baseOutputDir, "attempts", attemptID)
	}

	return &Orchestrator{
		config:        config,
		baseOutputDir: baseOutputDir,
		attemptID:     attemptID,
		progress: &ProgressTracker{
			processedByWorker: make(map[int]uint64),
		},
		checkpoint:    checkpoint,
		checkpointErr: checkpointErr,
		lineage:       NewLineageWriter(config.OutputDir, Version),
	}
}

func (o *Orchestrator) OutputDir() string { return o.config.OutputDir }

func (o *Orchestrator) ManifestPath() string {
	return filepath.Join(o.config.OutputDir, "manifest.json")
}

// Run splits the ledger range into shards and runs workers in parallel.
func (o *Orchestrator) Run() error {
	if o.checkpointErr != nil {
		return o.checkpointErr
	}
	// Persist the attempt identity before any output is created so a process
	// crash can resume the same isolated attempt instead of orphaning it.
	if err := o.checkpoint.Save(); err != nil {
		return fmt.Errorf("initialize attempt checkpoint: %w", err)
	}
	shards := o.splitIntoShards()

	log.Printf("Splitting range [%d, %d] into %d shards", o.config.StartLedger, o.config.EndLedger, len(shards))
	for _, s := range shards {
		shardSize := s.EndLedger - s.StartLedger + 1
		log.Printf("  Shard %d: ledgers %d - %d (%d ledgers)", s.ID, s.StartLedger, s.EndLedger, shardSize)
	}

	// Create one source factory. SDK streaming sources create and tear down an
	// independent backend for each worker's bounded StreamRange invocation.
	source, err := NewLedgerSourceFromConfig(o.config)
	if err != nil {
		return fmt.Errorf("create ledger source: %w", err)
	}
	defer source.Close()

	o.progress.startTime = time.Now()
	o.progress.lastReport = time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, len(shards))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start progress reporter
	stopProgress := make(chan struct{})
	var progressWg sync.WaitGroup
	progressWg.Add(1)
	go func() {
		defer progressWg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o.progress.PrintStatus()
			case <-stopProgress:
				return
			}
		}
	}()

	// Launch workers
	for _, shard := range shards {
		wg.Add(1)
		go func(s Shard) {
			defer wg.Done()
			if err := o.runWorker(ctx, s, source); err != nil {
				errCh <- fmt.Errorf("worker %d failed: %w", s.ID, err)
				cancel()
			}
		}(shard)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(stopProgress)
	progressWg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	// Flush lineage records. The manifest must never publish an attempt with
	// incomplete lineage.
	if err := o.lineage.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("flush lineage: %w", err))
	}

	// Print final status
	o.progress.PrintStatus()

	if len(errs) > 0 {
		for _, err := range errs {
			log.Printf("ERROR: %v", err)
		}
		return fmt.Errorf("%d worker(s) failed", len(errs))
	}

	if err := WriteAttemptManifest(o.ManifestPath(), o.attemptID, o.config, o.lineage.Records()); err != nil {
		return fmt.Errorf("publish attempt manifest: %w", err)
	}

	return nil
}

// splitIntoShards divides the ledger range evenly across workers.
func (o *Orchestrator) splitIntoShards() []Shard {
	totalLedgers := uint64(o.config.EndLedger-o.config.StartLedger) + 1
	numWorkers := o.config.NumWorkers

	// Don't create more shards than ledgers
	if uint64(numWorkers) > totalLedgers {
		numWorkers = int(totalLedgers)
	}

	shards := make([]Shard, 0, numWorkers)
	ledgersPerShard := totalLedgers / uint64(numWorkers)
	remainder := totalLedgers % uint64(numWorkers)

	current := o.config.StartLedger
	for i := 0; i < numWorkers; i++ {
		shardSize := ledgersPerShard
		// Distribute remainder across the first shards
		if uint64(i) < remainder {
			shardSize++
		}

		shard := Shard{
			ID:          i,
			StartLedger: current,
			EndLedger:   current + uint32(shardSize) - 1,
		}
		shards = append(shards, shard)
		current = shard.EndLedger + 1
	}

	return shards
}

// runWorker creates a Worker and runs it for the given shard.
func (o *Orchestrator) runWorker(ctx context.Context, shard Shard, source LedgerSource) error {
	log.Printf("[Worker %d] Starting: ledgers %d - %d", shard.ID, shard.StartLedger, shard.EndLedger)

	if !o.checkpoint.IsShardComplete(shard.ID) {
		if err := clearIncompleteShardFiles(o.config.OutputDir, shard.ID); err != nil {
			return fmt.Errorf("clear incomplete shard output: %w", err)
		}
	}

	writer, err := NewParquetWriter(o.config.OutputDir, shard.ID, Version)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}
	defer writer.Close()

	worker := NewWorker(shard.ID, shard, o.config, o.progress, writer, source, o.checkpoint, o.lineage)
	return worker.Run(ctx)
}

func clearIncompleteShardFiles(outputDir string, shardID int) error {
	bronzeDir := filepath.Join(outputDir, "bronze")
	prefix := fmt.Sprintf("shard_%04d_", shardID)
	return filepath.WalkDir(bronzeDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !entry.IsDir() && len(entry.Name()) > len(prefix) && entry.Name()[:len(prefix)] == prefix && filepath.Ext(entry.Name()) == ".parquet" {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove %s: %w", path, err)
			}
		}
		return nil
	})
}

// ReportProgress records that a worker has processed additional ledgers.
func (pt *ProgressTracker) ReportProgress(workerID int, ledgersProcessed uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.totalLedgers += ledgersProcessed
	pt.processedByWorker[workerID] += ledgersProcessed
}

// PrintStatus prints the current progress, throughput, and per-worker breakdown.
func (pt *ProgressTracker) PrintStatus() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	elapsed := time.Since(pt.startTime)
	if elapsed == 0 {
		return
	}

	throughput := float64(pt.totalLedgers) / elapsed.Seconds()

	fmt.Printf("[Progress] %d ledgers processed | %.1f ledgers/sec | elapsed %s",
		pt.totalLedgers, throughput, elapsed.Round(time.Second))

	// Per-worker breakdown
	if len(pt.processedByWorker) > 0 {
		fmt.Print(" | per-worker: ")
		first := true
		for id := 0; id < len(pt.processedByWorker); id++ {
			count, ok := pt.processedByWorker[id]
			if !ok {
				continue
			}
			if !first {
				fmt.Print(", ")
			}
			fmt.Printf("w%d=%d", id, count)
			first = false
		}
	}
	fmt.Println()

	pt.lastReport = time.Now()
}
