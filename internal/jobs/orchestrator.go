package jobs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/adapters"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/logging"
	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// Orchestrator manages parallel workers for history loading.
type Orchestrator struct {
	config        domain.OrchestratorConfig
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

	// What a UI watches. The same numbers this prints for a person are
	// reported as OSC 7501 program status, so a terminal, dashboard or
	// supervising tool reads the program's own account of its progress
	// rather than scraping the line below.
	statusEmitter *status.Emitter
	// expectedLedgers is the size of the requested range, so progress can be
	// a percentage. Zero means unknown, and no percentage is reported.
	expectedLedgers uint64
	// log narrates the progress line. Routed through the logger so --quiet
	// suppresses it and so the whole line is written at once; see
	// PrintStatus.
	log *logging.Logger
}

// expectedLedgers is how many ledgers the requested range covers, used to
// turn a count into a percentage. Zero when the range is not a sane pair,
// in which case progress is reported without a percentage rather than with a
// wrong one.
func expectedLedgers(config domain.OrchestratorConfig) uint64 {
	if config.EndLedger < config.StartLedger {
		return 0
	}
	return uint64(config.EndLedger-config.StartLedger) + 1
}

// reportStatus publishes the same progress as OSC 7501 program status.
//
// Errors are ignored on purpose: a status report that fails to write must
// never fail or interrupt the work it describes.
func (pt *ProgressTracker) reportStatus(throughput float64) {
	if !pt.statusEmitter.Enabled() {
		return
	}
	percent := -1
	if pt.expectedLedgers > 0 {
		percent = int(pt.totalLedgers * 100 / pt.expectedLedgers)
		if percent > 100 {
			percent = 100
		}
	}
	_ = pt.statusEmitter.Report(status.Report{
		State:    status.Working,
		Title:    fmt.Sprintf("%d ledgers, %.0f/s", pt.totalLedgers, throughput),
		Progress: progressPointer(percent),
	})
}

// progressPointer returns nil for an unknown percentage, which omits the
// field rather than reporting a value outside the protocol's range.
func progressPointer(percent int) *int {
	if percent < 0 {
		return nil
	}
	return &percent
}

// NewOrchestrator creates a new Orchestrator with the given configuration.
func NewOrchestrator(config domain.OrchestratorConfig) *Orchestrator {
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
			statusEmitter:     config.StatusEmitter,
			expectedLedgers:   expectedLedgers(config),
			log:               config.Log,
		},
		checkpoint:    checkpoint,
		checkpointErr: checkpointErr,
		lineage:       NewLineageWriter(config.OutputDir, domain.Version, config.Log),
	}
}

func (o *Orchestrator) OutputDir() string { return o.config.OutputDir }

func (o *Orchestrator) ManifestPath() string {
	return filepath.Join(o.config.OutputDir, "manifest.json")
}

// Run splits the ledger range into shards and runs workers in parallel.
//
// The context is the caller's, and cancelling it stops the run. It used to
// be context.Background(): every worker got a context that nothing could
// cancel, so the signal context main.go threads through app.Run died here
// and a SIGINT during extraction -- the long part of any backfill -- was
// ignored until the range finished. Measured before the fix: a run survived
// 60 seconds of SIGINT and had to be killed, losing the checkpoint and
// leaving the range claim held until its lease expired.
func (o *Orchestrator) Run(ctx context.Context) error {
	if o.checkpointErr != nil {
		return o.checkpointErr
	}
	// Persist the attempt identity before any output is created so a process
	// crash can resume the same isolated attempt instead of orphaning it.
	if err := o.checkpoint.Save(); err != nil {
		return fmt.Errorf("initialize attempt checkpoint: %w", err)
	}
	shards := o.splitIntoShards()

	o.config.Log.Info("Splitting range [%d, %d] into %d shards", o.config.StartLedger, o.config.EndLedger, len(shards))
	for _, s := range shards {
		shardSize := s.EndLedger - s.StartLedger + 1
		o.config.Log.Detail("  Shard %d: ledgers %d - %d (%d ledgers)", s.ID, s.StartLedger, s.EndLedger, shardSize)
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
	// Derived from the caller's, so a cancelled caller cancels every worker.
	// The local cancel still exists so the first worker to fail stops the
	// others rather than letting them finish work that is already wasted.
	//
	// caller is kept separate deliberately: both a stopped caller and a
	// failed worker cancel the derived context, so asking the derived one
	// "were we cancelled?" would report every worker failure as a clean
	// shutdown and hand a scheduler exit 130 for a real bug.
	caller := ctx
	ctx, cancel := context.WithCancel(caller)
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
		// A cancelled run is reported as cancellation, not as N failures.
		// Every worker returns ctx.Err() when the caller stops the run, and
		// calling that an internal failure would tell a scheduler to retry a
		// deliberate shutdown -- and would hide the one piece of information
		// the operator needs, which is that the stop was theirs.
		if cause := caller.Err(); cause != nil {
			o.config.Log.Info("Run stopped: %v (%d shard(s) interrupted)", cause, len(errs))
			return cause
		}
		for _, err := range errs {
			o.config.Log.Error("%v", err)
		}
		return fmt.Errorf("%d worker(s) failed", len(errs))
	}

	// A run the caller stopped must not publish a manifest, even if every
	// shard happened to return cleanly in the window before it noticed.
	// A manifest is the claim that the attempt is complete.
	if cause := caller.Err(); cause != nil {
		return cause
	}

	if err := domain.WriteAttemptManifest(o.ManifestPath(), o.attemptID, o.config, o.lineage.Records()); err != nil {
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
	o.config.Log.Info("[Worker %d] Starting: ledgers %d - %d", shard.ID, shard.StartLedger, shard.EndLedger)

	if !o.checkpoint.IsShardComplete(shard.ID) {
		if err := clearIncompleteShardFiles(o.config.OutputDir, shard.ID); err != nil {
			return fmt.Errorf("clear incomplete shard output: %w", err)
		}
	}

	writer, err := adapters.NewParquetWriter(o.config.OutputDir, shard.ID, domain.Version)
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

	// Built as one string and emitted once.
	//
	// This used to be five Fprintf calls to os.Stderr followed by a
	// Println to STDOUT, so the line's terminating newline landed on the
	// other stream. Successive progress lines then ran together on stderr:
	// the mainnet validation logs are full of
	// "...elapsed 5s[Progress] 0 ledgers processed...". Writing the whole
	// line at once also means a concurrent chunk cannot interleave into the
	// middle of it, and routing it through the logger is what makes --quiet
	// suppress progress.
	var line strings.Builder
	fmt.Fprintf(&line, "[Progress] %d ledgers processed | %.1f ledgers/sec | elapsed %s",
		pt.totalLedgers, throughput, elapsed.Round(time.Second))

	pt.reportStatus(throughput)

	// Per-worker breakdown
	if len(pt.processedByWorker) > 0 {
		line.WriteString(" | per-worker: ")
		first := true
		for id := 0; id < len(pt.processedByWorker); id++ {
			count, ok := pt.processedByWorker[id]
			if !ok {
				continue
			}
			if !first {
				line.WriteString(", ")
			}
			fmt.Fprintf(&line, "w%d=%d", id, count)
			first = false
		}
	}
	pt.log.Info("%s", line.String())

	pt.lastReport = time.Now()
}
