package app

// Run performs a full ingestion: acquire ledgers, extract the 21 Bronze
// datasets to Parquet, push them to DuckLake, and populate the PostgreSQL hot
// buffer. It is the operation the flat flag form has always performed.
//
// This body used to be func main. Moving it here is what makes it callable by
// something other than a terminal, which is the whole point of §2 of the CLI
// standard: an API, a UI or a test can run an ingestion without a process
// boundary or a flag parser.
//
// It returns an error rather than calling log.Fatal, which it did 39 times.
// Two things follow from that. The adapter chooses the exit code, so a
// scheduler can tell invalid input from an internal failure instead of
// receiving 1 for both. And the operation became testable: a function that
// terminates the process cannot be asserted on, which is why §10's first
// release gate is "core business logic testable without terminal or network
// transport".
//
// Returning also fixed latent bugs that log.Fatal was hiding. Several sites
// read like this:
//
//	pusher, err := adapters.NewDuckLakePusher(...)
//	if err != nil {
//		fatalChunk("ducklake_setup", err, "retry_chunk")
//	}
//	defer pusher.Close()
//
// That was only correct because fatalChunk never came back. Any future reader
// who made it return would have had a nil-pointer panic on the next line.
//
// CONTEXT NOW REACHES THE BLOCKING CALLS. Range-state claims, plans, chunked
// backfills and the hot-buffer load each used context.Background(), so no
// deadline or signal could reach them -- §5 asks for the opposite: "Pass
// context.Context through every blocking layer." Freeze is the one deliberate
// exception and says why at its call site.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	extract "github.com/withObsrvr/stellar-extract"
	flowcomponent "github.com/withobsrvr/flowctl/pkg/component"
	flowctlpb "github.com/withobsrvr/flowctl/proto"

	"github.com/withObsrvr/stellar-ingest/internal/adapters"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/events"
	"github.com/withObsrvr/stellar-ingest/internal/exit"
	"github.com/withObsrvr/stellar-ingest/internal/jobs"
)

// freezeGrace is how long a range freeze may take after the push has landed,
// even when the run is being cancelled.
const freezeGrace = 30 * time.Second

// Run executes the request, returning an error whose exit class the adapter
// turns into a process exit code.
func Run(ctx context.Context, req RunRequest, deps Deps) (*RunResult, error) {
	log := deps.log()
	started := time.Now()
	// One identity for the run. The event stream's job id and the range
	// catalog's attempt_id are the same value, so a consumer joins them
	// directly instead of matching on timestamps.
	jobID := deps.Events.JobID()
	if jobID == "" {
		jobID = jobs.NewAttemptID()
	}
	operation := req.Operation
	if operation == "" {
		operation = "run"
	}

	if req.EnrichContractBalances {
		return runEnrichment(ctx, req, deps, operation, started)
	}

	extractRequested := domain.ExtractionRequested(req.StorageType, req.Bucket)

	// A validate-only run reads a directory that already exists. It needs no
	// source, no destination and no ledger range, and demanding them made
	// the validate verb impossible to run at all: the preset clears Bucket
	// and RunDuckLake, which tripped the --bucket rule below on every
	// invocation. Found by running it.
	validateOnly := req.RunValidate && !extractRequested && !req.RunDuckLake && req.PgHost == ""

	// Validate required flags
	if req.OutputDir == "" {
		return nil, exit.Usagef("--output-dir is required")
	}
	if !validateOnly {
		if req.Start == 0 {
			return nil, exit.Usagef("--start is required and must be > 0")
		}
		if req.End == 0 {
			return nil, exit.Usagef("--end is required and must be > 0")
		}
		if !extractRequested && !req.RunDuckLake {
			return nil, exit.Usagef("--bucket is required (unless using CAPTIVE_CORE or --ducklake with existing output)")
		}
	}

	startLedger := uint32(req.Start)
	endLedger := uint32(req.End)
	extractOnlySet := domain.ParseCSVSet(req.OnlyTables)
	duckLakeOnlySet := domain.ParseCSVSet(req.DlOnlyTables)
	if len(duckLakeOnlySet) == 0 && len(extractOnlySet) > 0 {
		duckLakeOnlySet = extractOnlySet
	}
	duckLakeSkipSet := domain.ParseCSVSet(req.DlSkipTables)

	if err := domain.ValidateLedgerRange(startLedger, endLedger); err != nil {
		return nil, exit.Wrap(exit.Usage, err)
	}

	if req.Workers < 1 {
		return nil, exit.Usagef("--workers must be >= 1")
	}

	if req.BatchSize < 1 {
		return nil, exit.Usagef("--batch-size must be >= 1")
	}
	if req.ChunkSize > 0 && !extractRequested {
		return nil, exit.Usagef("--chunk-size requires an extraction source")
	}
	if req.ChunkConcurrency < 1 {
		return nil, exit.Usagef("--chunk-concurrency must be at least 1")
	}
	if req.PrefetchBuffer < 1 {
		return nil, exit.Usagef("--prefetch-buffer must be at least 1")
	}
	if req.ChunkConcurrency > 1 && req.ChunkSize == 0 {
		return nil, exit.Usagef("--chunk-concurrency requires --chunk-size: there is nothing to run concurrently without chunks")
	}
	if req.ChunkConcurrency > 1 && !req.RangeState {
		// Concurrent chunks are separate processes writing the same DuckLake
		// tables. Without the range catalog nothing stops two runs taking the
		// same chunk, and a crashed chunk leaves no record that it needs
		// redoing -- which is most of what makes concurrency safe here.
		return nil, exit.Usagef("--chunk-concurrency above 1 requires --range-state")
	}
	if uint64(req.ChunkSize) > uint64(^uint32(0)) {
		return nil, exit.Usagef("--chunk-size exceeds the supported uint32 ledger range")
	}
	if req.ChunkSize > 0 && req.PgHost != "" {
		return nil, exit.Usagef("--chunk-size cannot currently be combined with --pg-host; populate the hot buffer after the chunked backfill")
	}

	switch req.StorageType {
	case "GCS", "S3", "FS", "XDR", "RPC", "CAPTIVE_CORE":
		// valid
	default:
		return nil, exit.Usagef("--storage-type must be one of: GCS, S3, FS, XDR, RPC, CAPTIVE_CORE (got %q)", req.StorageType)
	}
	if err := domain.ValidateTableSelection(extractOnlySet, "only-tables"); err != nil {
		return nil, exit.Wrap(exit.Usage, err)
	}
	if err := domain.ValidateTableSelection(duckLakeOnlySet, "ducklake-only-tables"); err != nil {
		return nil, exit.Wrap(exit.Usage, err)
	}
	if err := domain.ValidateTableSelection(duckLakeSkipSet, "ducklake-skip-tables"); err != nil {
		return nil, exit.Wrap(exit.Usage, err)
	}

	// Credentials and range-state coherence are input rules, so they are
	// enforced here rather than where they are first used.
	//
	// They used to be checked at the point of use, which for the DuckLake
	// credentials was AFTER extraction: a run with --ducklake and no
	// B2_KEY_SECRET extracted the whole range to Parquet and then discovered
	// it could not push. On a 10,000-ledger chunk that is an hour of work
	// thrown away to report a missing environment variable. A test caught it
	// by asserting that a rejected run creates no output directory.
	if req.RangeState {
		if req.DlCatalog == "" {
			return nil, exit.Usagef("--range-state requires a catalog DSN (DUCKLAKE_CATALOG_DSN)")
		}
		// frozen means "pushed to DuckLake". Claiming a range this run can
		// never push would leave it freezing until the lease expired.
		if !req.RunDuckLake {
			return nil, exit.Usagef("--range-state requires --ducklake: a range is frozen when the push lands, not when extraction finishes")
		}
	}
	if req.RunDuckLake {
		if err := requireDuckLakeCredentials(req); err != nil {
			return nil, err
		}
	}

	// --plan answers "what is left" without doing anything. Read-only: it
	// takes no claim, so running it while a backfill is in flight is safe and
	// shows that backfill's live claims.
	//
	// This is the postcondition-driven half of Item 5. Dispatching ranges by
	// hand is what made a multi-day backfill depend on an operator's notes;
	// here the catalog is asked instead.
	if req.PlanOnly {
		return runPlan(ctx, req, deps, operation, started, startLedger, endLedger)
	}

	if req.ChunkSize > 0 {
		summary, err := jobs.RunChunkedCommand(ctx, os.Args[1:], startLedger, endLedger, uint32(req.ChunkSize), req.OutputDir, req.ChunkConcurrency, log)
		if err != nil {
			return nil, fmt.Errorf("chunked backfill failed: %w", err)
		}
		chunked := newRunResult(operation, domain.Version, started)
		chunked.Range = &RangeResult{Start: startLedger, End: endLedger, Ledgers: endLedger - startLedger + 1}
		chunked.Chunks = &ChunksResult{
			Total:       summary.Total,
			Built:       summary.Built,
			Skipped:     summary.Skipped,
			Failed:      summary.Failed,
			Size:        uint32(req.ChunkSize),
			Concurrency: req.ChunkConcurrency,
			DurationMS:  time.Since(started).Milliseconds(),
		}
		return chunked.finish(started), nil
	}

	result := newRunResult(operation, domain.Version, started)
	result.JobID = jobID
	// Omitted for a validate-only run, which has no range. Reporting
	// start 0, end 0 and "1 ledger" would be a number a consumer reads as
	// real.
	if !validateOnly {
		result.Range = &RangeResult{Start: startLedger, End: endLedger, Ledgers: endLedger - startLedger + 1}
	}

	// Emission errors are ignored throughout: a full disk on the event
	// stream must not fail an ingestion that is otherwise working.
	resource := "range/" + thisRangeLabel(startLedger, endLedger)
	_ = deps.Events.Emit(events.TypeRunStarted, resource, map[string]any{
		"operation": operation,
		"start":     startLedger,
		"end":       endLedger,
		"version":   domain.Version,
	})

	totalLedgers := endLedger - startLedger + 1

	log.Section("Stellar History Loader")
	log.Field("Ledger range", "%d - %d (%d ledgers)", startLedger, endLedger, totalLedgers)
	log.Field("Workers", "%d", req.Workers)
	log.Field("Prefetch buffer", "%d ledgers/worker", req.PrefetchBuffer)
	log.Field("Batch size", "%d", req.BatchSize)
	log.Field("Output directory", "%s", req.OutputDir)
	log.Field("Storage type", "%s", req.StorageType)
	log.Field("Bucket", "%s", req.Bucket)
	log.Field("Network", "%s", req.NetworkPassphrase)
	if len(extractOnlySet) > 0 {
		log.Field("Extract tables", "%s", domain.TableSetKey(extractOnlySet))
	}
	if len(duckLakeOnlySet) > 0 {
		log.Field("DuckLake tables", "%s", domain.TableSetKey(duckLakeOnlySet))
	}
	log.Line()

	// Ensure output directory exists
	if err := os.MkdirAll(req.OutputDir, 0o755); err != nil {
		// Not classified here: a missing parent and a permission denial are
		// both common and ClassOf reads the sentinel the filesystem already
		// attached.
		return nil, fmt.Errorf("failed to create output directory %q: %w", req.OutputDir, err)
	}
	effectiveOutput := req.OutputDir
	manifestPath := ""
	if !extractRequested {
		candidate := filepath.Join(effectiveOutput, "manifest.json")
		if _, err := os.Stat(candidate); err == nil {
			manifestPath = candidate
		}
	}

	reporter, err := flowcomponent.NewReporter(ctx, flowcomponent.ConfigFromEnv())
	if err != nil {
		return nil, fmt.Errorf("flowctl setup failed: %w", err)
	}
	defer reporter.Close()
	if err := reporter.Register(ctx, flowctlpb.ServiceType_SERVICE_TYPE_SOURCE, map[string]string{
		"component":    "stellar-history-loader",
		"version":      domain.Version,
		"network":      req.NetworkPassphrase,
		"storage_type": req.StorageType,
		"range_start":  fmt.Sprintf("%d", startLedger),
		"range_end":    fmt.Sprintf("%d", endLedger),
	}); err != nil {
		return nil, fmt.Errorf("flowctl registration failed: %w", err)
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go reporter.StartHeartbeatLoop(heartbeatCtx, nil, nil)

	// chunkFailed records the failure against the chunk before returning it,
	// so a watcher sees why rather than seeing the record stop. It replaces a
	// closure named fatalChunk that called log.Fatalf.
	chunkFailed := func(phase string, err error, action string) error {
		_ = reporter.ReportChunkFailed(ctx, int64(startLedger), int64(endLedger), phase, domain.ClassifyFlowctlFailure(err), err.Error(), action)
		return fmt.Errorf("%s failed: %w", phase, err)
	}

	// Durable range state. Claimed before any work and frozen only after the
	// DuckLake push returns, because a range is not done when its Parquet
	// exists -- it is done when the push lands.
	//
	// This sits on the single-range path deliberately: jobs.RunChunkedCommand
	// re-invokes this binary per chunk, so chunked backfills inherit it
	// without the chunk runner knowing anything about it, and so does a
	// one-range Nomad dispatch.
	var rangeStore *jobs.RangeStateStore
	thisRange := jobs.LedgerRange{Start: startLedger, End: endLedger}
	rangeAttemptID := jobID
	if req.RangeState {
		var err error
		rangeStore, err = jobs.NewRangeStateStore(ctx, req.DlCatalog, req.RangeStateTable, req.EraID)
		if err != nil {
			return nil, fmt.Errorf("range state: %w", err)
		}
		defer rangeStore.Close()

		claimed, err := rangeStore.Claim(ctx, thisRange, rangeAttemptID, domain.Version, jobs.DefaultClaimLease)
		if err != nil {
			return nil, fmt.Errorf("range state: %w", err)
		}
		if !claimed {
			// Either already frozen, or held by a live claim. Both mean there
			// is nothing to do here, and both are a success: a resumed plan
			// walking a range it already finished must not fail the run.
			// The marker is for the chunk runner; the sentence is for a
			// human reading a single-range dispatch.
			log.Info("%s Range %s is already frozen or claimed by another run; nothing to do.",
				jobs.RangeSkippedMarker, thisRange)
			result.Outcome = OutcomeSkipped
			_ = deps.Events.Emit(events.TypeRunSkipped, resource, nil)
			return result.finish(started), nil
		}
		log.Info("Range %s claimed (freezing)", thisRange)
		_ = deps.Events.Emit(events.TypeRangeClaimed, resource, map[string]any{"attempt_id": rangeAttemptID})
	}

	// Skip extraction if only doing DuckLake push on existing output
	if extractRequested {
		config := domain.OrchestratorConfig{
			StatusEmitter:     deps.Status,
			Log:               log,
			StartLedger:       startLedger,
			EndLedger:         endLedger,
			NumWorkers:        req.Workers,
			OutputDir:         req.OutputDir,
			BatchSize:         req.BatchSize,
			NetworkPassphrase: req.NetworkPassphrase,
			StorageType:       req.StorageType,
			Bucket:            req.Bucket,
			LedgersPerFile:    uint32(req.LedgersPerFile),
			FilesPerPartition: uint32(req.FilesPerPartition),
			EraID:             req.EraID,
			OnlyTables:        extractOnlySet,
			PrefetchBuffer:    req.PrefetchBuffer,
		}

		orchestrator := jobs.NewOrchestrator(config)

		startTime := time.Now()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "extract", nil, map[string]string{
			"output_dir": req.OutputDir,
		}); err != nil {
			log.Warn("flowctl progress report failed: %v", err)
		}

		_ = deps.Status.Working("", fmt.Sprintf("Extracting %d-%d", startLedger, endLedger), 0)
		if err := orchestrator.Run(); err != nil {
			// Reported before returning, so a watcher sees why rather than
			// seeing the record simply stop. error and done both survive
			// process exit by design.
			_ = deps.Status.Failed("", fmt.Sprintf("extract %d-%d: %v", startLedger, endLedger, err))
			return nil, chunkFailed("extract", err, "rerun_chunk")
		}
		effectiveOutput = orchestrator.OutputDir()
		manifestPath = orchestrator.ManifestPath()
		log.Field("Published attempt", "%s", effectiveOutput)

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "extract_complete", map[string]int64{
			"ledgers": int64(totalLedgers),
		}, nil); err != nil {
			log.Warn("flowctl progress report failed: %v", err)
		}

		elapsed := time.Since(startTime)
		throughput := float64(totalLedgers) / elapsed.Seconds()
		result.Extract = &ExtractResult{
			OutputDir:             effectiveOutput,
			Ledgers:               totalLedgers,
			DurationMS:            elapsed.Milliseconds(),
			Throughput:            throughput,
			CallGraphEdgesSkipped: int64(extract.CallGraphSkippedEdges()),
		}

		log.Line()
		log.Section("Summary")
		log.Field("Total ledgers", "%d", totalLedgers)
		log.Field("Elapsed time", "%s", elapsed.Round(time.Millisecond))
		log.Field("Throughput", "%.1f ledgers/sec", throughput)
		_ = deps.Events.Emit(events.TypeExtractCompleted, resource, map[string]any{
			"ledgers":            totalLedgers,
			"duration_ms":        elapsed.Milliseconds(),
			"ledgers_per_second": throughput,
			"output_dir":         effectiveOutput,
		})
		if skipped := extract.CallGraphSkippedEdges(); skipped > 0 {
			log.Field("Call-graph edges skipped", "%d (non-contract/unencodable address)", skipped)
		}
	} else {
		log.Info("Skipping extraction (no --bucket specified, using existing output)")
	}

	// Run quality validation if requested
	if req.RunValidate {
		log.Line()
		validator, err := adapters.NewValidator(effectiveOutput, log)
		if err != nil {
			log.Warn("validation setup failed: %v", err)
		} else {
			defer validator.Close()
			if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "validation", nil, nil); err != nil {
				log.Warn("flowctl progress report failed: %v", err)
			}
			passed, failed, err := validator.RunAll(ctx)
			result.Validate = &ValidateResult{Passed: passed, Failed: failed}
			_ = deps.Events.Emit(events.TypeValidateCompleted, resource, map[string]any{
				"passed": passed, "failed": failed,
			})
			if err != nil {
				_ = reporter.ReportChunkFailed(ctx, int64(startLedger), int64(endLedger), "validation", flowctlpb.FailureClass_FAILURE_CLASS_VERIFICATION, err.Error(), "inspect_output")
				return nil, fmt.Errorf("validation failed: %w", err)
			}
		}
	}

	// Push to DuckLake if configured
	if req.RunDuckLake {
		log.Line()
		log.Section("DuckLake Push")
		dlStart := time.Now()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "ducklake_setup", nil, map[string]string{
			"ducklake_metadata_schema": req.DlMetaSchema,
			"ducklake_schema":          req.DlSchema,
			"ducklake_view_schema":     req.DlViewSchema,
			"ducklake_data_path":       req.DlDataPath,
		}); err != nil {
			log.Warn("flowctl progress report failed: %v", err)
		}

		pusher, err := adapters.NewDuckLakePusher(adapters.DuckLakeConfig{
			CatalogDSN:      req.DlCatalog,
			DataPath:        req.DlDataPath,
			MetadataSchema:  req.DlMetaSchema,
			SchemaName:      req.DlSchema,
			ViewSchemaName:  req.DlViewSchema,
			S3KeyID:         req.B2KeyID,
			S3KeySecret:     req.B2KeySecret,
			S3Endpoint:      req.B2Endpoint,
			S3Region:        req.B2Region,
			BronzeSchemaSQL: req.DlSchemaSQL,
			StartLedger:     startLedger,
			EndLedger:       endLedger,
			OnlyTables:      duckLakeOnlySet,
			SkipTables:      duckLakeSkipSet,
			ManifestPath:    manifestPath,
		})
		if err != nil {
			return nil, chunkFailed("ducklake_setup", err, "retry_chunk")
		}
		defer pusher.Close()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "ducklake_push", nil, nil); err != nil {
			log.Warn("flowctl progress report failed: %v", err)
		}
		pushResult, err := pusher.Push(ctx, effectiveOutput)
		if err != nil {
			return nil, chunkFailed("ducklake_push", err, "retry_chunk")
		}

		log.Info("DuckLake push completed in %s", time.Since(dlStart).Round(time.Millisecond))
		_ = deps.Events.Emit(events.TypePushCompleted, resource, map[string]any{
			"total_rows":  pushResult.TotalRows,
			"duration_ms": time.Since(dlStart).Milliseconds(),
			"data_path":   req.DlDataPath,
		})
		result.DuckLake = &DuckLakeResult{
			DataPath:   req.DlDataPath,
			TotalRows:  pushResult.TotalRows,
			RowCounts:  pushResult.RowCounts,
			DurationMS: time.Since(dlStart).Milliseconds(),
		}

		// Frozen only now. Anything earlier would mark a range done whose
		// rows had not reached cold, and a resumed plan would skip it.
		if rangeStore != nil {
			if err := freezeRange(ctx, rangeStore, thisRange, rangeAttemptID, pushResult.TotalRows); err != nil {
				return nil, fmt.Errorf("range state: %w", err)
			}
			log.Info("Range %s frozen (%d rows)", thisRange, pushResult.TotalRows)
			result.Range.Frozen = true
			result.Range.Rows = pushResult.TotalRows
			_ = deps.Events.Emit(events.TypeRangeFrozen, resource, map[string]any{
				"rows": pushResult.TotalRows, "attempt_id": rangeAttemptID,
			})
		}
		if err := reporter.ReportChunkCompleted(ctx, int64(startLedger), int64(endLedger), true, pushResult.RowCounts, map[string]string{
			"gate":   "ducklake-push",
			"passed": "true",
		}); err != nil {
			log.Warn("flowctl completion report failed: %v", err)
		}
	}

	// Load hot buffer if PostgreSQL is configured
	if req.PgHost != "" {
		log.Line()
		log.Section("Hot Buffer Population")
		hotStart := time.Now()

		hot := &HotBufferResult{Host: req.PgHost, TailLedgers: uint32(req.TailLedgers)}
		result.HotBuffer = hot
		loader, err := adapters.NewHotBufferLoader(effectiveOutput, adapters.HotBufferConfig{
			Host:        req.PgHost,
			Port:        req.PgPort,
			Database:    req.PgDB,
			User:        req.PgUser,
			Password:    req.PgPassword,
			SSLMode:     req.PgSSL,
			TailLedgers: uint32(req.TailLedgers),
		})
		if err != nil {
			log.Warn("hot buffer connection failed: %v", err)
		} else {
			defer loader.Close()
			if err := loader.LoadTail(ctx, uint32(req.TailLedgers), endLedger); err != nil {
				log.Warn("hot buffer load had errors: %v", err)
			} else {
				hot.Loaded = true
			}
			hot.DurationMS = time.Since(hotStart).Milliseconds()
			log.Info("Hot buffer populated in %s", time.Since(hotStart).Round(time.Millisecond))
		}
	}

	log.Line()
	if err := adapters.GenerateCatalogReport(effectiveOutput, log); err != nil {
		log.Warn("catalog report failed: %v", err)
	}

	// The terminal report, and the one a watcher is actually waiting for.
	// done survives process exit by design, so a dashboard still shows the
	// outcome after the run is gone. Emitted here rather than earlier because
	// everything above -- extract, push, hot buffer, catalog report -- is part
	// of the run finishing.
	_ = deps.Status.Done("", fmt.Sprintf("Ingested ledgers %d-%d", startLedger, endLedger))

	if !req.RunDuckLake {
		if err := reporter.ReportChunkCompleted(ctx, int64(startLedger), int64(endLedger), false, map[string]int64{
			"ledgers": int64(totalLedgers),
		}, map[string]string{
			"gate":   "extract-only",
			"passed": "true",
		}); err != nil {
			log.Warn("flowctl completion report failed: %v", err)
		}
	}
	_ = deps.Events.Emit(events.TypeRunSucceeded, resource, map[string]any{
		"duration_ms": time.Since(started).Milliseconds(),
	})
	return result.finish(started), nil
}

// thisRangeLabel names the resource an event is about.
func thisRangeLabel(start, end uint32) string {
	return fmt.Sprintf("%d-%d", start, end)
}

// requireDuckLakeCredentials reports what is missing by the name the operator
// should now set.
//
// The old message named --ducklake-catalog, --b2-key-id and --b2-key-secret,
// which is the advice that makes the tool warn at you: those flags are
// deprecated precisely because they put secrets in argv. Naming the
// environment variables instead means following the error leaves you in the
// supported state.
func requireDuckLakeCredentials(req RunRequest) error {
	var missing []string
	if req.DlCatalog == "" {
		missing = append(missing, "DUCKLAKE_CATALOG_DSN")
	}
	if req.DlDataPath == "" {
		missing = append(missing, "--ducklake-data-path")
	}
	if req.B2KeyID == "" {
		missing = append(missing, "B2_KEY_ID")
	}
	if req.B2KeySecret == "" {
		missing = append(missing, "B2_KEY_SECRET")
	}
	if len(missing) == 0 {
		return nil
	}
	return exit.Usagef("--ducklake requires %s (each credential also accepts a _FILE form naming a file to read)",
		strings.Join(missing, ", "))
}

// freezeRange records a landed push even when the run is being cancelled.
//
// The push has already committed by the time this is called, so the rows are
// in DuckLake whatever happens next. Freezing under the run's own context
// would mean a SIGTERM arriving in this window leaves the range "freezing"
// with its data present: the next plan would wait out the lease and then redo
// work that was already done. A short uncancellable window is the lesser
// evil, and it is bounded so a hung catalog cannot keep the process alive.
func freezeRange(ctx context.Context, store *jobs.RangeStateStore, r jobs.LedgerRange, attemptID string, rows int64) error {
	freezeCtx, cancel := freezeContext(ctx)
	defer cancel()
	return store.Freeze(freezeCtx, r, attemptID, rows)
}

// freezeContext is split out so the property above can be asserted without a
// catalog: the returned context must outlive its parent's cancellation, and
// must still have a deadline.
func freezeContext(ctx context.Context) (context.Context, func()) {
	return context.WithTimeout(context.WithoutCancel(ctx), freezeGrace)
}

// runPlan reports which ranges still need building, taking no claim.
func runPlan(ctx context.Context, req RunRequest, deps Deps, operation string, started time.Time, startLedger, endLedger uint32) (*RunResult, error) {
	if req.DlCatalog == "" {
		return nil, exit.Usagef("--plan requires a catalog DSN (DUCKLAKE_CATALOG_DSN)")
	}
	size := uint32(req.ChunkSize)
	if size == 0 {
		// No chunking means the whole span is one range, matching what a
		// plain --start/--end run would claim.
		size = endLedger - startLedger + 1
	}
	store, err := jobs.NewRangeStateStore(ctx, req.DlCatalog, req.RangeStateTable, req.EraID)
	if err != nil {
		return nil, fmt.Errorf("range state: %w", err)
	}
	defer store.Close()

	plan, err := store.Plan(ctx, jobs.EnumerateRanges(startLedger, endLedger, size), jobs.DefaultClaimLease)
	if err != nil {
		return nil, fmt.Errorf("range state: %w", err)
	}
	result := newRunResult(operation, domain.Version, started)
	result.Range = &RangeResult{Start: startLedger, End: endLedger, Ledgers: endLedger - startLedger + 1}
	result.Plan = jobs.FormatPlan(plan)
	return result.finish(started), nil
}

// runEnrichment repairs missing Bronze contract balance fields in place. It
// is the one operation that was already a mode switch rather than a phase,
// and it returns without extracting.
func runEnrichment(ctx context.Context, req RunRequest, deps Deps, operation string, started time.Time) (*RunResult, error) {
	if req.Start == 0 || req.End == 0 || req.End < req.Start {
		return nil, exit.Usagef("--enrich-contract-balances requires a valid inclusive --start/--end range")
	}
	if req.EnrichmentChunkSize == 0 {
		return nil, exit.Usagef("--enrichment-chunk-size must be greater than zero")
	}
	if err := requireDuckLakeCredentials(req); err != nil {
		return nil, fmt.Errorf("--enrich-contract-balances: %w", err)
	}
	if !req.EnrichmentDryRun && strings.TrimSpace(req.EnrichmentRunID) == "" {
		return nil, exit.Usagef("--enrichment-run-id is required unless --enrichment-dry-run is set")
	}

	enricher, err := adapters.NewContractBalanceEnricher(adapters.ContractBalanceEnricherConfig{
		DuckLake: adapters.DuckLakeConfig{
			CatalogDSN:     req.DlCatalog,
			DataPath:       req.DlDataPath,
			MetadataSchema: req.DlMetaSchema,
			S3KeyID:        req.B2KeyID,
			S3KeySecret:    req.B2KeySecret,
			S3Endpoint:     req.B2Endpoint,
			S3Region:       req.B2Region,
		},
		StartLedger:    int64(req.Start),
		EndLedger:      int64(req.End),
		ChunkSize:      int64(req.EnrichmentChunkSize),
		RunID:          req.EnrichmentRunID,
		Network:        req.NetworkPassphrase,
		VersionLabel:   domain.Version,
		ManifestSchema: req.EnrichmentManifestSchema,
		DryRun:         req.EnrichmentDryRun,
		Resume:         req.EnrichmentResume,
	})
	if err != nil {
		return nil, fmt.Errorf("contract-balance enrichment setup failed: %w", err)
	}
	defer enricher.Close()
	repaired, err := enricher.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("contract-balance enrichment failed: %w", err)
	}
	result := newRunResult(operation, domain.Version, started)
	result.Range = &RangeResult{Start: uint32(req.Start), End: uint32(req.End), Ledgers: uint32(req.End - req.Start + 1)}
	result.Repair = &RepairResult{
		DryRun:      req.EnrichmentDryRun,
		Candidates:  repaired.CandidateRows,
		Decoded:     repaired.DecodedRows,
		Updated:     repaired.UpdatedRows,
		SkippedRows: repaired.SkippedRows,
	}
	return result.finish(started), nil
}
