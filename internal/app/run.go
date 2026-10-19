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
// ONE THING IT STILL DOES WRONG, deliberately left for its own change: it
// calls log.Fatal on failure, 38 times, 18 of them inside closures including
// fatalChunk. An application operation should return an error and let the
// adapter decide the exit code -- which is also what makes it testable, per
// §10's first gate. Converting them is a control-flow rewrite rather than a
// move, and doing it in the same change as relocating 69 files would make a
// move bug indistinguishable from a rewrite bug. It pairs naturally with
// documented exit classes, which the standard also wants and which this tool
// also lacks.
//
// So the signature returns nothing today. That is honest rather than
// aspirational: a function that cannot fail gracefully should not claim an
// error.

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	extract "github.com/withObsrvr/stellar-extract"
	flowcomponent "github.com/withobsrvr/flowctl/pkg/component"
	flowctlpb "github.com/withobsrvr/flowctl/proto"

	"github.com/withObsrvr/stellar-ingest/internal/adapters"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/jobs"
	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// Run executes the request. It exits the process on failure; see above.
func Run(ctx context.Context, req RunRequest, statusEmitter *status.Emitter) {
	if req.EnrichContractBalances {
		if req.Start == 0 || req.End == 0 || req.End < req.Start {
			log.Fatalf("--enrich-contract-balances requires a valid inclusive --start/--end range")
		}
		if req.EnrichmentChunkSize == 0 {
			log.Fatal("--enrichment-chunk-size must be greater than zero")
		}
		if req.DlCatalog == "" || req.DlDataPath == "" || req.B2KeyID == "" || req.B2KeySecret == "" {
			log.Fatal("--enrich-contract-balances requires --ducklake-catalog, --ducklake-data-path, --b2-key-id, and --b2-key-secret")
		}
		if !req.EnrichmentDryRun && strings.TrimSpace(req.EnrichmentRunID) == "" {
			log.Fatal("--enrichment-run-id is required unless --enrichment-dry-run is set")
		}

		ctx := context.Background()
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
			log.Fatalf("contract-balance enrichment setup failed: %v", err)
		}
		defer enricher.Close()
		result, err := enricher.Run(ctx)
		if err != nil {
			log.Fatalf("contract-balance enrichment failed: %v", err)
		}
		fmt.Printf(
			"Contract-balance enrichment complete: dry_run=%t candidates=%d decoded=%d updated=%d skipped=%d\n",
			req.EnrichmentDryRun, result.CandidateRows, result.DecodedRows, result.UpdatedRows, result.SkippedRows,
		)
		return
	}

	// Validate required flags
	if req.Start == 0 {
		log.Fatal("--start is required and must be > 0")
	}
	if req.End == 0 {
		log.Fatal("--end is required and must be > 0")
	}
	if req.OutputDir == "" {
		log.Fatal("--output is required")
	}
	extractRequested := domain.ExtractionRequested(req.StorageType, req.Bucket)
	if !extractRequested && !req.RunDuckLake {
		log.Fatal("--bucket is required (unless using CAPTIVE_CORE or --ducklake with existing output)")
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
		log.Fatal(err)
	}

	if req.Workers < 1 {
		log.Fatal("--workers must be >= 1")
	}

	if req.BatchSize < 1 {
		log.Fatal("--batch-size must be >= 1")
	}
	if req.ChunkSize > 0 && !extractRequested {
		log.Fatal("--chunk-size requires an extraction source")
	}
	if req.ChunkConcurrency < 1 {
		log.Fatal("--chunk-concurrency must be at least 1")
	}
	if req.ChunkConcurrency > 1 && req.ChunkSize == 0 {
		log.Fatal("--chunk-concurrency requires --chunk-size: there is nothing to run concurrently without chunks")
	}
	if req.ChunkConcurrency > 1 && !req.RangeState {
		// Concurrent chunks are separate processes writing the same DuckLake
		// tables. Without the range catalog nothing stops two runs taking the
		// same chunk, and a crashed chunk leaves no record that it needs
		// redoing -- which is most of what makes concurrency safe here.
		log.Fatal("--chunk-concurrency above 1 requires --range-state")
	}
	if uint64(req.ChunkSize) > uint64(^uint32(0)) {
		log.Fatal("--chunk-size exceeds the supported uint32 ledger range")
	}
	if req.ChunkSize > 0 && req.PgHost != "" {
		log.Fatal("--chunk-size cannot currently be combined with --pg-host; populate the hot buffer after the chunked backfill")
	}

	switch req.StorageType {
	case "GCS", "S3", "FS", "XDR", "RPC", "CAPTIVE_CORE":
		// valid
	default:
		log.Fatalf("--storage-type must be one of: GCS, S3, FS, XDR, RPC, CAPTIVE_CORE (got %q)", req.StorageType)
	}
	if err := domain.ValidateTableSelection(extractOnlySet, "only-tables"); err != nil {
		log.Fatal(err)
	}
	if err := domain.ValidateTableSelection(duckLakeOnlySet, "ducklake-only-tables"); err != nil {
		log.Fatal(err)
	}
	if err := domain.ValidateTableSelection(duckLakeSkipSet, "ducklake-skip-tables"); err != nil {
		log.Fatal(err)
	}

	// --plan answers "what is left" without doing anything. Read-only: it
	// takes no claim, so running it while a backfill is in flight is safe and
	// shows that backfill's live claims.
	//
	// This is the postcondition-driven half of Item 5. Dispatching ranges by
	// hand is what made a multi-day backfill depend on an operator's notes;
	// here the catalog is asked instead.
	if req.PlanOnly {
		if req.DlCatalog == "" {
			log.Fatal("--plan requires --ducklake-catalog")
		}
		size := uint32(req.ChunkSize)
		if size == 0 {
			// No chunking means the whole span is one range, matching what a
			// plain --start/--end run would claim.
			size = endLedger - startLedger + 1
		}
		store, err := jobs.NewRangeStateStore(context.Background(), req.DlCatalog, req.RangeStateTable, req.EraID)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		defer store.Close()

		plan, err := store.Plan(context.Background(), jobs.EnumerateRanges(startLedger, endLedger, size), jobs.DefaultClaimLease)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		fmt.Print(jobs.FormatPlan(plan))
		return
	}

	if req.ChunkSize > 0 {
		if err := jobs.RunChunkedCommand(context.Background(), os.Args[1:], startLedger, endLedger, uint32(req.ChunkSize), req.OutputDir, req.ChunkConcurrency); err != nil {
			log.Fatalf("chunked backfill failed: %v", err)
		}
		return
	}

	// Ensure output directory exists
	if err := os.MkdirAll(req.OutputDir, 0o755); err != nil {
		log.Fatalf("Failed to create output directory %q: %v", req.OutputDir, err)
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
		log.Fatalf("Flowctl setup failed: %v", err)
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
		log.Fatalf("Flowctl registration failed: %v", err)
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go reporter.StartHeartbeatLoop(heartbeatCtx, nil, nil)

	fatalChunk := func(phase string, err error, action string) {
		if err != nil {
			_ = reporter.ReportChunkFailed(ctx, int64(startLedger), int64(endLedger), phase, domain.ClassifyFlowctlFailure(err), err.Error(), action)
			log.Fatalf("%s failed: %v", phase, err)
		}
	}

	totalLedgers := endLedger - startLedger + 1

	fmt.Println("=== Stellar History Loader ===")
	fmt.Printf("Ledger range:      %d - %d (%d ledgers)\n", startLedger, endLedger, totalLedgers)
	fmt.Printf("Workers:           %d\n", req.Workers)
	fmt.Printf("Batch size:        %d\n", req.BatchSize)
	fmt.Printf("Output directory:  %s\n", req.OutputDir)
	fmt.Printf("Storage type:      %s\n", req.StorageType)
	fmt.Printf("Bucket:            %s\n", req.Bucket)
	fmt.Printf("Network:           %s\n", req.NetworkPassphrase)
	if len(extractOnlySet) > 0 {
		fmt.Printf("Extract tables:    %s\n", domain.TableSetKey(extractOnlySet))
	}
	if len(duckLakeOnlySet) > 0 {
		fmt.Printf("DuckLake tables:   %s\n", domain.TableSetKey(duckLakeOnlySet))
	}
	fmt.Println()

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
	rangeAttemptID := jobs.NewAttemptID()
	if req.RangeState {
		if req.DlCatalog == "" {
			log.Fatal("--range-state requires --ducklake-catalog")
		}
		// frozen means "pushed to DuckLake". Claiming a range this run can
		// never push would leave it freezing until the lease expired.
		if !req.RunDuckLake {
			log.Fatal("--range-state requires --ducklake: a range is frozen when the push lands, not when extraction finishes")
		}
		var err error
		rangeStore, err = jobs.NewRangeStateStore(context.Background(), req.DlCatalog, req.RangeStateTable, req.EraID)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		defer rangeStore.Close()

		claimed, err := rangeStore.Claim(context.Background(), thisRange, rangeAttemptID, domain.Version, jobs.DefaultClaimLease)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		if !claimed {
			// Either already frozen, or held by a live claim. Both mean there
			// is nothing to do here, and both are a success: a resumed plan
			// walking a range it already finished must not fail the run.
			// The marker is for the chunk runner; the sentence is for a
			// human reading a single-range dispatch.
			fmt.Printf("%s Range %s is already frozen or claimed by another run; nothing to do.\n",
				jobs.RangeSkippedMarker, thisRange)
			return
		}
		fmt.Printf("Range %s claimed (freezing)\n", thisRange)
	}

	// Skip extraction if only doing DuckLake push on existing output
	if extractRequested {
		config := domain.OrchestratorConfig{
			StatusEmitter:     statusEmitter,
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
		}

		orchestrator := jobs.NewOrchestrator(config)

		startTime := time.Now()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "extract", nil, map[string]string{
			"output_dir": req.OutputDir,
		}); err != nil {
			log.Printf("Flowctl progress report failed: %v", err)
		}

		_ = statusEmitter.Working("", fmt.Sprintf("Extracting %d-%d", startLedger, endLedger), 0)
		if err := orchestrator.Run(); err != nil {
			// Reported before exiting, so a watcher sees why rather than
			// seeing the record simply stop. error and done both survive
			// process exit by design.
			_ = statusEmitter.Failed("", fmt.Sprintf("extract %d-%d: %v", startLedger, endLedger, err))
			fatalChunk("extract", err, "rerun_chunk")
		}
		effectiveOutput = orchestrator.OutputDir()
		manifestPath = orchestrator.ManifestPath()
		fmt.Printf("Published attempt:    %s\n", effectiveOutput)

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "extract_complete", map[string]int64{
			"ledgers": int64(totalLedgers),
		}, nil); err != nil {
			log.Printf("Flowctl progress report failed: %v", err)
		}

		elapsed := time.Since(startTime)
		throughput := float64(totalLedgers) / elapsed.Seconds()

		fmt.Println()
		fmt.Println("=== Summary ===")
		fmt.Printf("Total ledgers processed: %d\n", totalLedgers)
		fmt.Printf("Elapsed time:           %s\n", elapsed.Round(time.Millisecond))
		fmt.Printf("Throughput:             %.1f ledgers/sec\n", throughput)
		if skipped := extract.CallGraphSkippedEdges(); skipped > 0 {
			fmt.Printf("Call-graph edges skipped (non-contract/unencodable address): %d\n", skipped)
		}
	} else {
		fmt.Println("Skipping extraction (no --bucket specified, using existing output)")
	}

	// Run quality validation if requested
	if req.RunValidate {
		fmt.Println()
		validator, err := adapters.NewValidator(effectiveOutput)
		if err != nil {
			log.Printf("Validation setup failed: %v", err)
		} else {
			defer validator.Close()
			if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "validation", nil, nil); err != nil {
				log.Printf("Flowctl progress report failed: %v", err)
			}
			if err := validator.RunAll(ctx); err != nil {
				_ = reporter.ReportChunkFailed(ctx, int64(startLedger), int64(endLedger), "validation", flowctlpb.FailureClass_FAILURE_CLASS_VERIFICATION, err.Error(), "inspect_output")
				log.Fatalf("Validation failed: %v", err)
			}
		}
	}

	// Push to DuckLake if configured
	if req.RunDuckLake {
		if req.DlCatalog == "" || req.DlDataPath == "" || req.B2KeyID == "" || req.B2KeySecret == "" {
			log.Fatal("--ducklake requires --ducklake-catalog, --ducklake-data-path, --b2-key-id, --b2-key-secret")
		}

		fmt.Println()
		fmt.Println("=== DuckLake Push ===")
		dlStart := time.Now()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "ducklake_setup", nil, map[string]string{
			"ducklake_metadata_schema": req.DlMetaSchema,
			"ducklake_schema":          req.DlSchema,
			"ducklake_view_schema":     req.DlViewSchema,
			"ducklake_data_path":       req.DlDataPath,
		}); err != nil {
			log.Printf("Flowctl progress report failed: %v", err)
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
			fatalChunk("ducklake_setup", err, "retry_chunk")
		}
		defer pusher.Close()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "ducklake_push", nil, nil); err != nil {
			log.Printf("Flowctl progress report failed: %v", err)
		}
		pushResult, err := pusher.Push(ctx, effectiveOutput)
		if err != nil {
			fatalChunk("ducklake_push", err, "retry_chunk")
		}

		fmt.Printf("DuckLake push completed in %s\n", time.Since(dlStart).Round(time.Millisecond))

		// Frozen only now. Anything earlier would mark a range done whose
		// rows had not reached cold, and a resumed plan would skip it.
		if rangeStore != nil {
			if err := rangeStore.Freeze(context.Background(), thisRange, rangeAttemptID, pushResult.TotalRows); err != nil {
				log.Fatalf("range state: %v", err)
			}
			fmt.Printf("Range %s frozen (%d rows)\n", thisRange, pushResult.TotalRows)
		}
		if err := reporter.ReportChunkCompleted(ctx, int64(startLedger), int64(endLedger), true, pushResult.RowCounts, map[string]string{
			"gate":   "ducklake-push",
			"passed": "true",
		}); err != nil {
			log.Printf("Flowctl completion report failed: %v", err)
		}
	}

	// Load hot buffer if PostgreSQL is configured
	if req.PgHost != "" {
		fmt.Println()
		fmt.Println("=== Hot Buffer Population ===")
		hotStart := time.Now()

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
			log.Printf("Hot buffer connection failed: %v", err)
		} else {
			defer loader.Close()
			if err := loader.LoadTail(context.Background(), uint32(req.TailLedgers), endLedger); err != nil {
				log.Printf("Hot buffer load had errors: %v", err)
			}
			fmt.Printf("Hot buffer populated in %s\n", time.Since(hotStart).Round(time.Millisecond))
		}
	}

	fmt.Println()
	if err := adapters.GenerateCatalogReport(effectiveOutput); err != nil {
		log.Printf("Warning: catalog report failed: %v", err)
	}

	// The terminal report, and the one a watcher is actually waiting for.
	// done survives process exit by design, so a dashboard still shows the
	// outcome after the run is gone. Emitted here rather than earlier because
	// everything above -- extract, push, hot buffer, catalog report -- is part
	// of the run finishing.
	_ = statusEmitter.Done("", fmt.Sprintf("Ingested ledgers %d-%d", startLedger, endLedger))

	if !req.RunDuckLake {
		if err := reporter.ReportChunkCompleted(ctx, int64(startLedger), int64(endLedger), false, map[string]int64{
			"ledgers": int64(totalLedgers),
		}, map[string]string{
			"gate":   "extract-only",
			"passed": "true",
		}); err != nil {
			log.Printf("Flowctl completion report failed: %v", err)
		}
	}
}
