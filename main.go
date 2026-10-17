package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	flowcomponent "github.com/withobsrvr/flowctl/pkg/component"
	flowctlpb "github.com/withobsrvr/flowctl/proto"

	extract "github.com/withObsrvr/stellar-extract"
)

// Version is set at build time via -ldflags "-X main.Version=..."
var Version = "dev"

func main() {
	start := flag.Uint("start", 0, "Start ledger sequence (required)")
	end := flag.Uint("end", 0, "End ledger sequence (required)")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of parallel workers")
	output := flag.String("output", "", "Output directory for Parquet files (required)")
	batchSize := flag.Int("batch-size", 1000, "Ledgers per extraction batch")
	chunkSize := flag.Uint("chunk-size", 0, "Maximum ledgers per sequential extract/validate/publish chunk; 0 processes the range as one chunk")
	networkPassphrase := flag.String("network-passphrase", "Test SDF Network ; September 2015", "Stellar network passphrase")
	storageType := flag.String("storage-type", "FS", "Backend storage type: GCS, S3, RPC, CAPTIVE_CORE, XDR, FS")
	bucket := flag.String("bucket", "", "Storage bucket/path for ledger data (required)")
	ledgersPerFile := flag.Uint("ledgers-per-file", 1, "Ledgers per archive file (GCS/S3)")
	filesPerPartition := flag.Uint("files-per-partition", 64000, "Files per archive partition (GCS/S3)")
	runValidate := flag.Bool("validate", false, "Run quality validation checks after extraction")
	runDuckLake := flag.Bool("ducklake", false, "Push bronze Parquet to DuckLake (B2 + catalog)")
	onlyTables := flag.String("only-tables", "", "Comma-separated extractor/source/DuckLake table names to extract; empty extracts all")
	dlCatalog := flag.String("ducklake-catalog", "", "PostgreSQL catalog DSN for DuckLake")
	dlDataPath := flag.String("ducklake-data-path", "", "S3/B2 bucket path (e.g., s3://obsrvr-lake-testnet/)")
	dlMetaSchema := flag.String("ducklake-metadata-schema", "bronze_meta", "DuckLake metadata schema")
	dlSchemaSQL := flag.String("ducklake-schema-sql", "", "Path to v3_bronze_schema.sql (optional)")
	dlOnlyTables := flag.String("ducklake-only-tables", "", "Comma-separated source or DuckLake table names to push; defaults to --only-tables when set")
	dlSkipTables := flag.String("ducklake-skip-tables", "", "Comma-separated source or DuckLake table names to skip during push")
	rangeState := flag.Bool("range-state", false, "Record this ledger range as freezing/frozen in the catalog so an interrupted backfill can resume (requires --ducklake-catalog)")
	rangeStateTable := flag.String("range-state-table", "public.ingest_range_state", "Table holding range state")
	planOnly := flag.Bool("plan", false, "Print which ledger ranges still need building, then exit without doing any work (requires --ducklake-catalog)")
	chunkConcurrency := flag.Int("chunk-concurrency", 1, "Chunks to build at once when --chunk-size is set; 1 is sequential. Memory scales with this times --workers")
	b2KeyID := flag.String("b2-key-id", "", "B2/S3 access key ID")
	b2KeySecret := flag.String("b2-key-secret", "", "B2/S3 secret access key")
	b2Endpoint := flag.String("b2-endpoint", "s3.us-west-004.backblazeb2.com", "B2/S3 endpoint")
	b2Region := flag.String("b2-region", "us-west-004", "B2/S3 region")
	pgHost := flag.String("pg-host", "", "PostgreSQL host for hot buffer (enables hot buffer loading)")
	pgPort := flag.Int("pg-port", 25060, "PostgreSQL port")
	pgDB := flag.String("pg-database", "stellar_hot", "PostgreSQL database")
	pgUser := flag.String("pg-user", "", "PostgreSQL user")
	pgPassword := flag.String("pg-password", "", "PostgreSQL password")
	pgSSL := flag.String("pg-sslmode", "require", "PostgreSQL SSL mode")
	tailLedgers := flag.Uint("tail-ledgers", 100000, "Number of recent ledgers to load into hot buffer")
	eraID := flag.String("era-id", "", "Era identifier for DuckLake partitioning (optional)")
	enrichContractBalances := flag.Bool("enrich-contract-balances", false, "Repair missing Bronze contract balance_holder and balance fields in place")
	enrichmentChunkSize := flag.Uint("enrichment-chunk-size", 100000, "Ledgers per contract-balance enrichment transaction")
	enrichmentRunID := flag.String("enrichment-run-id", "", "Durable manifest run ID (required for mutating enrichment)")
	enrichmentDryRun := flag.Bool("enrichment-dry-run", false, "Decode and count contract-balance candidates without mutating DuckLake")
	enrichmentResume := flag.Bool("enrichment-resume", true, "Skip chunks already completed under the enrichment run ID")
	enrichmentManifestSchema := flag.String("enrichment-manifest-schema", "bronze_operations", "DuckLake schema for the enrichment audit manifest")

	flag.Parse()
	if *enrichContractBalances {
		if *start == 0 || *end == 0 || *end < *start {
			log.Fatalf("--enrich-contract-balances requires a valid inclusive --start/--end range")
		}
		if *enrichmentChunkSize == 0 {
			log.Fatal("--enrichment-chunk-size must be greater than zero")
		}
		if *dlCatalog == "" || *dlDataPath == "" || *b2KeyID == "" || *b2KeySecret == "" {
			log.Fatal("--enrich-contract-balances requires --ducklake-catalog, --ducklake-data-path, --b2-key-id, and --b2-key-secret")
		}
		if !*enrichmentDryRun && strings.TrimSpace(*enrichmentRunID) == "" {
			log.Fatal("--enrichment-run-id is required unless --enrichment-dry-run is set")
		}

		ctx := context.Background()
		enricher, err := NewContractBalanceEnricher(ContractBalanceEnricherConfig{
			DuckLake: DuckLakeConfig{
				CatalogDSN:     *dlCatalog,
				DataPath:       *dlDataPath,
				MetadataSchema: *dlMetaSchema,
				S3KeyID:        *b2KeyID,
				S3KeySecret:    *b2KeySecret,
				S3Endpoint:     *b2Endpoint,
				S3Region:       *b2Region,
			},
			StartLedger:    int64(*start),
			EndLedger:      int64(*end),
			ChunkSize:      int64(*enrichmentChunkSize),
			RunID:          *enrichmentRunID,
			Network:        *networkPassphrase,
			VersionLabel:   Version,
			ManifestSchema: *enrichmentManifestSchema,
			DryRun:         *enrichmentDryRun,
			Resume:         *enrichmentResume,
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
			*enrichmentDryRun, result.CandidateRows, result.DecodedRows, result.UpdatedRows, result.SkippedRows,
		)
		return
	}

	// Validate required flags
	if *start == 0 {
		log.Fatal("--start is required and must be > 0")
	}
	if *end == 0 {
		log.Fatal("--end is required and must be > 0")
	}
	if *output == "" {
		log.Fatal("--output is required")
	}
	extractRequested := extractionRequested(*storageType, *bucket)
	if !extractRequested && !*runDuckLake {
		log.Fatal("--bucket is required (unless using CAPTIVE_CORE or --ducklake with existing output)")
	}

	startLedger := uint32(*start)
	endLedger := uint32(*end)
	extractOnlySet := parseCSVSet(*onlyTables)
	duckLakeOnlySet := parseCSVSet(*dlOnlyTables)
	if len(duckLakeOnlySet) == 0 && len(extractOnlySet) > 0 {
		duckLakeOnlySet = extractOnlySet
	}
	duckLakeSkipSet := parseCSVSet(*dlSkipTables)

	if endLedger <= startLedger {
		log.Fatalf("--end (%d) must be greater than --start (%d)", endLedger, startLedger)
	}

	if *workers < 1 {
		log.Fatal("--workers must be >= 1")
	}

	if *batchSize < 1 {
		log.Fatal("--batch-size must be >= 1")
	}
	if *chunkSize > 0 && !extractRequested {
		log.Fatal("--chunk-size requires an extraction source")
	}
	if *chunkConcurrency < 1 {
		log.Fatal("--chunk-concurrency must be at least 1")
	}
	if *chunkConcurrency > 1 && *chunkSize == 0 {
		log.Fatal("--chunk-concurrency requires --chunk-size: there is nothing to run concurrently without chunks")
	}
	if *chunkConcurrency > 1 && !*rangeState {
		// Concurrent chunks are separate processes writing the same DuckLake
		// tables. Without the range catalog nothing stops two runs taking the
		// same chunk, and a crashed chunk leaves no record that it needs
		// redoing -- which is most of what makes concurrency safe here.
		log.Fatal("--chunk-concurrency above 1 requires --range-state")
	}
	if uint64(*chunkSize) > uint64(^uint32(0)) {
		log.Fatal("--chunk-size exceeds the supported uint32 ledger range")
	}
	if *chunkSize > 0 && *pgHost != "" {
		log.Fatal("--chunk-size cannot currently be combined with --pg-host; populate the hot buffer after the chunked backfill")
	}

	switch *storageType {
	case "GCS", "S3", "FS", "XDR", "RPC", "CAPTIVE_CORE":
		// valid
	default:
		log.Fatalf("--storage-type must be one of: GCS, S3, FS, XDR, RPC, CAPTIVE_CORE (got %q)", *storageType)
	}
	if err := validateTableSelection(extractOnlySet, "only-tables"); err != nil {
		log.Fatal(err)
	}
	if err := validateTableSelection(duckLakeOnlySet, "ducklake-only-tables"); err != nil {
		log.Fatal(err)
	}
	if err := validateTableSelection(duckLakeSkipSet, "ducklake-skip-tables"); err != nil {
		log.Fatal(err)
	}

	// --plan answers "what is left" without doing anything. Read-only: it
	// takes no claim, so running it while a backfill is in flight is safe and
	// shows that backfill's live claims.
	//
	// This is the postcondition-driven half of Item 5. Dispatching ranges by
	// hand is what made a multi-day backfill depend on an operator's notes;
	// here the catalog is asked instead.
	if *planOnly {
		if *dlCatalog == "" {
			log.Fatal("--plan requires --ducklake-catalog")
		}
		size := uint32(*chunkSize)
		if size == 0 {
			// No chunking means the whole span is one range, matching what a
			// plain --start/--end run would claim.
			size = endLedger - startLedger + 1
		}
		store, err := NewRangeStateStore(context.Background(), *dlCatalog, *rangeStateTable, *eraID)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		defer store.Close()

		plan, err := store.Plan(context.Background(), enumerateRanges(startLedger, endLedger, size), defaultClaimLease)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		fmt.Print(formatPlan(plan))
		return
	}

	if *chunkSize > 0 {
		if err := runChunkedCommand(context.Background(), os.Args[1:], startLedger, endLedger, uint32(*chunkSize), *output, *chunkConcurrency); err != nil {
			log.Fatalf("chunked backfill failed: %v", err)
		}
		return
	}

	// Ensure output directory exists
	if err := os.MkdirAll(*output, 0o755); err != nil {
		log.Fatalf("Failed to create output directory %q: %v", *output, err)
	}
	effectiveOutput := *output
	manifestPath := ""
	if !extractRequested {
		candidate := filepath.Join(effectiveOutput, "manifest.json")
		if _, err := os.Stat(candidate); err == nil {
			manifestPath = candidate
		}
	}

	ctx := context.Background()
	reporter, err := flowcomponent.NewReporter(ctx, flowcomponent.ConfigFromEnv())
	if err != nil {
		log.Fatalf("Flowctl setup failed: %v", err)
	}
	defer reporter.Close()
	if err := reporter.Register(ctx, flowctlpb.ServiceType_SERVICE_TYPE_SOURCE, map[string]string{
		"component":    "stellar-history-loader",
		"version":      Version,
		"network":      *networkPassphrase,
		"storage_type": *storageType,
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
			_ = reporter.ReportChunkFailed(ctx, int64(startLedger), int64(endLedger), phase, classifyFlowctlFailure(err), err.Error(), action)
			log.Fatalf("%s failed: %v", phase, err)
		}
	}

	totalLedgers := endLedger - startLedger + 1

	fmt.Println("=== Stellar History Loader ===")
	fmt.Printf("Ledger range:      %d - %d (%d ledgers)\n", startLedger, endLedger, totalLedgers)
	fmt.Printf("Workers:           %d\n", *workers)
	fmt.Printf("Batch size:        %d\n", *batchSize)
	fmt.Printf("Output directory:  %s\n", *output)
	fmt.Printf("Storage type:      %s\n", *storageType)
	fmt.Printf("Bucket:            %s\n", *bucket)
	fmt.Printf("Network:           %s\n", *networkPassphrase)
	if len(extractOnlySet) > 0 {
		fmt.Printf("Extract tables:    %s\n", tableSetKey(extractOnlySet))
	}
	if len(duckLakeOnlySet) > 0 {
		fmt.Printf("DuckLake tables:   %s\n", tableSetKey(duckLakeOnlySet))
	}
	fmt.Println()

	// Durable range state. Claimed before any work and frozen only after the
	// DuckLake push returns, because a range is not done when its Parquet
	// exists -- it is done when the push lands.
	//
	// This sits on the single-range path deliberately: runChunkedCommand
	// re-invokes this binary per chunk, so chunked backfills inherit it
	// without the chunk runner knowing anything about it, and so does a
	// one-range Nomad dispatch.
	var rangeStore *RangeStateStore
	thisRange := LedgerRange{Start: startLedger, End: endLedger}
	rangeAttemptID := newAttemptID()
	if *rangeState {
		if *dlCatalog == "" {
			log.Fatal("--range-state requires --ducklake-catalog")
		}
		// frozen means "pushed to DuckLake". Claiming a range this run can
		// never push would leave it freezing until the lease expired.
		if !*runDuckLake {
			log.Fatal("--range-state requires --ducklake: a range is frozen when the push lands, not when extraction finishes")
		}
		var err error
		rangeStore, err = NewRangeStateStore(context.Background(), *dlCatalog, *rangeStateTable, *eraID)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		defer rangeStore.Close()

		claimed, err := rangeStore.Claim(context.Background(), thisRange, rangeAttemptID, Version, defaultClaimLease)
		if err != nil {
			log.Fatalf("range state: %v", err)
		}
		if !claimed {
			// Either already frozen, or held by a live claim. Both mean there
			// is nothing to do here, and both are a success: a resumed plan
			// walking a range it already finished must not fail the run.
			fmt.Printf("Range %s is already frozen or claimed by another run; nothing to do.\n", thisRange)
			return
		}
		fmt.Printf("Range %s claimed (freezing)\n", thisRange)
	}

	// Skip extraction if only doing DuckLake push on existing output
	if extractRequested {
		config := OrchestratorConfig{
			StartLedger:       startLedger,
			EndLedger:         endLedger,
			NumWorkers:        *workers,
			OutputDir:         *output,
			BatchSize:         *batchSize,
			NetworkPassphrase: *networkPassphrase,
			StorageType:       *storageType,
			Bucket:            *bucket,
			LedgersPerFile:    uint32(*ledgersPerFile),
			FilesPerPartition: uint32(*filesPerPartition),
			EraID:             *eraID,
			OnlyTables:        extractOnlySet,
		}

		orchestrator := NewOrchestrator(config)

		startTime := time.Now()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "extract", nil, map[string]string{
			"output_dir": *output,
		}); err != nil {
			log.Printf("Flowctl progress report failed: %v", err)
		}

		if err := orchestrator.Run(); err != nil {
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
	if *runValidate {
		fmt.Println()
		validator, err := NewValidator(effectiveOutput)
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
	if *runDuckLake {
		if *dlCatalog == "" || *dlDataPath == "" || *b2KeyID == "" || *b2KeySecret == "" {
			log.Fatal("--ducklake requires --ducklake-catalog, --ducklake-data-path, --b2-key-id, --b2-key-secret")
		}

		fmt.Println()
		fmt.Println("=== DuckLake Push ===")
		dlStart := time.Now()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "ducklake_setup", nil, map[string]string{
			"ducklake_metadata_schema": *dlMetaSchema,
			"ducklake_data_path":       *dlDataPath,
		}); err != nil {
			log.Printf("Flowctl progress report failed: %v", err)
		}

		pusher, err := NewDuckLakePusher(DuckLakeConfig{
			CatalogDSN:      *dlCatalog,
			DataPath:        *dlDataPath,
			MetadataSchema:  *dlMetaSchema,
			S3KeyID:         *b2KeyID,
			S3KeySecret:     *b2KeySecret,
			S3Endpoint:      *b2Endpoint,
			S3Region:        *b2Region,
			BronzeSchemaSQL: *dlSchemaSQL,
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
	if *pgHost != "" {
		fmt.Println()
		fmt.Println("=== Hot Buffer Population ===")
		hotStart := time.Now()

		loader, err := NewHotBufferLoader(effectiveOutput, HotBufferConfig{
			Host:        *pgHost,
			Port:        *pgPort,
			Database:    *pgDB,
			User:        *pgUser,
			Password:    *pgPassword,
			SSLMode:     *pgSSL,
			TailLedgers: uint32(*tailLedgers),
		})
		if err != nil {
			log.Printf("Hot buffer connection failed: %v", err)
		} else {
			defer loader.Close()
			if err := loader.LoadTail(context.Background(), uint32(*tailLedgers), endLedger); err != nil {
				log.Printf("Hot buffer load had errors: %v", err)
			}
			fmt.Printf("Hot buffer populated in %s\n", time.Since(hotStart).Round(time.Millisecond))
		}
	}

	fmt.Println()
	if err := GenerateCatalogReport(effectiveOutput); err != nil {
		log.Printf("Warning: catalog report failed: %v", err)
	}

	if !*runDuckLake {
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

func extractionRequested(storageType, bucket string) bool {
	return bucket != "" || storageType == "CAPTIVE_CORE"
}

func parseCSVSet(value string) map[string]bool {
	result := map[string]bool{}
	for _, part := range strings.Split(value, ",") {
		item := strings.TrimSpace(part)
		if item != "" {
			result[item] = true
		}
	}
	return result
}

func classifyFlowctlFailure(err error) flowctlpb.FailureClass {
	msg := strings.ToLower(err.Error())
	switch {
	// Check non-retryable data/schema buckets first. Infrastructure substrings
	// like "catalog"/"postgres" routinely appear inside DuckLake schema/data error
	// messages; if the broad infrastructure case were evaluated first, a genuinely
	// non-retryable poison chunk would be classed retryable and an orchestrator
	// would retry it forever instead of surfacing it to a human.
	case strings.Contains(msg, "schema"), strings.Contains(msg, "column"), strings.Contains(msg, "type"), strings.Contains(msg, "parquet"):
		return flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_SCHEMA
	case strings.Contains(msg, "utf"), strings.Contains(msg, "encoding"), strings.Contains(msg, "xdr"):
		return flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_DATA
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "connection"), strings.Contains(msg, "temporar"), strings.Contains(msg, "503"), strings.Contains(msg, "catalog"), strings.Contains(msg, "postgres"), strings.Contains(msg, "b2"), strings.Contains(msg, "s3"):
		return flowctlpb.FailureClass_FAILURE_CLASS_RETRYABLE_INFRASTRUCTURE
	default:
		return flowctlpb.FailureClass_FAILURE_CLASS_UNKNOWN
	}
}
