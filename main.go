package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/withObsrvr/stellar-ingest/internal/credential"
	"github.com/withObsrvr/stellar-ingest/internal/status"
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

// validateLedgerRange accepts any range where end is not before start,
// including a single ledger.
//
// Closes roadmap known gap 2: --start/--end are inclusive, so start == end is
// a legitimate one-ledger range, but validation rejected it and a single
// ledger could not be expressed.
//
// Chunking forced the decision the roadmap said this needed. A
// boundary-aligned chunker emits a single-ledger trailing chunk whenever the
// range ends one past a boundary -- 64350000..64354000 at chunk size 1000
// ends with 64354000-64354000 -- and merging that into the previous chunk
// would break the alignment splitLedgerChunks exists to preserve. So the
// range semantics give way, not the chunk geometry.
//
// This is a function rather than three lines in main so it can be tested:
// the rule cost a real dispatch, where four chunks succeeded and the trailing
// single-ledger chunk failed.
func validateLedgerRange(start, end uint32) error {
	if end < start {
		return fmt.Errorf("--end (%d) must not be less than --start (%d)", end, start)
	}
	return nil
}

func main() {
	start := flag.Uint("start", 0, "Start ledger sequence (required)")
	end := flag.Uint("end", 0, "End ledger sequence (required)")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of parallel workers")
	outputDir := flag.String("output-dir", "", "Directory for Parquet output (required)")
	// --output is the response format, per the CLI standard. A path is still
	// accepted here for one release and resolved as the old directory
	// meaning; see resolveOutput.
	output := flag.String("output", "", "Response format: table|json|ndjson (a directory here is deprecated)")
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
	dlCatalog := flag.String("ducklake-catalog", "", "PostgreSQL catalog DSN for DuckLake (deprecated: set DUCKLAKE_CATALOG_DSN or DUCKLAKE_CATALOG_DSN_FILE)")
	dlDataPath := flag.String("ducklake-data-path", "", "S3/B2 bucket path (e.g., s3://obsrvr-lake-testnet/)")
	dlMetaSchema := flag.String("ducklake-metadata-schema", "bronze_meta", "DuckLake metadata schema")
	// The storage layer and the presentation layer live in different schemas:
	// physical tables in the store schema, the compatibility views in the
	// schema consumers already query. Both were reachable only as defaults
	// until now, which left no way to isolate a validation run by schema name
	// or to point the loader at a differently-named layout.
	dlSchema := flag.String("ducklake-schema", "bronze_store", "DuckLake schema holding the physical tables")
	dlViewSchema := flag.String("ducklake-view-schema", "bronze", "DuckLake schema holding the presentation views")
	dlSchemaSQL := flag.String("ducklake-schema-sql", "", "Path to v3_bronze_schema.sql (optional)")
	// OSC 7501 program status, so a terminal, dashboard or supervising tool
	// can follow the run from the program's own account of its state instead
	// of scraping its output. auto reports only when stderr is a terminal,
	// because the escape sequence is noise in a log file or a pipe.
	programStatus := flag.String("program-status", "auto",
		"Report OSC 7501 program status on stderr: auto|always|never")
	dlOnlyTables := flag.String("ducklake-only-tables", "", "Comma-separated source or DuckLake table names to push; defaults to --only-tables when set")
	dlSkipTables := flag.String("ducklake-skip-tables", "", "Comma-separated source or DuckLake table names to skip during push")
	rangeState := flag.Bool("range-state", false, "Record this ledger range as freezing/frozen in the catalog so an interrupted backfill can resume (requires --ducklake-catalog)")
	rangeStateTable := flag.String("range-state-table", "public.ingest_range_state", "Table holding range state")
	planOnly := flag.Bool("plan", false, "Print which ledger ranges still need building, then exit without doing any work (requires --ducklake-catalog)")
	chunkConcurrency := flag.Int("chunk-concurrency", 1, "Chunks to build at once when --chunk-size is set; 1 is sequential. Memory scales with this times --workers")
	b2KeyID := flag.String("b2-key-id", "", "B2/S3 access key ID (deprecated: set B2_KEY_ID or B2_KEY_ID_FILE)")
	b2KeySecret := flag.String("b2-key-secret", "", "B2/S3 secret access key (deprecated: set B2_KEY_SECRET or B2_KEY_SECRET_FILE)")
	b2Endpoint := flag.String("b2-endpoint", "s3.us-west-004.backblazeb2.com", "B2/S3 endpoint")
	b2Region := flag.String("b2-region", "us-west-004", "B2/S3 region")
	pgHost := flag.String("pg-host", "", "PostgreSQL host for hot buffer (enables hot buffer loading)")
	pgPort := flag.Int("pg-port", 25060, "PostgreSQL port")
	pgDB := flag.String("pg-database", "stellar_hot", "PostgreSQL database")
	pgUser := flag.String("pg-user", "", "PostgreSQL user")
	pgPassword := flag.String("pg-password", "", "PostgreSQL password (deprecated: set PG_PASSWORD or PG_PASSWORD_FILE)")
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

	statusEmitter, err := newStatusEmitter(*programStatus)
	if err != nil {
		log.Fatal(err)
	}

	creds, err := resolveCredentials(*dlCatalog, *b2KeyID, *b2KeySecret, *pgPassword)
	if err != nil {
		log.Fatal(err)
	}
	for _, notice := range creds.Notices() {
		// stderr, and never the value: the point of the warning is to get the
		// secret out of places like this one.
		log.Print(notice)
	}

	outputs, err := resolveOutput(*outputDir, *output)
	if err != nil {
		log.Fatal(err)
	}
	if notice := outputs.deprecationNotice(); notice != "" {
		// stderr: a warning is a diagnostic, and stdout carries results.
		log.Print(notice)
	}
	if *enrichContractBalances {
		if *start == 0 || *end == 0 || *end < *start {
			log.Fatalf("--enrich-contract-balances requires a valid inclusive --start/--end range")
		}
		if *enrichmentChunkSize == 0 {
			log.Fatal("--enrichment-chunk-size must be greater than zero")
		}
		if creds.CatalogDSN.Value == "" || *dlDataPath == "" || creds.B2KeyID.Value == "" || creds.B2KeySecret.Value == "" {
			log.Fatal("--enrich-contract-balances requires --ducklake-catalog, --ducklake-data-path, --b2-key-id, and --b2-key-secret")
		}
		if !*enrichmentDryRun && strings.TrimSpace(*enrichmentRunID) == "" {
			log.Fatal("--enrichment-run-id is required unless --enrichment-dry-run is set")
		}

		ctx := context.Background()
		enricher, err := NewContractBalanceEnricher(ContractBalanceEnricherConfig{
			DuckLake: DuckLakeConfig{
				CatalogDSN:     creds.CatalogDSN.Value,
				DataPath:       *dlDataPath,
				MetadataSchema: *dlMetaSchema,
				S3KeyID:        creds.B2KeyID.Value,
				S3KeySecret:    creds.B2KeySecret.Value,
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
	if outputs.Dir == "" {
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

	if err := validateLedgerRange(startLedger, endLedger); err != nil {
		log.Fatal(err)
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
		if creds.CatalogDSN.Value == "" {
			log.Fatal("--plan requires --ducklake-catalog")
		}
		size := uint32(*chunkSize)
		if size == 0 {
			// No chunking means the whole span is one range, matching what a
			// plain --start/--end run would claim.
			size = endLedger - startLedger + 1
		}
		store, err := NewRangeStateStore(context.Background(), creds.CatalogDSN.Value, *rangeStateTable, *eraID)
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
		if err := runChunkedCommand(context.Background(), os.Args[1:], startLedger, endLedger, uint32(*chunkSize), outputs.Dir, *chunkConcurrency); err != nil {
			log.Fatalf("chunked backfill failed: %v", err)
		}
		return
	}

	// Ensure output directory exists
	if err := os.MkdirAll(outputs.Dir, 0o755); err != nil {
		log.Fatalf("Failed to create output directory %q: %v", outputs.Dir, err)
	}
	effectiveOutput := outputs.Dir
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
	fmt.Printf("Output directory:  %s\n", outputs.Dir)
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
		if creds.CatalogDSN.Value == "" {
			log.Fatal("--range-state requires --ducklake-catalog")
		}
		// frozen means "pushed to DuckLake". Claiming a range this run can
		// never push would leave it freezing until the lease expired.
		if !*runDuckLake {
			log.Fatal("--range-state requires --ducklake: a range is frozen when the push lands, not when extraction finishes")
		}
		var err error
		rangeStore, err = NewRangeStateStore(context.Background(), creds.CatalogDSN.Value, *rangeStateTable, *eraID)
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
			// The marker is for the chunk runner; the sentence is for a
			// human reading a single-range dispatch.
			fmt.Printf("%s Range %s is already frozen or claimed by another run; nothing to do.\n",
				rangeSkippedMarker, thisRange)
			return
		}
		fmt.Printf("Range %s claimed (freezing)\n", thisRange)
	}

	// Skip extraction if only doing DuckLake push on existing output
	if extractRequested {
		config := OrchestratorConfig{
			StatusEmitter:     statusEmitter,
			StartLedger:       startLedger,
			EndLedger:         endLedger,
			NumWorkers:        *workers,
			OutputDir:         outputs.Dir,
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
			"output_dir": outputs.Dir,
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
		if creds.CatalogDSN.Value == "" || *dlDataPath == "" || creds.B2KeyID.Value == "" || creds.B2KeySecret.Value == "" {
			log.Fatal("--ducklake requires --ducklake-catalog, --ducklake-data-path, --b2-key-id, --b2-key-secret")
		}

		fmt.Println()
		fmt.Println("=== DuckLake Push ===")
		dlStart := time.Now()

		if err := reporter.ReportChunkProgress(ctx, int64(startLedger), int64(endLedger), "ducklake_setup", nil, map[string]string{
			"ducklake_metadata_schema": *dlMetaSchema,
			"ducklake_schema":          *dlSchema,
			"ducklake_view_schema":     *dlViewSchema,
			"ducklake_data_path":       *dlDataPath,
		}); err != nil {
			log.Printf("Flowctl progress report failed: %v", err)
		}

		pusher, err := NewDuckLakePusher(DuckLakeConfig{
			CatalogDSN:      creds.CatalogDSN.Value,
			DataPath:        *dlDataPath,
			MetadataSchema:  *dlMetaSchema,
			SchemaName:      *dlSchema,
			ViewSchemaName:  *dlViewSchema,
			S3KeyID:         creds.B2KeyID.Value,
			S3KeySecret:     creds.B2KeySecret.Value,
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
			Password:    creds.PGPassword.Value,
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

	// The terminal report, and the one a watcher is actually waiting for.
	// done survives process exit by design, so a dashboard still shows the
	// outcome after the run is gone. Emitted here rather than earlier because
	// everything above -- extract, push, hot buffer, catalog report -- is part
	// of the run finishing.
	_ = statusEmitter.Done("", fmt.Sprintf("Ingested ledgers %d-%d", startLedger, endLedger))

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

// newStatusEmitter builds the OSC 7501 emitter from the --program-status mode.
//
// auto is the default and reports only when stderr is a terminal: the escape
// sequence is meaningful to a terminal or a supervising tool and is noise in a
// log file or a pipe. always is for a consumer that reads a captured stream;
// never is for anyone who wants the stream clean.
//
// Reports go to stderr rather than stdout because stdout carries results, and
// an escape sequence written into a result stream corrupts it.
func newStatusEmitter(mode string) (*status.Emitter, error) {
	var enabled bool
	switch mode {
	case "auto":
		enabled = status.StderrIsTerminal()
	case "always":
		enabled = true
	case "never":
		enabled = false
	default:
		return nil, fmt.Errorf("--program-status must be auto, always or never, got %q", mode)
	}
	return status.NewEmitter(os.Stderr, "stellar-ingest", enabled), nil
}

// credentialSet is every credential this run needs, resolved.
type credentialSet struct {
	CatalogDSN  credential.Resolution
	B2KeyID     credential.Resolution
	B2KeySecret credential.Resolution
	PGPassword  credential.Resolution
}

// credentialSpecs names the four credentials and the variables that supply
// them. Unprefixed SCREAMING_SNAKE matches the house convention already used
// by RPC_AUTH_HEADER, S3_ENDPOINT_URL and STELLAR_CORE_BINARY_PATH.
//
// DUCKLAKE_CATALOG_DSN rather than DUCKLAKE_CATALOG because that is the name
// the README's own examples already pass to --ducklake-catalog.
var credentialSpecs = struct {
	CatalogDSN  credential.Spec
	B2KeyID     credential.Spec
	B2KeySecret credential.Spec
	PGPassword  credential.Spec
}{
	CatalogDSN:  credential.Spec{Flag: "ducklake-catalog", EnvVar: "DUCKLAKE_CATALOG_DSN"},
	B2KeyID:     credential.Spec{Flag: "b2-key-id", EnvVar: "B2_KEY_ID"},
	B2KeySecret: credential.Spec{Flag: "b2-key-secret", EnvVar: "B2_KEY_SECRET"},
	PGPassword:  credential.Spec{Flag: "pg-password", EnvVar: "PG_PASSWORD"},
}

// resolveCredentials resolves all four, from flag, file or environment.
func resolveCredentials(catalogDSN, b2KeyID, b2KeySecret, pgPassword string) (credentialSet, error) {
	env := credential.Environment{Getenv: os.Getenv, ReadFile: os.ReadFile}

	var set credentialSet
	for _, item := range []struct {
		spec  credential.Spec
		value string
		into  *credential.Resolution
	}{
		{credentialSpecs.CatalogDSN, catalogDSN, &set.CatalogDSN},
		{credentialSpecs.B2KeyID, b2KeyID, &set.B2KeyID},
		{credentialSpecs.B2KeySecret, b2KeySecret, &set.B2KeySecret},
		{credentialSpecs.PGPassword, pgPassword, &set.PGPassword},
	} {
		resolved, err := credential.Resolve(item.spec, item.value, env)
		if err != nil {
			return credentialSet{}, err
		}
		*item.into = resolved
	}
	return set, nil
}

// Notices lists the warnings for credentials that arrived on the command
// line, in a stable order so output is comparable.
func (c credentialSet) Notices() []string {
	var out []string
	for _, r := range []credential.Resolution{c.CatalogDSN, c.B2KeyID, c.B2KeySecret, c.PGPassword} {
		if notice := r.DeprecationNotice(); notice != "" {
			out = append(out, notice)
		}
	}
	return out
}
