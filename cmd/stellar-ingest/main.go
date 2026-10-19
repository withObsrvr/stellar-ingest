// Command stellar-ingest is the composition root.
//
// It declares the flags, resolves what belongs to the command line -- which
// flag named the output directory, where each credential came from, whether
// program status should be reported -- and hands a typed request to the
// application layer. It owns no ingestion logic, which is what §2 of the CLI
// Engineering Standard asks of an adapter.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/withObsrvr/stellar-ingest/internal/app"
	"github.com/withObsrvr/stellar-ingest/internal/cli"
	"github.com/withObsrvr/stellar-ingest/internal/ingest"
	"log"
	"runtime"
)

// Version is set at build time via -ldflags "-X main.Version=..."

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
	// --version, which the CLI standard's discoverability row asks for. It
	// also makes the build stamp observable: Version is set by a linker flag
	// naming internal/ingest, from three separate build paths, and a build
	// that misses it stamps "dev" into every manifest and parity record it
	// writes. `make version-stamp` checks it rather than trusting that the
	// Makefile, the Dockerfile and flake.nix agree.
	showVersion := flag.Bool("version", false, "Print the build version and exit")
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

	if *showVersion {
		// stdout: this is the result, not a diagnostic.
		fmt.Println(ingest.Version)
		return
	}

	statusEmitter, err := cli.NewStatusEmitter(*programStatus)
	if err != nil {
		log.Fatal(err)
	}

	creds, err := cli.ResolveCredentials(*dlCatalog, *b2KeyID, *b2KeySecret, *pgPassword)
	if err != nil {
		log.Fatal(err)
	}
	for _, notice := range creds.Notices() {
		// stderr, and never the value: the point of the warning is to get the
		// secret out of places like this one.
		log.Print(notice)
	}

	outputs, err := cli.ResolveOutput(*outputDir, *output)
	if err != nil {
		log.Fatal(err)
	}
	if notice := outputs.DeprecationNotice(); notice != "" {
		// stderr: a warning is a diagnostic, and stdout carries results.
		log.Print(notice)
	}

	// Everything the command line had to decide is decided. From here the
	// request is data, and the operation behind it has no idea a terminal
	// exists.
	app.Run(context.Background(), app.RunRequest{
		Start:                    *start,
		End:                      *end,
		Workers:                  *workers,
		OutputDir:                outputs.Dir,
		Output:                   string(outputs.Format),
		BatchSize:                *batchSize,
		ChunkSize:                *chunkSize,
		NetworkPassphrase:        *networkPassphrase,
		StorageType:              *storageType,
		Bucket:                   *bucket,
		LedgersPerFile:           *ledgersPerFile,
		FilesPerPartition:        *filesPerPartition,
		RunValidate:              *runValidate,
		RunDuckLake:              *runDuckLake,
		OnlyTables:               *onlyTables,
		DlCatalog:                creds.CatalogDSN.Value,
		DlDataPath:               *dlDataPath,
		DlMetaSchema:             *dlMetaSchema,
		DlSchema:                 *dlSchema,
		DlViewSchema:             *dlViewSchema,
		DlSchemaSQL:              *dlSchemaSQL,
		ProgramStatus:            *programStatus,
		DlOnlyTables:             *dlOnlyTables,
		DlSkipTables:             *dlSkipTables,
		RangeState:               *rangeState,
		RangeStateTable:          *rangeStateTable,
		PlanOnly:                 *planOnly,
		ChunkConcurrency:         *chunkConcurrency,
		B2KeyID:                  creds.B2KeyID.Value,
		B2KeySecret:              creds.B2KeySecret.Value,
		B2Endpoint:               *b2Endpoint,
		B2Region:                 *b2Region,
		PgHost:                   *pgHost,
		PgPort:                   *pgPort,
		PgDB:                     *pgDB,
		PgUser:                   *pgUser,
		PgPassword:               creds.PGPassword.Value,
		PgSSL:                    *pgSSL,
		TailLedgers:              *tailLedgers,
		EraID:                    *eraID,
		EnrichContractBalances:   *enrichContractBalances,
		EnrichmentChunkSize:      *enrichmentChunkSize,
		EnrichmentRunID:          *enrichmentRunID,
		EnrichmentDryRun:         *enrichmentDryRun,
		EnrichmentResume:         *enrichmentResume,
		EnrichmentManifestSchema: *enrichmentManifestSchema,
	}, statusEmitter)
}
