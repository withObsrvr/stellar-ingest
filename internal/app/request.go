package app

// Typed requests for the operations this application performs.
//
// §2 of the CLI Engineering Standard: the application layer owns the use
// cases, and the adapters -- a CLI today, an API or a UI later -- only
// translate into and out of these structures. Before this existed the
// pipeline lived in a func main that read flag pointers directly, so nothing
// but a terminal could run it.
//
// One field per flag, deliberately. A narrower request would need a decision
// about which inputs matter, and that decision belongs with whoever splits
// these into per-operation requests rather than with the move that made them
// callable at all.
//
// Six fields carry a value the adapter RESOLVED rather than the raw flag,
// because where a secret came from and which flag named the output directory
// are command-line questions, not ingestion ones. They are marked below.

// RunRequest is everything a full ingestion run needs: acquire, extract to
// Parquet, push to DuckLake, populate the hot buffer.
type RunRequest struct {
	// Operation names the use case this request came from -- extract, load,
	// validate, repair or run. It is not a flag: the verb the caller used is
	// the adapter's knowledge, and it is carried here so the result can say
	// which operation produced it. Empty means "run", which is what the
	// flat flag form has always performed.
	Operation string

	// --start: Start ledger sequence (required)
	Start uint
	// --end: End ledger sequence (required)
	End uint
	// --workers: Number of parallel workers
	Workers int
	// --output-dir, RESOLVED: the resolved output directory, after --output-dir and --output are reconciled
	OutputDir string
	// --output, RESOLVED: the resolved response format: table, json or ndjson
	Output string
	// --batch-size: Ledgers per extraction batch
	BatchSize int
	// --chunk-size: Maximum ledgers per sequential extract/validate/publish chunk; 0 processes the range as one chunk
	ChunkSize uint
	// --network-passphrase: Stellar network passphrase
	NetworkPassphrase string
	// --storage-type: Backend storage type: GCS, S3, RPC, CAPTIVE_CORE, XDR, FS
	StorageType string
	// --bucket: Storage bucket/path for ledger data (required)
	Bucket string
	// --ledgers-per-file: Ledgers per archive file (GCS/S3)
	LedgersPerFile uint
	// --files-per-partition: Files per archive partition (GCS/S3)
	FilesPerPartition uint
	// --validate: Run quality validation checks after extraction
	RunValidate bool
	// --ducklake: Push bronze Parquet to DuckLake (B2 + catalog)
	RunDuckLake bool
	// --only-tables: Comma-separated extractor/source/DuckLake table names to extract; empty extracts all
	OnlyTables string
	// --ducklake-catalog, RESOLVED: resolved from DUCKLAKE_CATALOG_DSN, its _FILE form, or the flag
	DlCatalog string
	// --ducklake-data-path: S3/B2 bucket path (e.g., s3://obsrvr-lake-testnet/)
	DlDataPath string
	// --ducklake-metadata-schema: DuckLake metadata schema
	DlMetaSchema string
	// --ducklake-schema: DuckLake schema holding the physical tables
	DlSchema string
	// --ducklake-view-schema: DuckLake schema holding the presentation views
	DlViewSchema string
	// --ducklake-schema-sql: Path to v3_bronze_schema.sql (optional)
	DlSchemaSQL string
	// --program-status: Report OSC 7501 program status on stderr: auto|always|never
	ProgramStatus string
	// --ducklake-only-tables: Comma-separated source or DuckLake table names to push; defaults to --only-tables when set
	DlOnlyTables string
	// --ducklake-skip-tables: Comma-separated source or DuckLake table names to skip during push
	DlSkipTables string
	// --range-state: Record this ledger range as freezing/frozen in the catalog so an interrupted backfill can resume (requires --ducklake-catalog)
	RangeState bool
	// --range-state-table: Table holding range state
	RangeStateTable string
	// --plan: Print which ledger ranges still need building, then exit without doing any work (requires --ducklake-catalog)
	PlanOnly bool
	// --chunk-concurrency: Chunks to build at once when --chunk-size is set; 1 is sequential. Memory scales with this times --workers
	ChunkConcurrency int
	// --b2-key-id, RESOLVED: resolved from B2_KEY_ID, its _FILE form, or the flag
	B2KeyID string
	// --b2-key-secret, RESOLVED: resolved from B2_KEY_SECRET, its _FILE form, or the flag
	B2KeySecret string
	// --b2-endpoint: B2/S3 endpoint
	B2Endpoint string
	// --b2-region: B2/S3 region
	B2Region string
	// --pg-host: PostgreSQL host for hot buffer (enables hot buffer loading)
	PgHost string
	// --pg-port: PostgreSQL port
	PgPort int
	// --pg-database: PostgreSQL database
	PgDB string
	// --pg-user: PostgreSQL user
	PgUser string
	// --pg-password, RESOLVED: resolved from PG_PASSWORD, its _FILE form, or the flag
	PgPassword string
	// --pg-sslmode: PostgreSQL SSL mode
	PgSSL string
	// --tail-ledgers: Number of recent ledgers to load into hot buffer
	TailLedgers uint
	// --era-id: Era identifier for DuckLake partitioning (optional)
	EraID string
	// --enrich-contract-balances: Repair missing Bronze contract balance_holder and balance fields in place
	EnrichContractBalances bool
	// --enrichment-chunk-size: Ledgers per contract-balance enrichment transaction
	EnrichmentChunkSize uint
	// --enrichment-run-id: Durable manifest run ID (required for mutating enrichment)
	EnrichmentRunID string
	// --enrichment-dry-run: Decode and count contract-balance candidates without mutating DuckLake
	EnrichmentDryRun bool
	// --enrichment-resume: Skip chunks already completed under the enrichment run ID
	EnrichmentResume bool
	// --enrichment-manifest-schema: DuckLake schema for the enrichment audit manifest
	EnrichmentManifestSchema string
}
