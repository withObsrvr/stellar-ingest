package cli

// One flag table, two registrations.
//
// The flat form keeps using the standard library's flag package and the
// subcommands use pflag, which Cobra requires. That is not duplication for
// its own sake: pflag reads a single dash followed by several characters as a
// cluster of shorthands, so `-output-dir x` -- which the standard library
// accepts and which --help has always rendered -- would fail there. Keeping
// the flat path on the standard library means no existing invocation changes
// behaviour, and driving both from this table means neither can drift from
// the other. A test asserts they register the same names.

import (
	"flag"
	"fmt"
	"runtime"

	"github.com/spf13/pflag"
	"github.com/withObsrvr/stellar-ingest/internal/app"
)

// Values holds every flag the tool accepts.
type Values struct {
	Start                    uint
	End                      uint
	Workers                  int
	OutputDir                string
	Output                   string
	BatchSize                int
	ChunkSize                uint
	NetworkPassphrase        string
	StorageType              string
	Bucket                   string
	LedgersPerFile           uint
	FilesPerPartition        uint
	RunValidate              bool
	RunDuckLake              bool
	OnlyTables               string
	DlCatalog                string
	DlDataPath               string
	DlMetaSchema             string
	DlSchema                 string
	DlViewSchema             string
	DlSchemaSQL              string
	ShowVersion              bool
	ProgramStatus            string
	DlOnlyTables             string
	DlSkipTables             string
	RangeState               bool
	RangeStateTable          string
	PlanOnly                 bool
	ChunkConcurrency         int
	B2KeyID                  string
	B2KeySecret              string
	B2Endpoint               string
	B2Region                 string
	PgHost                   string
	PgPort                   int
	PgDB                     string
	PgUser                   string
	PgPassword               string
	PgSSL                    string
	TailLedgers              uint
	EraID                    string
	EnrichContractBalances   bool
	EnrichmentChunkSize      uint
	EnrichmentRunID          string
	EnrichmentDryRun         bool
	EnrichmentResume         bool
	EnrichmentManifestSchema string
}

// flagKind is how a flag is registered.
type flagKind int

const (
	kindString flagKind = iota
	kindBool
	kindUint
	kindInt
	kindInt64
)

// flagSpec is one flag: its name, its help text, its default, and where its
// value lands in Values.
type flagSpec struct {
	Name  string
	Usage string
	Kind  flagKind
	// Target returns a pointer into a Values for this flag, so one table can
	// drive both registrations without reflection.
	Target    func(*Values) any
	DefString string
	DefBool   bool
	DefUint   uint
	DefInt    int
	DefInt64  int64
}

// flagSpecs is the single source of truth for the flag surface.
var flagSpecs = []flagSpec{
	{
		Name: "start", Usage: "Start ledger sequence (required)",
		Kind: kindUint, DefUint: 0,
		Target: func(v *Values) any { return &v.Start },
	},
	{
		Name: "end", Usage: "End ledger sequence (required)",
		Kind: kindUint, DefUint: 0,
		Target: func(v *Values) any { return &v.End },
	},
	{
		Name: "workers", Usage: "Number of parallel workers",
		Kind: kindInt, DefInt: runtime.NumCPU(),
		Target: func(v *Values) any { return &v.Workers },
	},
	{
		Name: "output-dir", Usage: "Directory for Parquet output (required)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.OutputDir },
	},
	{
		Name: "output", Usage: "Response format: table|json|ndjson (a directory here is deprecated)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.Output },
	},
	{
		Name: "batch-size", Usage: "Ledgers per extraction batch",
		Kind: kindInt, DefInt: 1000,
		Target: func(v *Values) any { return &v.BatchSize },
	},
	{
		Name: "chunk-size", Usage: "Maximum ledgers per sequential extract/validate/publish chunk; 0 processes the range as one chunk",
		Kind: kindUint, DefUint: 0,
		Target: func(v *Values) any { return &v.ChunkSize },
	},
	{
		Name: "network-passphrase", Usage: "Stellar network passphrase",
		Kind: kindString, DefString: "Test SDF Network ; September 2015",
		Target: func(v *Values) any { return &v.NetworkPassphrase },
	},
	{
		Name: "storage-type", Usage: "Backend storage type: GCS, S3, RPC, CAPTIVE_CORE, XDR, FS",
		Kind: kindString, DefString: "FS",
		Target: func(v *Values) any { return &v.StorageType },
	},
	{
		Name: "bucket", Usage: "Storage bucket/path for ledger data (required)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.Bucket },
	},
	{
		Name: "ledgers-per-file", Usage: "Ledgers per archive file (GCS/S3)",
		Kind: kindUint, DefUint: 1,
		Target: func(v *Values) any { return &v.LedgersPerFile },
	},
	{
		Name: "files-per-partition", Usage: "Files per archive partition (GCS/S3)",
		Kind: kindUint, DefUint: 64000,
		Target: func(v *Values) any { return &v.FilesPerPartition },
	},
	{
		Name: "validate", Usage: "Run quality validation checks after extraction",
		Kind: kindBool, DefBool: false,
		Target: func(v *Values) any { return &v.RunValidate },
	},
	{
		Name: "ducklake", Usage: "Push bronze Parquet to DuckLake (B2 + catalog)",
		Kind: kindBool, DefBool: false,
		Target: func(v *Values) any { return &v.RunDuckLake },
	},
	{
		Name: "only-tables", Usage: "Comma-separated extractor/source/DuckLake table names to extract; empty extracts all",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.OnlyTables },
	},
	{
		Name: "ducklake-catalog", Usage: "PostgreSQL catalog DSN for DuckLake (deprecated: set DUCKLAKE_CATALOG_DSN or DUCKLAKE_CATALOG_DSN_FILE)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.DlCatalog },
	},
	{
		Name: "ducklake-data-path", Usage: "S3/B2 bucket path (e.g., s3://obsrvr-lake-testnet/)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.DlDataPath },
	},
	{
		Name: "ducklake-metadata-schema", Usage: "DuckLake metadata schema",
		Kind: kindString, DefString: "bronze_meta",
		Target: func(v *Values) any { return &v.DlMetaSchema },
	},
	{
		Name: "ducklake-schema", Usage: "DuckLake schema holding the physical tables",
		Kind: kindString, DefString: "bronze_store",
		Target: func(v *Values) any { return &v.DlSchema },
	},
	{
		Name: "ducklake-view-schema", Usage: "DuckLake schema holding the presentation views",
		Kind: kindString, DefString: "bronze",
		Target: func(v *Values) any { return &v.DlViewSchema },
	},
	{
		Name: "ducklake-schema-sql", Usage: "Path to v3_bronze_schema.sql (optional)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.DlSchemaSQL },
	},
	{
		Name: "version", Usage: "Print the build version and exit",
		Kind: kindBool, DefBool: false,
		Target: func(v *Values) any { return &v.ShowVersion },
	},
	{
		Name: "program-status", Usage: "Report OSC 7501 program status on stderr: auto|always|never",
		Kind: kindString, DefString: "auto",
		Target: func(v *Values) any { return &v.ProgramStatus },
	},
	{
		Name: "ducklake-only-tables", Usage: "Comma-separated source or DuckLake table names to push; defaults to --only-tables when set",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.DlOnlyTables },
	},
	{
		Name: "ducklake-skip-tables", Usage: "Comma-separated source or DuckLake table names to skip during push",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.DlSkipTables },
	},
	{
		Name: "range-state", Usage: "Record this ledger range as freezing/frozen in the catalog so an interrupted backfill can resume (requires --ducklake-catalog)",
		Kind: kindBool, DefBool: false,
		Target: func(v *Values) any { return &v.RangeState },
	},
	{
		Name: "range-state-table", Usage: "Table holding range state",
		Kind: kindString, DefString: "public.ingest_range_state",
		Target: func(v *Values) any { return &v.RangeStateTable },
	},
	{
		Name: "plan", Usage: "Print which ledger ranges still need building, then exit without doing any work (requires --ducklake-catalog)",
		Kind: kindBool, DefBool: false,
		Target: func(v *Values) any { return &v.PlanOnly },
	},
	{
		Name: "chunk-concurrency", Usage: "Chunks to build at once when --chunk-size is set; 1 is sequential. Memory scales with this times --workers",
		Kind: kindInt, DefInt: 1,
		Target: func(v *Values) any { return &v.ChunkConcurrency },
	},
	{
		Name: "b2-key-id", Usage: "B2/S3 access key ID (deprecated: set B2_KEY_ID or B2_KEY_ID_FILE)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.B2KeyID },
	},
	{
		Name: "b2-key-secret", Usage: "B2/S3 secret access key (deprecated: set B2_KEY_SECRET or B2_KEY_SECRET_FILE)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.B2KeySecret },
	},
	{
		Name: "b2-endpoint", Usage: "B2/S3 endpoint",
		Kind: kindString, DefString: "s3.us-west-004.backblazeb2.com",
		Target: func(v *Values) any { return &v.B2Endpoint },
	},
	{
		Name: "b2-region", Usage: "B2/S3 region",
		Kind: kindString, DefString: "us-west-004",
		Target: func(v *Values) any { return &v.B2Region },
	},
	{
		Name: "pg-host", Usage: "PostgreSQL host for hot buffer (enables hot buffer loading)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.PgHost },
	},
	{
		Name: "pg-port", Usage: "PostgreSQL port",
		Kind: kindInt, DefInt: 25060,
		Target: func(v *Values) any { return &v.PgPort },
	},
	{
		Name: "pg-database", Usage: "PostgreSQL database",
		Kind: kindString, DefString: "stellar_hot",
		Target: func(v *Values) any { return &v.PgDB },
	},
	{
		Name: "pg-user", Usage: "PostgreSQL user",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.PgUser },
	},
	{
		Name: "pg-password", Usage: "PostgreSQL password (deprecated: set PG_PASSWORD or PG_PASSWORD_FILE)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.PgPassword },
	},
	{
		Name: "pg-sslmode", Usage: "PostgreSQL SSL mode",
		Kind: kindString, DefString: "require",
		Target: func(v *Values) any { return &v.PgSSL },
	},
	{
		Name: "tail-ledgers", Usage: "Number of recent ledgers to load into hot buffer",
		Kind: kindUint, DefUint: 100000,
		Target: func(v *Values) any { return &v.TailLedgers },
	},
	{
		Name: "era-id", Usage: "Era identifier for DuckLake partitioning (optional)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.EraID },
	},
	{
		Name: "enrich-contract-balances", Usage: "Repair missing Bronze contract balance_holder and balance fields in place",
		Kind: kindBool, DefBool: false,
		Target: func(v *Values) any { return &v.EnrichContractBalances },
	},
	{
		Name: "enrichment-chunk-size", Usage: "Ledgers per contract-balance enrichment transaction",
		Kind: kindUint, DefUint: 100000,
		Target: func(v *Values) any { return &v.EnrichmentChunkSize },
	},
	{
		Name: "enrichment-run-id", Usage: "Durable manifest run ID (required for mutating enrichment)",
		Kind: kindString, DefString: "",
		Target: func(v *Values) any { return &v.EnrichmentRunID },
	},
	{
		Name: "enrichment-dry-run", Usage: "Decode and count contract-balance candidates without mutating DuckLake",
		Kind: kindBool, DefBool: false,
		Target: func(v *Values) any { return &v.EnrichmentDryRun },
	},
	{
		Name: "enrichment-resume", Usage: "Skip chunks already completed under the enrichment run ID",
		Kind: kindBool, DefBool: true,
		Target: func(v *Values) any { return &v.EnrichmentResume },
	},
	{
		Name: "enrichment-manifest-schema", Usage: "DuckLake schema for the enrichment audit manifest",
		Kind: kindString, DefString: "bronze_operations",
		Target: func(v *Values) any { return &v.EnrichmentManifestSchema },
	},
}

// RegisterGoFlags registers every flag on a standard-library FlagSet. This is
// the flat form, kept byte-identical so no deployed invocation changes.
func (v *Values) RegisterGoFlags(fs *flag.FlagSet) {
	for _, spec := range flagSpecs {
		switch target := spec.Target(v).(type) {
		case *string:
			fs.StringVar(target, spec.Name, spec.DefString, spec.Usage)
		case *bool:
			fs.BoolVar(target, spec.Name, spec.DefBool, spec.Usage)
		case *uint:
			fs.UintVar(target, spec.Name, spec.DefUint, spec.Usage)
		case *int:
			fs.IntVar(target, spec.Name, spec.DefInt, spec.Usage)
		case *int64:
			fs.Int64Var(target, spec.Name, spec.DefInt64, spec.Usage)
		default:
			panic(fmt.Sprintf("flag %s has an unhandled target type %T", spec.Name, target))
		}
	}
}

// RegisterPFlags registers every flag on a pflag FlagSet, which is what Cobra
// subcommands use.
func (v *Values) RegisterPFlags(fs *pflag.FlagSet) {
	for _, spec := range flagSpecs {
		switch target := spec.Target(v).(type) {
		case *string:
			fs.StringVar(target, spec.Name, spec.DefString, spec.Usage)
		case *bool:
			fs.BoolVar(target, spec.Name, spec.DefBool, spec.Usage)
		case *uint:
			fs.UintVar(target, spec.Name, spec.DefUint, spec.Usage)
		case *int:
			fs.IntVar(target, spec.Name, spec.DefInt, spec.Usage)
		case *int64:
			fs.Int64Var(target, spec.Name, spec.DefInt64, spec.Usage)
		default:
			panic(fmt.Sprintf("flag %s has an unhandled target type %T", spec.Name, target))
		}
	}
}

// FlagNames lists every registered flag name, for tests and diagnostics.
func FlagNames() []string {
	names := make([]string, 0, len(flagSpecs))
	for _, spec := range flagSpecs {
		names = append(names, spec.Name)
	}
	return names
}

// Resolve turns raw flag values into the request an operation takes, doing
// the work that belongs to the command line: reconciling the two output
// flags, and finding each credential.
//
// Returns the warnings to print as well, rather than printing them, so the
// caller owns the stream and a test can read them.
func (v *Values) Resolve() (app.RunRequest, []string, error) {
	var notices []string

	outputs, err := ResolveOutput(v.OutputDir, v.Output)
	if err != nil {
		return app.RunRequest{}, nil, err
	}
	if notice := outputs.DeprecationNotice(); notice != "" {
		notices = append(notices, notice)
	}

	creds, err := ResolveCredentials(v.DlCatalog, v.B2KeyID, v.B2KeySecret, v.PgPassword)
	if err != nil {
		return app.RunRequest{}, nil, err
	}
	notices = append(notices, creds.Notices()...)

	req := v.request()
	req.OutputDir = outputs.Dir
	req.Output = string(outputs.Format)
	req.DlCatalog = creds.CatalogDSN.Value
	req.B2KeyID = creds.B2KeyID.Value
	req.B2KeySecret = creds.B2KeySecret.Value
	req.PgPassword = creds.PGPassword.Value
	return req, notices, nil
}

// request copies the flag values into the request, field by field. The six
// values the adapter resolves are overwritten by Resolve above.
func (v *Values) request() app.RunRequest {
	return app.RunRequest{
		Start:                    v.Start,
		End:                      v.End,
		Workers:                  v.Workers,
		OutputDir:                v.OutputDir,
		Output:                   v.Output,
		BatchSize:                v.BatchSize,
		ChunkSize:                v.ChunkSize,
		NetworkPassphrase:        v.NetworkPassphrase,
		StorageType:              v.StorageType,
		Bucket:                   v.Bucket,
		LedgersPerFile:           v.LedgersPerFile,
		FilesPerPartition:        v.FilesPerPartition,
		RunValidate:              v.RunValidate,
		RunDuckLake:              v.RunDuckLake,
		OnlyTables:               v.OnlyTables,
		DlCatalog:                v.DlCatalog,
		DlDataPath:               v.DlDataPath,
		DlMetaSchema:             v.DlMetaSchema,
		DlSchema:                 v.DlSchema,
		DlViewSchema:             v.DlViewSchema,
		DlSchemaSQL:              v.DlSchemaSQL,
		DlOnlyTables:             v.DlOnlyTables,
		DlSkipTables:             v.DlSkipTables,
		RangeState:               v.RangeState,
		RangeStateTable:          v.RangeStateTable,
		PlanOnly:                 v.PlanOnly,
		ChunkConcurrency:         v.ChunkConcurrency,
		B2KeyID:                  v.B2KeyID,
		B2KeySecret:              v.B2KeySecret,
		B2Endpoint:               v.B2Endpoint,
		B2Region:                 v.B2Region,
		PgHost:                   v.PgHost,
		PgPort:                   v.PgPort,
		PgDB:                     v.PgDB,
		PgUser:                   v.PgUser,
		PgPassword:               v.PgPassword,
		PgSSL:                    v.PgSSL,
		TailLedgers:              v.TailLedgers,
		EraID:                    v.EraID,
		EnrichContractBalances:   v.EnrichContractBalances,
		EnrichmentChunkSize:      v.EnrichmentChunkSize,
		EnrichmentRunID:          v.EnrichmentRunID,
		EnrichmentDryRun:         v.EnrichmentDryRun,
		EnrichmentResume:         v.EnrichmentResume,
		EnrichmentManifestSchema: v.EnrichmentManifestSchema,
	}
}
