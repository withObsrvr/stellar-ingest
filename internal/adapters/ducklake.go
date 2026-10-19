package adapters

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// embeddedBronzeSchema is the canonical bronze schema, kept in sync with
// postgres-ducklake-flusher/v3_bronze_schema.sql. The file is copied into
// this package and embedded so the loader creates DuckLake tables with the
// exact same column set the streaming flusher expects. Without this match,
// whichever writer ran first would define a different schema and the other
// would get "table has N columns but M values supplied" errors on insert.
//
//go:embed v3_bronze_schema.sql
var embeddedBronzeSchema string

// embeddedBronzeViews is the presentation layer. Bronze stores the most
// compact faithful representation; these views render the shapes consumers
// already query, so a storage change does not become a six-service
// migration. They are created in the view schema and read the store schema.
//
//go:embed v3_bronze_views.sql
var embeddedBronzeViews string

// DuckLakeConfig holds configuration for pushing Parquet to DuckLake.
type DuckLakeConfig struct {
	CatalogDSN      string // PostgreSQL catalog connection string
	DataPath        string // S3/B2 bucket path (e.g., "s3://obsrvr-lake-testnet/")
	MetadataSchema  string // Catalog schema (e.g., "bronze_meta")
	CatalogName     string // DuckLake catalog name (e.g., "lake")
	SchemaName      string // Schema holding the physical tables (e.g., "bronze_store")
	ViewSchemaName  string // Schema holding the presentation views (e.g., "bronze")
	S3KeyID         string
	S3KeySecret     string
	S3Endpoint      string
	S3Region        string
	BronzeSchemaSQL string // Path to v3_bronze_schema.sql (optional, embedded fallback)
	StartLedger     uint32 // Requested push start; used for idempotent full-chunk deletes
	EndLedger       uint32 // Requested push end; used for idempotent full-chunk deletes
	OnlyTables      map[string]bool
	SkipTables      map[string]bool
	ManifestPath    string // optional; when set, only files published by this manifest are ingested
}

// DuckLakePusher pushes local Parquet files into DuckLake.
type DuckLakePusher struct {
	db     *sql.DB
	config DuckLakeConfig
}

// DuckLakePushResult describes rows written during a DuckLake push.
// Commit-conflict retry policy. Five attempts over roughly 15 seconds of
// backoff: a conflict clears as soon as the competing writer commits, and a
// push that cannot land in that window is better reported than retried
// forever -- the range catalog leaves the chunk unclaimed and a later run
// re-derives it.
const (
	duckLakeCommitAttempts = 5
	duckLakeCommitBackoff  = 500 * time.Millisecond
)

type DuckLakePushResult struct {
	TotalRows int64
	RowCounts map[string]int64
}

func NewDuckLakePusher(config DuckLakeConfig) (*DuckLakePusher, error) {
	if config.CatalogName == "" {
		config.CatalogName = "lake"
	}
	if config.SchemaName == "" {
		config.SchemaName = "bronze_store"
	}
	// The views take the name consumers already query, which is why the
	// physical tables had to move rather than the readers.
	if config.ViewSchemaName == "" {
		config.ViewSchemaName = "bronze"
	}
	if err := validateSchemaNames(config.SchemaName, config.ViewSchemaName); err != nil {
		return nil, err
	}
	if config.MetadataSchema == "" {
		config.MetadataSchema = "bronze_meta"
	}

	db, err := OpenDuckDB()
	if err != nil {
		return nil, err
	}
	// DuckDB connection state (attached catalogs, secrets, and temporary staging
	// tables) is connection-local. Keep this process on one connection so a
	// chunk cannot attach on one connection and execute on another.
	db.SetMaxOpenConns(1)

	return &DuckLakePusher{db: db, config: config}, nil
}

func (p *DuckLakePusher) Close() error {
	return p.db.Close()
}

// Setup attaches the configured DuckLake catalog on the pusher's connection.
// It is shared by the normal Parquet pusher and in-place maintenance jobs.
func (p *DuckLakePusher) Setup(ctx context.Context) error {
	// Step 1: Load extensions
	log.Println("[DuckLake] Loading extensions...")
	if _, err := p.db.ExecContext(ctx, "INSTALL ducklake; LOAD ducklake;"); err != nil {
		return fmt.Errorf("load extension ducklake: %w", err)
	}
	if _, err := p.db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return fmt.Errorf("load extension httpfs: %w", err)
	}

	// Step 2: Configure S3 credentials
	log.Println("[DuckLake] Configuring S3 credentials...")
	endpoint := strings.TrimPrefix(p.config.S3Endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	p.db.ExecContext(ctx, "DROP SECRET IF EXISTS __default_s3")
	createSecretSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path',
			URL_COMPATIBILITY_MODE true
		)
	`, p.config.S3KeyID, p.config.S3KeySecret, p.config.S3Region, endpoint)

	if _, err := p.db.ExecContext(ctx, createSecretSQL); err != nil {
		return fmt.Errorf("create S3 secret: %w", err)
	}

	// Step 3: Attach DuckLake catalog
	log.Printf("[DuckLake] Attaching catalog: %s...", p.config.CatalogName)
	catalogPath := duckLakeCatalogTarget(p.config.CatalogDSN)

	// Try attaching existing catalog first, then create if needed
	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);",
		catalogPath, p.config.CatalogName, p.config.DataPath, p.config.MetadataSchema)

	if _, err := p.db.ExecContext(ctx, attachSQL); err != nil {
		createAttachSQL := fmt.Sprintf(
			"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);",
			catalogPath, p.config.CatalogName, p.config.DataPath, p.config.MetadataSchema)
		if _, err := p.db.ExecContext(ctx, createAttachSQL); err != nil {
			return fmt.Errorf("attach catalog: %w", err)
		}
		log.Println("[DuckLake] Created new catalog")
	}

	// Step 4: Create both schemas -- the one holding physical tables and the
	// one holding the presentation views consumers read.
	for _, schema := range []string{p.config.SchemaName, p.config.ViewSchemaName} {
		createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", p.config.CatalogName, schema)
		if _, err := p.db.ExecContext(ctx, createSchemaSQL); err != nil {
			return fmt.Errorf("create schema %s: %w", schema, err)
		}
	}
	return nil
}

// Push reads local Parquet files and inserts them into DuckLake.
// pushTableWithRetry pushes one table, retrying a DuckLake commit conflict.
//
// Concurrent chunks are separate processes writing the same DuckLake tables
// for different ledger ranges. Each does delete-then-insert for its own
// range, which is what makes a push idempotent -- but DuckLake detects the
// concurrent delete at table granularity, not row or partition granularity,
// and aborts the commit:
//
//	Transaction conflict - attempting to insert into table with index "14"
//	- but another transaction has deleted from it
//
// Observed on the first concurrent dispatch: two chunks hit
// contract_events_stream_v1 together, one lost, and the chunk died 70
// seconds in. The ranges did not overlap; the table did.
//
// Retrying is safe for exactly the reason the range catalog's lease is safe:
// the transaction deletes its own range before inserting, so re-running it
// converges rather than doubling. A conflict also clears on its own -- it
// means another writer committed -- so a short randomised backoff is the
// right shape rather than anything cleverer.
func (p *DuckLakePusher) pushTableWithRetry(ctx context.Context, duckTableName, fullTableName, insertSQL, parquetSourceSQL string) (int64, error) {
	var rows int64
	err := retryOnCommitConflict(ctx, duckLakeCommitAttempts, duckLakeCommitBackoff, duckTableName, func() error {
		var err error
		rows, err = p.pushTableOnce(ctx, duckTableName, fullTableName, insertSQL, parquetSourceSQL)
		return err
	})
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// retryOnCommitConflict runs attempt until it succeeds, fails permanently, or
// runs out of attempts.
//
// Separated from the push so the policy can be tested without a database.
// The policy is where the mistakes live: retrying a permanent error turns an
// immediate clear failure into a slow one, and not retrying a conflict loses
// a chunk that would have succeeded a second later.
func retryOnCommitConflict(ctx context.Context, attempts int, base time.Duration, label string, attempt func() error) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for n := 1; n <= attempts; n++ {
		err := attempt()
		if err == nil {
			if n > 1 {
				log.Printf("[DuckLake] %s: committed on attempt %d", label, n)
			}
			return nil
		}
		if !isDuckLakeCommitConflict(err) {
			// Only conflicts are transient. A schema mismatch or a full disk
			// retried five times is five times the wait and the same failure.
			return err
		}
		lastErr = err
		if n == attempts {
			break
		}
		// Exponential with jitter: without jitter two chunks that collide
		// back off in lockstep and collide again on the same schedule.
		backoff := time.Duration(1<<uint(n-1))*base + time.Duration(rand.Int63n(int64(base)))
		log.Printf("[DuckLake] %s: commit conflict on attempt %d/%d, retrying in %s",
			label, n, attempts, backoff.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("%s: commit conflict persisted across %d attempts: %w", label, attempts, lastErr)
}

// isDuckLakeCommitConflict reports whether an error is DuckLake refusing a
// commit because another transaction touched the same table.
//
// Matched on the message because the duckdb driver surfaces no error code
// through database/sql. Narrow on purpose: "transaction conflict" is
// DuckDB's own wording for this case, and matching "conflict" alone would
// also catch constraint violations, which must not be retried.
func isDuckLakeCommitConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "transaction conflict")
}

// pushTableOnce runs one table's delete-and-insert as a single DuckLake
// transaction.
func (p *DuckLakePusher) pushTableOnce(ctx context.Context, duckTableName, fullTableName, insertSQL, parquetSourceSQL string) (int64, error) {
	// Delete-before-insert for idempotent multi-push. Keep delete and insert
	// in one transaction and fail hard on any table error; otherwise a failed
	// insert after a successful delete can create/expand Bronze gaps while the
	// Nomad allocation still reports success.
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin DuckLake transaction for %s: %w", duckTableName, err)
	}
	committed := false
	defer func() {
		if !committed {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				log.Printf("[DuckLake] rollback failed for %s: %v", duckTableName, rbErr)
			}
		}
	}()

	seqCol := ledgerSequenceColumn(duckTableName)
	if seqCol != "" {
		var minSeq, maxSeq int64
		if p.config.StartLedger > 0 && p.config.EndLedger >= p.config.StartLedger {
			// For chunked repairs, delete the whole requested chunk, not the
			// per-table Parquet min/max. Previous partial pushes can leave stale
			// table tails outside the new Parquet row range, causing false Bronze
			// consistency where operations/effects exist beyond transactions.
			minSeq = int64(p.config.StartLedger)
			maxSeq = int64(p.config.EndLedger)
		} else {
			rangeSQL := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM read_parquet(%s)", seqCol, seqCol, parquetSourceSQL)
			var parquetMinSeq, parquetMaxSeq sql.NullInt64
			if err := tx.QueryRowContext(ctx, rangeSQL).Scan(&parquetMinSeq, &parquetMaxSeq); err != nil {
				return 0, fmt.Errorf("read ledger range for %s: %w", duckTableName, err)
			} else if !parquetMinSeq.Valid || !parquetMaxSeq.Valid {
				minSeq = 0
				maxSeq = -1
			} else {
				minSeq = parquetMinSeq.Int64
				maxSeq = parquetMaxSeq.Int64
			}
		}
		if minSeq <= maxSeq {
			deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN %d AND %d",
				fullTableName, seqCol, minSeq, maxSeq)
			delResult, err := tx.ExecContext(ctx, deleteSQL)
			if err != nil {
				return 0, fmt.Errorf("delete existing %s rows in range %d-%d: %w", duckTableName, minSeq, maxSeq, err)
			}
			deleted, _ := delResult.RowsAffected()
			if deleted > 0 {
				log.Printf("[DuckLake] %s: deleted %d existing rows in range %d-%d (idempotent push)",
					duckTableName, deleted, minSeq, maxSeq)
			}
		}
	}

	log.Printf("[DuckLake] Pushing %s...", duckTableName)
	result, err := tx.ExecContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("insert %s into DuckLake: %w", duckTableName, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit DuckLake transaction for %s: %w", duckTableName, err)
	}
	committed = true

	rows, _ := result.RowsAffected()
	return rows, nil
}

func (p *DuckLakePusher) Push(ctx context.Context, outputDir string) (*DuckLakePushResult, error) {
	pushResult := &DuckLakePushResult{RowCounts: map[string]int64{}}
	if err := p.Setup(ctx); err != nil {
		return nil, err
	}

	// Step 5: Create tables, then the views over them. Views come second
	// because they reference the tables.
	if err := p.createTables(ctx); err != nil {
		return nil, fmt.Errorf("create tables: %w", err)
	}
	// Partitioning goes on before anything is inserted: DuckLake applies the
	// layout to files written from then on, so doing it afterwards leaves the
	// backfill's own files unpartitioned.
	if err := p.setPartitioning(ctx, partitionableTables(p.bronzeSchemaSQL())); err != nil {
		return nil, fmt.Errorf("partition tables: %w", err)
	}
	if err := p.createViews(ctx); err != nil {
		return nil, fmt.Errorf("create views: %w", err)
	}

	// Step 6: Push each bronze table. Fresh extraction runs provide a manifest
	// so an attempt can never ingest stale or overlapping files.
	bronzeDir := filepath.Join(outputDir, "bronze")
	var manifest *domain.AttemptManifest
	var tableNames []string
	if p.config.ManifestPath != "" {
		var err error
		manifest, err = domain.ReadAttemptManifest(p.config.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("read publication manifest: %w", err)
		}
		for tableName := range manifest.Tables {
			tableNames = append(tableNames, tableName)
		}
		sort.Strings(tableNames)
	} else {
		entries, err := os.ReadDir(bronzeDir)
		if err != nil {
			return nil, fmt.Errorf("read bronze dir: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir() && !strings.HasPrefix(entry.Name(), "_meta") {
				tableNames = append(tableNames, entry.Name())
			}
		}
	}

	totalRows := int64(0)
	for _, tableName := range tableNames {
		duckTableName := domain.MapToDuckLakeTable(tableName)
		if duckTableName == "" {
			log.Printf("[DuckLake] Skipping unmapped table: %s", tableName)
			continue
		}
		if p.shouldSkipTable(tableName, duckTableName) {
			log.Printf("[DuckLake] Skipping configured table: %s (%s)", tableName, duckTableName)
			continue
		}
		fullTableName := fmt.Sprintf("%s.%s.%s", p.config.CatalogName, p.config.SchemaName, duckTableName)

		parquetGlob := filepath.Join(bronzeDir, tableName, "**", "*.parquet")
		parquetSourceSQL := "'" + strings.ReplaceAll(filepath.ToSlash(parquetGlob), "'", "''") + "'"
		if manifest != nil {
			table := manifest.Tables[tableName]
			if table.Status == "success_zero" {
				seqCol := ledgerSequenceColumn(duckTableName)
				if seqCol == "" {
					return nil, fmt.Errorf("cannot publish zero-row table %s without a ledger sequence column", duckTableName)
				}
				deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN %d AND %d", fullTableName, seqCol, p.config.StartLedger, p.config.EndLedger)
				if _, err := p.db.ExecContext(ctx, deleteSQL); err != nil {
					return nil, fmt.Errorf("clear zero-row %s range: %w", duckTableName, err)
				}
				pushResult.RowCounts[duckTableName] = 0
				log.Printf("[DuckLake] %s: published validated zero-row range", duckTableName)
				continue
			}
			var files []string
			manifestRoot := filepath.Dir(p.config.ManifestPath)
			for _, published := range table.Files {
				path := filepath.Clean(filepath.Join(manifestRoot, filepath.FromSlash(published.Path)))
				rel, err := filepath.Rel(manifestRoot, path)
				if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					return nil, fmt.Errorf("manifest file escapes attempt root: %q", published.Path)
				}
				info, err := os.Stat(path)
				if err != nil {
					return nil, fmt.Errorf("stat published parquet %s: %w", path, err)
				}
				if info.Size() != published.Bytes {
					return nil, fmt.Errorf("published parquet size changed for %s: got %d, want %d", path, info.Size(), published.Bytes)
				}
				checksum, err := domain.FileSHA256(path)
				if err != nil {
					return nil, fmt.Errorf("checksum published parquet %s: %w", path, err)
				}
				if checksum != published.Checksum {
					return nil, fmt.Errorf("published parquet checksum changed for %s", path)
				}
				files = append(files, path)
			}
			parquetSourceSQL = parquetFileListSQL(files)
		}
		// Get Parquet columns. A failed DESCRIBE here previously fell through to a
		// positional INSERT ... SELECT *, which can silently write misaligned or
		// partial columns into Bronze while still reporting success. Treat any
		// column-discovery failure as fatal so a backfill never reports success on
		// a wrong-shaped insert.
		parquetColSQL := fmt.Sprintf("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet(%s))", parquetSourceSQL)
		parquetCols, err := describeColumns(ctx, p.db, parquetColSQL)
		if err != nil {
			return nil, fmt.Errorf("describe parquet columns for %s: %w", duckTableName, err)
		}
		if len(parquetCols) == 0 {
			return nil, fmt.Errorf("no parquet columns found for %s (glob %s)", duckTableName, parquetGlob)
		}

		// Get DuckLake table columns to find the intersection
		// (Parquet may have extra columns the table doesn't, e.g. list types).
		tableColSQL := fmt.Sprintf("SELECT column_name FROM (DESCRIBE %s)", fullTableName)
		tableCols, err := describeColumns(ctx, p.db, tableColSQL)
		if err != nil {
			return nil, fmt.Errorf("describe table columns for %s: %w", duckTableName, err)
		}
		tableColSet := make(map[string]bool, len(tableCols))
		for _, col := range tableCols {
			tableColSet[col] = true
		}

		// Use only columns that exist in BOTH Parquet and DuckLake table.
		var cols []string
		for _, col := range parquetCols {
			if tableColSet[col] {
				cols = append(cols, col)
			}
		}
		if len(cols) == 0 {
			return nil, fmt.Errorf("no shared columns between parquet and table %s; refusing positional insert", duckTableName)
		}

		// Quote every column name to handle reserved words like "from", "to".
		quoted := make([]string, len(cols))
		for i, c := range cols {
			quoted[i] = fmt.Sprintf(`"%s"`, c)
		}
		colList := strings.Join(quoted, ", ")
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM read_parquet(%s)", fullTableName, colList, colList, parquetSourceSQL)

		rows, err := p.pushTableWithRetry(ctx, duckTableName, fullTableName, insertSQL, parquetSourceSQL)
		if err != nil {
			return nil, err
		}
		totalRows += rows
		pushResult.RowCounts[duckTableName] = rows
		log.Printf("[DuckLake] %s: %d rows", duckTableName, rows)
	}

	pushResult.TotalRows = totalRows
	log.Printf("[DuckLake] Push complete: %d total rows", totalRows)
	return pushResult, nil
}

func parquetFileListSQL(files []string) string {
	quoted := make([]string, len(files))
	for i, file := range files {
		quoted[i] = "'" + strings.ReplaceAll(filepath.ToSlash(file), "'", "''") + "'"
	}
	return "[" + strings.Join(quoted, ",") + "]"
}

func (p *DuckLakePusher) shouldSkipTable(sourceName, duckTableName string) bool {
	if len(p.config.OnlyTables) > 0 && !domain.TableSetIncludes(p.config.OnlyTables, sourceName, duckTableName) {
		return true
	}
	return p.config.SkipTables[sourceName] || p.config.SkipTables[duckTableName]
}

// describeColumns runs a DESCRIBE-style query and returns the column names. It
// fails hard on query or scan errors instead of silently returning an empty set,
// because an empty set would let callers fall back to a positional INSERT that
// can write misaligned columns while reporting success.
func describeColumns(ctx context.Context, db *sql.DB, query string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// duckLakeCatalogTarget turns a catalog DSN into the target DuckLake ATTACHes.
//
// DuckLake can keep its catalog in several places and the prefix is how it
// tells them apart: `postgres:` for PostgreSQL, `sqlite:` for SQLite, and a
// bare path for a DuckDB database file. This was hardcoded to `postgres:`,
// which meant the only supported catalog was PostgreSQL.
//
// A DuckDB file catalog is worth having for two different reasons. It removes
// the PostgreSQL dependency from the integration tests, which otherwise need
// a container to run at all; and it is a plausible long-term home for the
// catalog, since the thing reading it is DuckDB either way.
//
// Note for anyone choosing one: a DuckDB catalog file is written in the
// DuckDB storage format of whatever version created it, and 2.0.0's format is
// not backward compatible with 1.5.x. A PostgreSQL catalog has no such
// coupling -- every service reads it through SQL regardless of which DuckDB
// it links. With six services on their own pins, that difference is the whole
// decision.
func duckLakeCatalogTarget(dsn string) string {
	// Already explicit: the caller said exactly what it wants.
	if strings.HasPrefix(dsn, "ducklake:") {
		return dsn
	}
	for _, prefix := range []string{"postgres:", "postgresql:", "sqlite:", "mysql:"} {
		if strings.HasPrefix(dsn, prefix) {
			// postgres://host/db is a URL PostgreSQL understands; sqlite: and
			// mysql: are DuckLake's own prefixes. Both pass through with the
			// prefix DuckLake expects.
			if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
				return "ducklake:postgres:" + dsn
			}
			return "ducklake:" + dsn
		}
	}
	// A key=value PostgreSQL DSN -- "host=.. user=.. dbname=.." -- rather than
	// a path. Distinguished by the "=" a filename does not have.
	if strings.Contains(dsn, "=") && !strings.Contains(dsn, "/") {
		return "ducklake:postgres:" + dsn
	}
	// Anything else is a path: a DuckDB database file.
	return "ducklake:" + dsn
}

// validateSchemaNames rejects a store and view schema that are the same.
//
// A table and a view cannot share a name within one schema, so every table
// would create and then every view would fail against it. createViews returns
// that error rather than logging it, so the run would stop -- but only after
// writing 21 tables, and with a message about one view rather than about the
// configuration. Checked up front instead.
func validateSchemaNames(store, view string) error {
	if store == view {
		return fmt.Errorf("--ducklake-schema and --ducklake-view-schema are both %q; the tables "+
			"and the views that render them cannot share a schema, because a view would have to "+
			"take the name of the table it reads", store)
	}
	if store == "" {
		return fmt.Errorf("--ducklake-schema is empty")
	}
	if view == "" {
		return fmt.Errorf("--ducklake-view-schema is empty")
	}
	return nil
}

// qualifySchemaSQL rewrites the two schema placeholders to the configured
// catalog and schemas. `bronze_store.` names physical tables and `bronze.`
// names the presentation views; they are separate prefixes because the same
// SQL text would otherwise be ambiguous about which layer it meant, and
// getting that backwards would create tables where consumers expect views.
//
// Order is safe in either direction -- "bronze_store." does not contain
// "bronze." as a substring -- but store is rewritten first to make the
// intent explicit rather than to rely on that.
func (p *DuckLakePusher) qualifySchemaSQL(sql string) string {
	sql = strings.ReplaceAll(sql, "bronze_store.",
		fmt.Sprintf("%s.%s.", p.config.CatalogName, p.config.SchemaName))
	return strings.ReplaceAll(sql, "bronze.",
		fmt.Sprintf("%s.%s.", p.config.CatalogName, p.config.ViewSchemaName))
}

// duckLakePartitionColumn is the bucket every Bronze table is partitioned on.
//
// 10,000-ledger ranges, which is rule 6 of the Bronze storage contract and the
// unit backfill-scope, the range catalog and the flusher already speak. The
// Parquet the loader writes is already grouped this way -- the writer keys its
// output directories off LedgerRange -- so this tells DuckLake about a layout
// the data already has rather than imposing a new one.
const duckLakePartitionColumn = "ledger_range"

// partitionableTables returns the tables the schema declares with a
// ledger_range column.
//
// Derived from the schema rather than hardcoded. Every Bronze table has the
// column today, so a list in code would be 21 names that silently stop
// matching the schema the moment a table is added -- and a table that misses
// partitioning is not visible until someone measures a query.
func partitionableTables(schemaSQL string) []string {
	var out []string
	for _, stmt := range splitSQLStatements(schemaSQL) {
		name, body, ok := parseCreateTable(stmt)
		if !ok {
			continue
		}
		for _, col := range body {
			if col == duckLakePartitionColumn {
				out = append(out, name)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// parseCreateTable pulls the table name and column names out of a CREATE
// TABLE statement, after splitSQLStatements has already stripped comments.
func parseCreateTable(stmt string) (string, []string, bool) {
	const marker = "CREATE TABLE IF NOT EXISTS "
	i := strings.Index(stmt, marker)
	if i < 0 {
		return "", nil, false
	}
	rest := stmt[i+len(marker):]
	open := strings.Index(rest, "(")
	if open < 0 {
		return "", nil, false
	}
	qualified := strings.TrimSpace(rest[:open])
	name := qualified
	if dot := strings.LastIndex(qualified, "."); dot >= 0 {
		name = qualified[dot+1:]
	}

	var cols []string
	for _, line := range strings.Split(rest[open+1:], "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if line == "" || line == ")" {
			continue
		}
		if f := strings.Fields(line); len(f) >= 2 {
			cols = append(cols, strings.Trim(f[0], `"`))
		}
	}
	return name, cols, name != ""
}

// setPartitioning partitions each table by ledger_range.
//
// Runs after createTables and before any insert, because DuckLake applies a
// partition layout to files written from then on -- setting it after a
// backfill would leave every file already written unpartitioned, which is the
// state a loader-created catalog is in today.
//
// Errors are returned rather than logged. Every Bronze table declares
// ledger_range, so there is no table this can legitimately fail for, and a
// partitioning that silently did not happen shows up much later as a query
// reading more files than it should.
func (p *DuckLakePusher) setPartitioning(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return fmt.Errorf("no tables declare %s; the schema parser is broken", duckLakePartitionColumn)
	}
	for _, table := range tables {
		sql := fmt.Sprintf("ALTER TABLE %s.%s.%s SET PARTITIONED BY (%s)",
			p.config.CatalogName, p.config.SchemaName, table, duckLakePartitionColumn)
		if _, err := p.db.ExecContext(ctx, sql); err != nil {
			return fmt.Errorf("partition %s by %s: %w", table, duckLakePartitionColumn, err)
		}
	}
	log.Printf("[DuckLake] Partitioned %d tables by %s", len(tables), duckLakePartitionColumn)
	return nil
}

// createViews creates the presentation views over the physical tables.
//
// Unlike createTables, a failure here is returned rather than logged. A table
// that fails to create shows up immediately as a failed insert; a view that
// fails to create shows up as consumers reading nothing, which is the kind of
// silence this layer has produced before.
func (p *DuckLakePusher) createViews(ctx context.Context) error {
	for _, stmt := range splitSQLStatements(p.qualifySchemaSQL(embeddedBronzeViews)) {
		if _, err := p.db.ExecContext(ctx, stmt+";"); err != nil {
			return fmt.Errorf("create presentation view: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}

// createTables creates DuckLake tables from the bronze schema SQL.
// bronzeSchemaSQL returns the table schema this run uses: the embedded copy,
// or the file named by --ducklake-schema-sql when one is given.
//
// Shared with the partitioning step so both read the same definitions. Parsing
// the embedded schema while creating tables from a file override would
// partition a different set of tables than it created.
func (p *DuckLakePusher) bronzeSchemaSQL() string {
	if p.config.BronzeSchemaSQL != "" {
		if data, err := os.ReadFile(p.config.BronzeSchemaSQL); err == nil {
			return string(data)
		}
	}
	return embeddedBronzeSchema
}

func (p *DuckLakePusher) createTables(ctx context.Context) error {
	schemaSQL := p.qualifySchemaSQL(p.bronzeSchemaSQL())

	for _, stmt := range splitSQLStatements(schemaSQL) {
		if _, err := p.db.ExecContext(ctx, stmt+";"); err != nil {
			log.Printf("[DuckLake] Schema warning: %v", err)
		}
	}
	return nil
}

// splitSQLStatements splits a schema file into executable statements.
//
// Comment lines are stripped before splitting, not after. Splitting first
// looks equivalent and is not: a semicolon inside a comment ends a statement
// that was never a statement, and the remainder of the comment arrives at the
// parser as bare SQL. Both schema files are mostly comment by line count,
// because every column decision carries its rationale, so prose with a
// semicolon in it is ordinary rather than exotic.
func splitSQLStatements(sql string) []string {
	var code []string
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		code = append(code, line)
	}

	var out []string
	for _, stmt := range strings.Split(strings.Join(code, "\n"), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// ledgerSequenceColumn returns the ledger sequence column name for a given table.
// Returns empty string for tables without a clear ledger sequence column.
func ledgerSequenceColumn(tableName string) string {
	switch tableName {
	case "ledgers_row_v2":
		return "sequence"
	case "contract_creations_v1":
		return "created_ledger"
	default:
		return "ledger_sequence"
	}
}
