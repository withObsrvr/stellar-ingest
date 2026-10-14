package main

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

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

// DuckLakeConfig holds configuration for pushing Parquet to DuckLake.
type DuckLakeConfig struct {
	CatalogDSN      string // PostgreSQL catalog connection string
	DataPath        string // S3/B2 bucket path (e.g., "s3://obsrvr-lake-testnet/")
	MetadataSchema  string // Catalog schema (e.g., "bronze_meta")
	CatalogName     string // DuckLake catalog name (e.g., "lake")
	SchemaName      string // Schema within catalog (e.g., "bronze")
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
type DuckLakePushResult struct {
	TotalRows int64
	RowCounts map[string]int64
}

func NewDuckLakePusher(config DuckLakeConfig) (*DuckLakePusher, error) {
	if config.CatalogName == "" {
		config.CatalogName = "lake"
	}
	if config.SchemaName == "" {
		config.SchemaName = "bronze"
	}
	if config.MetadataSchema == "" {
		config.MetadataSchema = "bronze_meta"
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
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
	catalogPath := fmt.Sprintf("ducklake:postgres:%s", p.config.CatalogDSN)

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

	// Step 4: Create schema
	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", p.config.CatalogName, p.config.SchemaName)
	if _, err := p.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

// Push reads local Parquet files and inserts them into DuckLake.
func (p *DuckLakePusher) Push(ctx context.Context, outputDir string) (*DuckLakePushResult, error) {
	pushResult := &DuckLakePushResult{RowCounts: map[string]int64{}}
	if err := p.Setup(ctx); err != nil {
		return nil, err
	}

	// Step 5: Create tables
	if err := p.createTables(ctx); err != nil {
		return nil, fmt.Errorf("create tables: %w", err)
	}

	// Step 6: Push each bronze table. Fresh extraction runs provide a manifest
	// so an attempt can never ingest stale or overlapping files.
	bronzeDir := filepath.Join(outputDir, "bronze")
	var manifest *AttemptManifest
	var tableNames []string
	if p.config.ManifestPath != "" {
		var err error
		manifest, err = ReadAttemptManifest(p.config.ManifestPath)
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
		duckTableName := mapToDuckLakeTable(tableName)
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
				checksum, err := fileSHA256(path)
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

		// Delete-before-insert for idempotent multi-push. Keep delete and insert
		// in one transaction and fail hard on any table error; otherwise a failed
		// insert after a successful delete can create/expand Bronze gaps while the
		// Nomad allocation still reports success.
		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("begin DuckLake transaction for %s: %w", duckTableName, err)
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
					return nil, fmt.Errorf("read ledger range for %s: %w", duckTableName, err)
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
					return nil, fmt.Errorf("delete existing %s rows in range %d-%d: %w", duckTableName, minSeq, maxSeq, err)
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
			return nil, fmt.Errorf("insert %s into DuckLake: %w", duckTableName, err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit DuckLake transaction for %s: %w", duckTableName, err)
		}
		committed = true

		rows, _ := result.RowsAffected()
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
	if len(p.config.OnlyTables) > 0 && !tableSetIncludes(p.config.OnlyTables, sourceName, duckTableName) {
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

// createTables creates DuckLake tables from the bronze schema SQL.
func (p *DuckLakePusher) createTables(ctx context.Context) error {
	schemaSQL := embeddedBronzeSchema
	if p.config.BronzeSchemaSQL != "" {
		if data, err := os.ReadFile(p.config.BronzeSchemaSQL); err == nil {
			schemaSQL = string(data)
		}
	}

	schemaSQL = strings.ReplaceAll(schemaSQL, "bronze.", fmt.Sprintf("%s.%s.", p.config.CatalogName, p.config.SchemaName))

	for _, stmt := range strings.Split(schemaSQL, ";") {
		// Strip comment lines before evaluating the fragment.
		// A naive HasPrefix("--") check would skip the entire CREATE TABLE
		// when a comment like "-- Core tables" sits between two semicolons.
		var cleaned []string
		for _, line := range strings.Split(stmt, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			cleaned = append(cleaned, line)
		}
		stmt = strings.TrimSpace(strings.Join(cleaned, "\n"))
		if stmt == "" {
			continue
		}
		if _, err := p.db.ExecContext(ctx, stmt+";"); err != nil {
			log.Printf("[DuckLake] Schema warning: %v", err)
		}
	}
	return nil
}

// mapToDuckLakeTable maps Parquet directory names to DuckLake table names.
func mapToDuckLakeTable(dirName string) string {
	return duckLakeTableBySource[dirName]
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
