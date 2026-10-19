package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// HotBufferLoader loads the most recent ledgers from Parquet into PostgreSQL
// for real-time query serving. Uses COPY for maximum throughput.
type HotBufferLoader struct {
	db        *sql.DB
	bronzeDir string
	silverDir string
	duckdb    *sql.DB // for reading Parquet
}

// HotBufferConfig configures the PostgreSQL hot buffer connection.
type HotBufferConfig struct {
	Host        string
	Port        int
	Database    string
	User        string
	Password    string
	SSLMode     string
	TailLedgers uint32 // how many recent ledgers to load (default 100000)
}

func (c HotBufferConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.Database, c.User, c.Password, c.SSLMode)
}

func NewHotBufferLoader(outputDir string, pgConfig HotBufferConfig) (*HotBufferLoader, error) {
	pgDSN := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		pgConfig.User, pgConfig.Password, pgConfig.Host, pgConfig.Port, pgConfig.Database, pgConfig.SSLMode)

	pgDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pgDB.Ping(); err != nil {
		pgDB.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	duckDB, err := openDuckDB()
	if err != nil {
		pgDB.Close()
		return nil, err
	}

	return &HotBufferLoader{
		db:        pgDB,
		bronzeDir: filepath.Join(outputDir, "bronze"),
		silverDir: filepath.Join(outputDir, "silver"),
		duckdb:    duckDB,
	}, nil
}

func (h *HotBufferLoader) Close() error {
	h.duckdb.Close()
	return h.db.Close()
}

// LoadTail loads the most recent N ledgers from Parquet into PostgreSQL hot buffer.
func (h *HotBufferLoader) LoadTail(ctx context.Context, tailLedgers uint32, endLedger uint32) error {
	startLedger := uint32(0)
	if endLedger >= tailLedgers {
		startLedger = endLedger - tailLedgers + 1 // +1 for exclusive start, inclusive end
	}

	log.Printf("[HotBuffer] Loading tail: ledgers %d-%d into PostgreSQL", startLedger, endLedger)

	tables := []struct {
		name      string
		bronzeDir string
		columns   string
		seqFilter string // column name for ledger_sequence filtering
	}{
		{
			name:      "transactions_row_v2",
			bronzeDir: "transactions",
			columns:   "ledger_sequence, transaction_hash, source_account, fee_charged, max_fee, successful, transaction_result_code, operation_count, memo_type, memo, created_at, account_sequence, ledger_range, signatures_count, tx_envelope, envelope_size_bytes, tx_result, tx_meta, tx_fee_meta, tx_signers, new_account, rent_fee_charged, soroban_resources_instructions, soroban_resources_read_bytes, soroban_resources_write_bytes",
			seqFilter: "ledger_sequence",
		},
		{
			name:      "operations_row_v2",
			bronzeDir: "operations",
			// Column names match v3_bronze_schema.sql / streaming PG hot:
			// the operation type column is `type`, not `op_type`. Loader's
			// Parquet output also uses `type` (see ParquetOperation tag).
			columns:   "ledger_sequence, transaction_hash, operation_index, source_account, type, type_string, created_at, transaction_successful, operation_result_code, ledger_range, amount, asset, destination, soroban_operation, soroban_contract_id, soroban_function, soroban_arguments_json, contract_calls_json, max_call_depth",
			seqFilter: "ledger_sequence",
		},
		{
			name:      "contract_events_stream_v1",
			bronzeDir: "contract_events",
			columns:   "event_id, contract_id, ledger_sequence, transaction_hash, closed_at, event_type, in_successful_contract_call, topics_json, topics_decoded, data_xdr, data_decoded, topic_count, topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded, operation_index, event_index, ledger_range",
			seqFilter: "ledger_sequence",
		},
		{
			name:      "accounts_snapshot_v1",
			bronzeDir: "accounts_snapshot",
			columns:   "account_id, ledger_sequence, closed_at, balance, sequence_number, sequence_ledger, sequence_time, num_subentries, num_sponsoring, num_sponsored, home_domain, master_weight, low_threshold, med_threshold, high_threshold, flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled, signers, sponsor_account, ledger_range",
			seqFilter: "ledger_sequence",
		},
		{
			name:      "native_balances_snapshot_v1",
			bronzeDir: "native_balances",
			columns:   "account_id, balance, buying_liabilities, selling_liabilities, num_subentries, num_sponsoring, num_sponsored, sequence_number, last_modified_ledger, ledger_sequence, ledger_range",
			seqFilter: "ledger_sequence",
		},
	}

	for _, t := range tables {
		start := time.Now()
		parquetGlob := filepath.Join(h.bronzeDir, t.bronzeDir, "**", "*.parquet")

		// Read from Parquet with DuckDB, filter to tail range
		query := fmt.Sprintf(
			"SELECT %s FROM read_parquet('%s') WHERE %s >= %d AND %s <= %d",
			t.columns, parquetGlob, t.seqFilter, startLedger, t.seqFilter, endLedger,
		)

		rows, err := h.duckdb.QueryContext(ctx, query)
		if err != nil {
			log.Printf("[HotBuffer] %s: query failed: %v", t.name, err)
			continue
		}

		// Use pgx COPY for bulk insert
		count, err := h.copyRows(ctx, t.name, t.columns, rows)
		rows.Close()
		if err != nil {
			log.Printf("[HotBuffer] %s: copy failed: %v", t.name, err)
			continue
		}

		log.Printf("[HotBuffer] %s: %d rows in %s", t.name, count, time.Since(start).Round(time.Millisecond))
	}

	return nil
}

// copyRows reads from DuckDB result set and COPYs into PostgreSQL.
func (h *HotBufferLoader) copyRows(ctx context.Context, tableName, columns string, rows *sql.Rows) (int64, error) {
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	// Begin transaction
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Create temp table and COPY into it, then INSERT with ON CONFLICT
	tmpTable := fmt.Sprintf("tmp_%s_%d", tableName, time.Now().UnixNano())

	_, err = tx.ExecContext(ctx, fmt.Sprintf(
		"CREATE TEMP TABLE %s (LIKE %s INCLUDING ALL) ON COMMIT DROP", tmpTable, tableName))
	if err != nil {
		return 0, fmt.Errorf("create temp table: %w", err)
	}

	// Build COPY statement
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", tmpTable, columns)
	stmt, err := tx.PrepareContext(ctx, copySQL)
	if err != nil {
		// Fallback: use INSERT if COPY FROM STDIN not supported by driver
		return h.insertRows(ctx, tx, tableName, columns, cols, rows)
	}
	defer stmt.Close()

	var count int64
	values := make([]interface{}, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return count, fmt.Errorf("scan row: %w", err)
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return count, fmt.Errorf("copy row: %w", err)
		}
		count++
	}

	// Insert from temp to real table with ON CONFLICT DO NOTHING
	_, err = tx.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT DO NOTHING",
		tableName, columns, columns, tmpTable))
	if err != nil {
		return count, fmt.Errorf("insert from temp: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("commit: %w", err)
	}

	return count, nil
}

// insertRows is a fallback that uses batch INSERT instead of COPY.
func (h *HotBufferLoader) insertRows(ctx context.Context, tx *sql.Tx, tableName, columns string, cols []string, rows *sql.Rows) (int64, error) {
	values := make([]interface{}, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Build parameterized INSERT
	placeholders := ""
	for i := range cols {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		tableName, columns, placeholders)

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	var count int64
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return count, fmt.Errorf("scan row: %w", err)
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return count, fmt.Errorf("insert row %d: %w", count, err)
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("commit: %w", err)
	}

	return count, nil
}
