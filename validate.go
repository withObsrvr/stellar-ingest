package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"time"
)

// Validator runs quality checks over Parquet output using DuckDB.
type Validator struct {
	db        *sql.DB
	bronzeDir string
}

func NewValidator(outputDir string) (*Validator, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb for validation: %w", err)
	}
	return &Validator{
		db:        db,
		bronzeDir: filepath.Join(outputDir, "bronze"),
	}, nil
}

func (v *Validator) Close() error {
	return v.db.Close()
}

// RunAll runs all quality checks and reports results.
func (v *Validator) RunAll(ctx context.Context) error {
	checks := []struct {
		name string
		fn   func(context.Context) (bool, string, error)
	}{
		{"monotonic_ledger_sequence", v.checkMonotonicSequence},
		{"no_empty_tables", v.checkNoEmptyTables},
		{"transaction_operation_consistency", v.checkTxOpConsistency},
		{"pipeline_version_present", v.checkPipelineVersion},
	}

	fmt.Println("=== Quality Validation ===")
	passed := 0
	failed := 0

	for _, check := range checks {
		start := time.Now()
		ok, detail, err := check.fn(ctx)
		elapsed := time.Since(start).Round(time.Millisecond)

		if err != nil {
			log.Printf("[Quality] %s: ERROR (%v) [%s]", check.name, err, elapsed)
			failed++
			continue
		}

		if ok {
			fmt.Printf("  PASS  %-40s %s [%s]\n", check.name, detail, elapsed)
			passed++
		} else {
			fmt.Printf("  FAIL  %-40s %s [%s]\n", check.name, detail, elapsed)
			failed++
		}
	}

	fmt.Printf("\nResults: %d passed, %d failed\n", passed, failed)

	if failed > 0 {
		return fmt.Errorf("%d quality check(s) failed", failed)
	}
	return nil
}

// checkMonotonicSequence verifies ledger sequences have no gaps in transactions.
func (v *Validator) checkMonotonicSequence(ctx context.Context) (bool, string, error) {
	glob := filepath.Join(v.bronzeDir, "transactions", "**", "*.parquet")
	query := fmt.Sprintf(`
		SELECT COUNT(*) as gaps FROM (
			SELECT ledger_sequence,
				LAG(ledger_sequence) OVER (ORDER BY ledger_sequence) as prev_seq
			FROM (SELECT DISTINCT ledger_sequence FROM read_parquet('%s'))
		) WHERE prev_seq IS NOT NULL AND ledger_sequence != prev_seq + 1
	`, glob)

	var gaps int64
	if err := v.db.QueryRowContext(ctx, query).Scan(&gaps); err != nil {
		return false, "", err
	}

	if gaps == 0 {
		return true, "no sequence gaps found", nil
	}
	return false, fmt.Sprintf("%d sequence gap(s) found", gaps), nil
}

// checkNoEmptyTables verifies that high-volume tables have data.
func (v *Validator) checkNoEmptyTables(ctx context.Context) (bool, string, error) {
	expectedTables := []string{"transactions", "operations", "accounts_snapshot", "native_balances"}
	emptyTables := []string{}

	for _, table := range expectedTables {
		glob := filepath.Join(v.bronzeDir, table, "**", "*.parquet")
		query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", glob)
		var count int64
		if err := v.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			continue // table might not exist
		}
		if count == 0 {
			emptyTables = append(emptyTables, table)
		}
	}

	if len(emptyTables) == 0 {
		return true, fmt.Sprintf("all %d core tables have data", len(expectedTables)), nil
	}
	return false, fmt.Sprintf("empty tables: %v", emptyTables), nil
}

// checkTxOpConsistency verifies operations reference valid transactions.
func (v *Validator) checkTxOpConsistency(ctx context.Context) (bool, string, error) {
	txGlob := filepath.Join(v.bronzeDir, "transactions", "**", "*.parquet")
	opGlob := filepath.Join(v.bronzeDir, "operations", "**", "*.parquet")

	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			SELECT DISTINCT o.transaction_hash
			FROM read_parquet('%s') o
			LEFT JOIN read_parquet('%s') t ON o.transaction_hash = t.transaction_hash
			WHERE t.transaction_hash IS NULL
		)
	`, opGlob, txGlob)

	var orphans int64
	if err := v.db.QueryRowContext(ctx, query).Scan(&orphans); err != nil {
		return false, "", err
	}

	if orphans == 0 {
		return true, "all operations reference valid transactions", nil
	}
	return false, fmt.Sprintf("%d orphaned operation transaction hashes", orphans), nil
}

// checkPipelineVersion verifies version_label is populated. (Column was
// renamed from pipeline_version to version_label so the loader matches
// v3_bronze_schema.sql / streaming PG hot.)
func (v *Validator) checkPipelineVersion(ctx context.Context) (bool, string, error) {
	glob := filepath.Join(v.bronzeDir, "transactions", "**", "*.parquet")
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT version_label) as versions,
			COUNT(*) FILTER (WHERE version_label IS NULL OR version_label = '') as missing
		FROM read_parquet('%s')
	`, glob)

	var versions, missing int64
	if err := v.db.QueryRowContext(ctx, query).Scan(&versions, &missing); err != nil {
		return false, "", err
	}

	if missing > 0 {
		return false, fmt.Sprintf("%d rows missing version_label", missing), nil
	}
	return true, fmt.Sprintf("%d distinct version(s)", versions), nil
}
