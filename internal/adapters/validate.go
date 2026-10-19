package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/logging"
)

// Validator runs quality checks over Parquet output using DuckDB.
type Validator struct {
	db        *sql.DB
	bronzeDir string
	log       *logging.Logger
}

func NewValidator(outputDir string, log *logging.Logger) (*Validator, error) {
	db, err := OpenDuckDB()
	if err != nil {
		return nil, fmt.Errorf("open duckdb for validation: %w", err)
	}
	return &Validator{
		db:        db,
		bronzeDir: filepath.Join(outputDir, "bronze"),
		log:       log,
	}, nil
}

func (v *Validator) Close() error {
	return v.db.Close()
}

// RunAll runs all quality checks and reports results.
// RunAll runs every quality check and reports how many passed and failed.
//
// The counts are returned rather than only narrated because they belong in
// the typed result: "2 passed, 1 failed" as prose on stderr cannot be read
// by anything but a person.
func (v *Validator) RunAll(ctx context.Context) (passedCount, failedCount int, err error) {
	checks := []struct {
		name string
		fn   func(context.Context) (bool, string, error)
	}{
		{"monotonic_ledger_sequence", v.checkMonotonicSequence},
		{"no_empty_tables", v.checkNoEmptyTables},
		{"transaction_operation_consistency", v.checkTxOpConsistency},
		{"pipeline_version_present", v.checkPipelineVersion},
	}

	v.log.Section("Quality Validation")
	passed := 0
	failed := 0

	for _, check := range checks {
		start := time.Now()
		ok, detail, err := check.fn(ctx)
		elapsed := time.Since(start).Round(time.Millisecond)

		if err != nil {
			v.log.Warn("[Quality] %s: ERROR (%v) [%s]", check.name, err, elapsed)
			failed++
			continue
		}

		if ok {
			v.log.Info("  PASS  %-40s %s [%s]", check.name, detail, elapsed)
			passed++
		} else {
			v.log.Warn("  FAIL  %-40s %s [%s]", check.name, detail, elapsed)
			failed++
		}
	}

	v.log.Line()
	v.log.Info("Results: %d passed, %d failed", passed, failed)

	if failed > 0 {
		return passed, failed, fmt.Errorf("%d quality check(s) failed", failed)
	}
	return passed, failed, nil
}

// checkMonotonicSequence verifies ledger sequences have no gaps in transactions.
func (v *Validator) checkMonotonicSequence(ctx context.Context) (bool, string, error) {
	// The ledgers table, not transactions.
	//
	// Contiguity is only an invariant where there is one row per ledger.
	// This read transactions and demanded a gapless sequence, which holds
	// on Soroban-era data because every ledger there carries transactions --
	// and is simply false for early history. A 100,000-ledger slice from
	// ledger 3 has 52 ledgers with any transactions at all, so the check
	// reported "51 sequence gap(s)": exactly the 51 discontinuities between
	// those 52 ledgers. The data was perfectly contiguous; the check was
	// looking at the wrong table.
	glob := filepath.Join(v.bronzeDir, "ledgers", "**", "*.parquet")
	query := fmt.Sprintf(`
		SELECT COUNT(*) as gaps FROM (
			SELECT sequence,
				LAG(sequence) OVER (ORDER BY sequence) as prev_seq
			FROM (SELECT DISTINCT sequence FROM read_parquet('%s'))
		) WHERE prev_seq IS NOT NULL AND sequence != prev_seq + 1
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

	// Joined on transaction_id, not transaction_hash.
	//
	// The storage contract dropped operations.transaction_hash -- 714 GB of
	// hex for an identity transaction_id already carries in 8 bytes -- and
	// this check kept joining on the column that no longer exists. DuckDB
	// reported it as a binder error, which the harness counts as a failed
	// check, so --validate has been failing on every run since that change
	// and nothing noticed: the Nomad validation job never passes --validate.
	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			SELECT DISTINCT o.transaction_id
			FROM read_parquet('%s') o
			LEFT JOIN read_parquet('%s') t ON o.transaction_id = t.transaction_id
			WHERE t.transaction_id IS NULL
		)
	`, opGlob, txGlob)

	var orphans int64
	if err := v.db.QueryRowContext(ctx, query).Scan(&orphans); err != nil {
		return false, "", err
	}

	if orphans == 0 {
		return true, "all operations reference valid transactions", nil
	}
	return false, fmt.Sprintf("%d orphaned operation transaction ids", orphans), nil
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
