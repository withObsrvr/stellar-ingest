package adapters

// The quality checks, against Parquet that reproduces the shapes that broke
// them.
//
// Both of these were found by running a 100,000-ledger slice from ledger 3
// and neither had any test:
//
//  1. monotonic_ledger_sequence read the TRANSACTIONS table and demanded a
//     gapless ledger_sequence. That holds on Soroban-era data, where every
//     ledger carries transactions, and is simply false for early history:
//     52 of those 100,000 ledgers had any transactions at all, so the check
//     reported "51 sequence gap(s)" -- exactly the 51 discontinuities
//     between them. The data was perfectly contiguous.
//
//  2. transaction_operation_consistency joined operations to transactions on
//     transaction_hash, a column the storage contract dropped from
//     operations. DuckDB reported a binder error, the harness counted it as
//     a failed check, and --validate had been failing on every run since --
//     unnoticed, because the Nomad validation job never passes --validate.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeParquet materialises one table of the Bronze layout from a SELECT.
func writeParquet(t *testing.T, v *Validator, table, selectSQL string) {
	t.Helper()
	dir := filepath.Join(v.bronzeDir, table, "part")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, "000.parquet")
	if _, err := v.db.Exec(fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET)`, selectSQL, path)); err != nil {
		t.Fatalf("write %s: %v", table, err)
	}
}

func newTestValidator(t *testing.T) *Validator {
	t.Helper()
	v, err := NewValidator(t.TempDir(), nil)
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	t.Cleanup(func() { v.Close() })
	return v
}

// The genesis shape: every ledger present and contiguous, almost none of
// them carrying a transaction.
func TestMonotonicCheckPassesOnSparseHistory(t *testing.T) {
	v := newTestValidator(t)

	// 1000 contiguous ledgers from 3.
	writeParquet(t, v, "ledgers", `SELECT i AS sequence FROM range(3, 1003) t(i)`)
	// Transactions in only five of them, far apart -- four discontinuities.
	writeParquet(t, v, "transactions",
		`SELECT * FROM (VALUES (3),(200),(500),(777),(1002)) AS t(ledger_sequence)`)

	ok, detail, err := v.checkMonotonicSequence(context.Background())
	if err != nil {
		t.Fatalf("check errored: %v", err)
	}
	if !ok {
		t.Errorf("contiguous ledgers reported as gapped (%s). The check is reading a table "+
			"where contiguity is not an invariant: on early history almost no ledger carries "+
			"a transaction.", detail)
	}
}

// And it must still catch a real gap, or the fix has just disabled it.
func TestMonotonicCheckCatchesARealGap(t *testing.T) {
	v := newTestValidator(t)

	// 3..100 then 200..300: one genuine hole in the ledgers themselves.
	writeParquet(t, v, "ledgers",
		`SELECT i AS sequence FROM range(3, 101) t(i)
		 UNION ALL SELECT i FROM range(200, 301) t(i)`)
	writeParquet(t, v, "transactions", `SELECT 3 AS ledger_sequence`)

	ok, detail, err := v.checkMonotonicSequence(context.Background())
	if err != nil {
		t.Fatalf("check errored: %v", err)
	}
	if ok {
		t.Error("a real gap in the ledgers table was not reported; the check is now inert")
	}
	if detail == "" {
		t.Error("no detail reported for a failing check")
	}
}

// The consistency check must bind at all -- it did not, for every run since
// operations.transaction_hash was dropped.
func TestConsistencyCheckBindsAgainstTheCompactSchema(t *testing.T) {
	v := newTestValidator(t)

	writeParquet(t, v, "transactions",
		`SELECT * FROM (VALUES (100::BIGINT),(200::BIGINT)) AS t(transaction_id)`)
	writeParquet(t, v, "operations",
		`SELECT * FROM (VALUES (100::BIGINT),(100::BIGINT),(200::BIGINT)) AS t(transaction_id)`)

	ok, detail, err := v.checkTxOpConsistency(context.Background())
	if err != nil {
		t.Fatalf("check errored, which is how it failed silently for months: %v", err)
	}
	if !ok {
		t.Errorf("operations that all reference real transactions were reported as orphaned: %s", detail)
	}
}

// And it must still find a genuine orphan.
func TestConsistencyCheckCatchesAnOrphan(t *testing.T) {
	v := newTestValidator(t)

	writeParquet(t, v, "transactions", `SELECT 100::BIGINT AS transaction_id`)
	writeParquet(t, v, "operations",
		`SELECT * FROM (VALUES (100::BIGINT),(999::BIGINT)) AS t(transaction_id)`)

	ok, detail, err := v.checkTxOpConsistency(context.Background())
	if err != nil {
		t.Fatalf("check errored: %v", err)
	}
	if ok {
		t.Error("an operation referencing a transaction that does not exist was not reported")
	}
	if detail == "" {
		t.Error("no detail reported for a failing check")
	}
}
