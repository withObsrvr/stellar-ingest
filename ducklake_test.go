package main

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestDescribeColumns locks the C2 fix: a failed DESCRIBE must surface an error,
// never an empty column slice. The old Push fell back to a positional
// INSERT ... SELECT * on an empty slice, silently writing misaligned/partial
// columns into Bronze while reporting success.
func TestDescribeColumns(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	t.Run("valid describe returns columns", func(t *testing.T) {
		cols, err := describeColumns(ctx, db, "SELECT column_name FROM (DESCRIBE SELECT 1 AS a, 2 AS b)")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
			t.Fatalf("got %v, want [a b]", cols)
		}
	})

	t.Run("missing parquet describe returns error not empty", func(t *testing.T) {
		cols, err := describeColumns(ctx, db, "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('/nonexistent/path/**/*.parquet'))")
		if err == nil {
			t.Fatalf("expected error for missing parquet, got cols=%v", cols)
		}
	})

	t.Run("missing table describe returns error", func(t *testing.T) {
		if _, err := describeColumns(ctx, db, "SELECT column_name FROM (DESCRIBE no_such_table)"); err == nil {
			t.Fatal("expected error for missing table")
		}
	})
}
