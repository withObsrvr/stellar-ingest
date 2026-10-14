package main

// Ticket 04 storage tier, historical side.
//
// The coverage audit proves the Parquet layer *reads* every row field. It
// cannot prove the value arrives: a field can be read, converted, and still
// land as NULL in every row. tx_max_footprint_entries did exactly that —
// the column existed in the Parquet schema and in the goldens, and the
// conversion never populated it, so it shipped as a column of nulls that
// looked like a successful addition.
//
// So this reads the written files back through DuckDB and compares each
// column's null rate against the same field's null rate in the extracted
// rows. A field that is populated in memory and null on disk is the
// signature of a dropped conversion, and it is invisible to every other
// check in this repository.

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/parity"
)

// parquetColumnFor maps a row field to its Parquet column, for the handful
// of places the names differ.
var parquetColumnFor = map[string]string{
	"PipelineVersion": "version_label",
	// The column predates the Go field name; `type` is what consumers read.
	"OpType": "type",
}

// TestParquetStorageTierPreservesValues extracts a fixture, writes Parquet
// through the real orchestrator, and compares on-disk null rates against
// the extracted rows.
//
// Run on one small fixture by default: this writes real files.
func TestParquetStorageTierPreservesValues(t *testing.T) {
	if testing.Short() {
		t.Skip("writes Parquet through the real orchestrator; skipped under -short")
	}

	name := "pubnet-tiny-dump"
	meta, _ := loadFixtureMeta(t, name)
	tmp := t.TempDir()
	ledgerPath := materializeLedgers(t, filepath.Join("testdata", "fixtures", name), tmp)

	// Extracted rows: what the mapping produced in memory.
	record, rows, err := BuildHistoricalParityRecord(ParityInput{
		Fixture:           name,
		NetworkPassphrase: meta.NetworkPassphrase,
		StartLedger:       meta.StartLedger,
		EndLedger:         meta.EndLedger,
	}, ledgerPath, nil)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}

	// Stored rows: what reached the files.
	scrubEnv(t)
	bronze := runFixture(t, meta, ledgerPath, filepath.Join(tmp, "out"))

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	tables, err := listTableDirs(bronze)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	emitted := map[string]bool{}
	for _, dir := range tables {
		emitted[dir] = true
	}

	for _, table := range parityTableOrder() {
		snap := record.Tables[table]
		if !snap.Emitted || snap.RowCount == 0 {
			continue
		}
		// The output directory is not always the table name: snapshot tables
		// carry a _snapshot suffix. extractorOutputDirByName is the one
		// place that mapping lives, so take it from there rather than
		// restating it.
		dir, ok := extractorOutputDirByName[table]
		if !ok {
			t.Errorf("%s has no entry in extractorOutputDirByName", table)
			continue
		}
		if !emitted[dir] {
			t.Errorf("%s: %d rows extracted, no Parquet output directory %q written",
				table, snap.RowCount, dir)
			continue
		}

		// Files are nested under a ledger_range directory, which is why the
		// golden harness globs the same way.
		glob := filepath.Join(bronze, dir, "**", "*.parquet")

		var stored int64
		if err := db.QueryRow(
			fmt.Sprintf("SELECT count(*) FROM read_parquet(%s)", quoteLiteral(glob)),
		).Scan(&stored); err != nil {
			t.Errorf("%s: count: %v", table, err)
			continue
		}
		if stored != int64(snap.RowCount) {
			t.Errorf("%s: extracted %d rows, stored %d", table, snap.RowCount, stored)
		}

		cols, err := parquetColumns(db, glob)
		if err != nil {
			t.Errorf("%s: describe: %v", table, err)
			continue
		}
		have := map[string]bool{}
		for _, c := range cols {
			have[strings.ToLower(c)] = true
		}

		for _, field := range snap.Columns {
			col := parquetColumnName(field)
			if !have[col] {
				// Absence is the coverage audit's job; report it here only
				// so a reader is not left wondering why a field has no
				// null-rate line.
				t.Errorf("%s: field %s has no Parquet column %q", table, field, col)
				continue
			}
			wantNulls := nullCount(rows.Tables[table], field)
			gotNulls, err := parquetNullCount(db, glob, col)
			if err != nil {
				t.Errorf("%s.%s: %v", table, col, err)
				continue
			}
			if wantNulls == snap.RowCount && gotNulls == int64(snap.RowCount) {
				continue // null in memory, null on disk: consistent
			}
			if gotNulls == int64(snap.RowCount) && wantNulls < snap.RowCount {
				t.Errorf("%s.%s: populated in %d of %d extracted rows but NULL in every stored row — "+
					"the Parquet conversion is dropping the value",
					table, col, snap.RowCount-wantNulls, snap.RowCount)
				continue
			}
			if gotNulls != int64(wantNulls) {
				t.Errorf("%s.%s: %d NULLs extracted, %d NULLs stored",
					table, col, wantNulls, gotNulls)
				continue
			}

			// A dropped field is only NULL on disk if its column is
			// nullable. A non-nullable one silently takes Go's zero value —
			// an empty string, a 0 — which the null check above cannot see.
			// Such a column is constant, so compare distinct counts: varied
			// in memory and constant on disk is the same defect wearing a
			// different mask.
			wantDistinct := distinctCount(rows.Tables[table], field)
			if wantDistinct <= 1 {
				continue
			}
			gotDistinct, err := parquetDistinctCount(db, glob, col)
			if err != nil {
				t.Errorf("%s.%s: %v", table, col, err)
				continue
			}
			if gotDistinct <= 1 {
				t.Errorf("%s.%s: %d distinct values extracted but %d stored — "+
					"the Parquet conversion is dropping the value",
					table, col, wantDistinct, gotDistinct)
			}
		}
	}
}

func parquetColumnName(field string) string {
	if c, ok := parquetColumnFor[field]; ok {
		return c
	}
	return toSnakeCase(field)
}

// toSnakeCase mirrors the parquet tags, which are the Go field names in
// snake case with acronyms kept together (TTL -> ttl, XDR -> xdr, ID -> id).
func toSnakeCase(s string) string {
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		isUpper := r >= 'A' && r <= 'Z'
		if isUpper && i > 0 {
			prevLower := runes[i-1] >= 'a' && runes[i-1] <= 'z'
			prevDigit := runes[i-1] >= '0' && runes[i-1] <= '9'
			nextLower := i+1 < len(runes) && runes[i+1] >= 'a' && runes[i+1] <= 'z'
			if prevLower || prevDigit || nextLower {
				b.WriteByte('_')
			}
		}
		if isUpper {
			b.WriteRune(r - 'A' + 'a')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func nullCount(rows []parity.Row, field string) int {
	n := 0
	for _, r := range rows {
		if v, ok := r[field]; !ok || v == "n" {
			n++
		}
	}
	return n
}

// distinctCount counts distinct non-null values, matching SQL's
// count(DISTINCT col), which ignores NULLs. Counting the null marker here
// would make every nullable column with one real value look like it had two
// in memory and one on disk.
func distinctCount(rows []parity.Row, field string) int {
	seen := map[string]bool{}
	for _, r := range rows {
		v, ok := r[field]
		if !ok || v == "n" {
			continue
		}
		seen[v] = true
	}
	return len(seen)
}

func parquetDistinctCount(db *sql.DB, glob, col string) (int64, error) {
	var n int64
	q := fmt.Sprintf("SELECT count(DISTINCT %s) FROM read_parquet(%s)",
		quoteIdent(col), quoteLiteral(glob))
	if err := db.QueryRow(q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func parquetNullCount(db *sql.DB, glob, col string) (int64, error) {
	var n int64
	q := fmt.Sprintf("SELECT count(*) FROM read_parquet(%s) WHERE %s IS NULL",
		quoteLiteral(glob), quoteIdent(col))
	if err := db.QueryRow(q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func parityTableOrder() []string {
	out := make([]string, 0, 21)
	for _, t := range bronzeTableNames() {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func bronzeTableNames() []string {
	var out []string
	for name := range bronzeTables(&LedgerData{}) {
		out = append(out, name)
	}
	return out
}
