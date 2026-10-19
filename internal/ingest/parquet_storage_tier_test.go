package ingest

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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/parity"
)

// parquetRowType maps each Bronze table to the struct that defines its
// stored columns.
//
// Written out so the column names can be read off the parquet tags rather
// than guessed from the Go field names. Guessing was its own source of
// false findings: OpType stores as `type`, PipelineVersion as
// `version_label`, and CreatedAt became `closed_at` on some tables and
// `ingested_at` on others. The tag is the only authority on what a column
// is called.
var parquetRowType = map[string]reflect.Type{
	"transactions":       reflect.TypeOf(ParquetTransaction{}),
	"operations":         reflect.TypeOf(ParquetOperation{}),
	"effects":            reflect.TypeOf(ParquetEffect{}),
	"trades":             reflect.TypeOf(ParquetTrade{}),
	"accounts":           reflect.TypeOf(ParquetAccount{}),
	"offers":             reflect.TypeOf(ParquetOffer{}),
	"trustlines":         reflect.TypeOf(ParquetTrustline{}),
	"account_signers":    reflect.TypeOf(ParquetAccountSigner{}),
	"claimable_balances": reflect.TypeOf(ParquetClaimableBalance{}),
	"liquidity_pools":    reflect.TypeOf(ParquetLiquidityPool{}),
	"config_settings":    reflect.TypeOf(ParquetConfigSetting{}),
	"ttl":                reflect.TypeOf(ParquetTTL{}),
	"evicted_keys":       reflect.TypeOf(ParquetEvictedKey{}),
	"contract_events":    reflect.TypeOf(ParquetContractEvent{}),
	"contract_data":      reflect.TypeOf(ParquetContractData{}),
	"contract_code":      reflect.TypeOf(ParquetContractCode{}),
	"native_balances":    reflect.TypeOf(ParquetNativeBalance{}),
	"restored_keys":      reflect.TypeOf(ParquetRestoredKey{}),
	"contract_creations": reflect.TypeOf(ParquetContractCreation{}),
	"ledgers":            reflect.TypeOf(ParquetLedger{}),
	"token_transfers":    reflect.TypeOf(ParquetTokenTransfer{}),
}

func TestParquetRowTypeRegistryIsComplete(t *testing.T) {
	if len(parquetRowType) != len(parity.AllTables) {
		t.Fatalf("registry covers %d tables, Ticket 04 names %d", len(parquetRowType), len(parity.AllTables))
	}
	for _, name := range parity.AllTables {
		if _, ok := parquetRowType[name]; !ok {
			t.Errorf("table %q has no Parquet row type registered", name)
		}
	}
}

// parquetColumn returns the stored column name for a row field, read from
// the Parquet struct tag. Returns false when the struct has no such field,
// which means the Parquet layer does not carry it — the coverage audit's
// business, not this test's.
func parquetColumn(table, field string) (string, bool) {
	rt, ok := parquetRowType[table]
	if !ok {
		return "", false
	}
	f, ok := rt.FieldByName(field)
	if !ok {
		return "", false
	}
	tag := f.Tag.Get("parquet")
	if tag == "" {
		return "", false
	}
	name, _, _ := strings.Cut(tag, ",")
	return name, name != ""
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
			col, mapped := parquetColumn(table, field)
			if !mapped {
				// The Parquet struct does not carry this field at all.
				// That is TestParquetLayerWritesEveryRowField's business,
				// and it is allowlisted there when deliberate.
				continue
			}
			if !have[col] {
				t.Errorf("%s: field %s maps to column %q, which the written files do not have",
					table, field, col)
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
