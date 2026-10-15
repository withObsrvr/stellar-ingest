package parity

import "testing"

type txRow struct {
	LedgerSequence uint32
	Hash           string
	FeeCharged     int64
}

func recordWith(t *testing.T, side Side, table string, rows any) *Record {
	t.Helper()
	r := NewRecord(side, "fixture", map[string]string{"rev": "test"})
	if err := r.AddTable(table, rows); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	return r
}

func find(t *testing.T, c FixtureComparison, table string) TableComparison {
	t.Helper()
	for _, tc := range c.Tables {
		if tc.Table == table {
			return tc
		}
	}
	t.Fatalf("table %q missing from comparison", table)
	return TableComparison{}
}

// Every one of the 21 tables must appear in every report. A table that is
// simply absent from the output is indistinguishable from one that was never
// checked, and the ticket's exit gate does not accept a silent exclusion.
func TestCompareReportsAllTablesEvenWhenNeitherSideEmitted(t *testing.T) {
	c := Compare(NewRecord(SideHistorical, "f", nil), NewRecord(SideLive, "f", nil), nil, nil)
	if len(c.Tables) != len(AllTables) {
		t.Fatalf("got %d tables, want %d", len(c.Tables), len(AllTables))
	}
	for _, tc := range c.Tables {
		if tc.Verdict != VerdictBothAbsent {
			t.Fatalf("%s: verdict %q, want %q", tc.Table, tc.Verdict, VerdictBothAbsent)
		}
	}
}

func TestCompareEqualTables(t *testing.T) {
	rows := []txRow{{1, "a", 100}, {1, "b", 200}}
	c := Compare(recordWith(t, SideHistorical, "transactions", rows),
		recordWith(t, SideLive, "transactions", rows), nil, nil)

	if got := find(t, c, "transactions"); got.Verdict != VerdictEqual {
		t.Fatalf("verdict %q, want %q (differing: %v)", got.Verdict, VerdictEqual, got.DifferingColumns)
	}
}

// The two sides fan out their extractors differently — historical runs 21
// goroutines, live runs them in sequence — so identical content can arrive in
// a different order. That is not a discrepancy.
func TestCompareIgnoresRowOrder(t *testing.T) {
	c := Compare(
		recordWith(t, SideHistorical, "transactions", []txRow{{1, "a", 100}, {1, "b", 200}}),
		recordWith(t, SideLive, "transactions", []txRow{{1, "b", 200}, {1, "a", 100}}),
		nil, nil)

	if got := find(t, c, "transactions"); got.Verdict != VerdictEqual {
		t.Fatalf("verdict %q, want %q", got.Verdict, VerdictEqual)
	}
}

func TestCompareLocalizesDifferenceToOneColumn(t *testing.T) {
	c := Compare(
		recordWith(t, SideHistorical, "transactions", []txRow{{1, "a", 100}}),
		recordWith(t, SideLive, "transactions", []txRow{{1, "a", 999}}),
		nil, nil)

	got := find(t, c, "transactions")
	if got.Verdict != VerdictValuesDiffer {
		t.Fatalf("verdict %q, want %q", got.Verdict, VerdictValuesDiffer)
	}
	if len(got.DifferingColumns) != 1 || got.DifferingColumns[0] != "FeeCharged" {
		t.Fatalf("differing columns %v, want only [FeeCharged]", got.DifferingColumns)
	}
}

// A field present on one side only is the most common Ticket 04 finding —
// live carries footprint counts and contract errors the historical row has
// no column for, and vice versa. It must not be reported as a value
// mismatch on the fields both sides do share.
func TestCompareSeparatesExtraColumnsFromValueDifferences(t *testing.T) {
	type liveTxRow struct {
		LedgerSequence    uint32
		Hash              string
		FeeCharged        int64
		ContractErrorType *string
	}
	c := Compare(
		recordWith(t, SideHistorical, "transactions", []txRow{{1, "a", 100}}),
		recordWith(t, SideLive, "transactions", []liveTxRow{{1, "a", 100, nil}}),
		nil, nil)

	got := find(t, c, "transactions")
	if got.Verdict != VerdictEqualOnSharedColumns {
		t.Fatalf("verdict %q, want %q", got.Verdict, VerdictEqualOnSharedColumns)
	}
	if len(got.DifferingColumns) != 0 {
		t.Fatalf("shared columns should be equal, got differing %v", got.DifferingColumns)
	}
	if len(got.LiveOnlyColumns) != 1 || got.LiveOnlyColumns[0] != "ContractErrorType" {
		t.Fatalf("live-only columns %v, want [ContractErrorType]", got.LiveOnlyColumns)
	}
}

func TestCompareFlagsOneSidedTable(t *testing.T) {
	c := Compare(
		recordWith(t, SideHistorical, "restored_keys", []txRow{{1, "a", 1}}),
		NewRecord(SideLive, "f", nil),
		nil, nil)

	got := find(t, c, "restored_keys")
	if got.Verdict != VerdictOneSideAbsent {
		t.Fatalf("verdict %q, want %q", got.Verdict, VerdictOneSideAbsent)
	}
	if got.LiveEmitted {
		t.Fatal("live reported as emitting a table it never registered")
	}
}

// A row-count difference outranks a value difference: losing or inventing
// rows is a worse finding than a column whose values moved, and the report is
// read worst-first.
func TestCompareRanksTablesWorstFirst(t *testing.T) {
	h := NewRecord(SideHistorical, "f", nil)
	l := NewRecord(SideLive, "f", nil)
	_ = h.AddTable("transactions", []txRow{{1, "a", 100}})
	_ = l.AddTable("transactions", []txRow{{1, "a", 999}}) // values differ
	_ = h.AddTable("trades", []txRow{{1, "a", 1}, {1, "b", 2}})
	_ = l.AddTable("trades", []txRow{{1, "a", 1}}) // row count differs
	_ = h.AddTable("effects", []txRow{{1, "a", 1}})
	_ = l.AddTable("effects", []txRow{{1, "a", 1}}) // equal

	c := Compare(h, l, nil, nil)
	if c.Tables[0].Table != "trades" {
		t.Fatalf("worst table is %q, want trades", c.Tables[0].Table)
	}
	if c.Tables[1].Table != "transactions" {
		t.Fatalf("second table is %q, want transactions", c.Tables[1].Table)
	}
}

// A digest comparison across differing canonical rules is meaningless. The
// harness must say so rather than present the mismatch as a data finding.
func TestCompareFlagsCanonicalFormMismatch(t *testing.T) {
	h := NewRecord(SideHistorical, "f", nil)
	l := NewRecord(SideLive, "f", nil)
	l.CanonicalFormVersion = "999"

	if got := Compare(h, l, nil, nil); got.FormMismatch == "" {
		t.Fatal("a canonical form mismatch was not reported")
	}
}

func TestCompareWithRowsCountsUnmatchedRowsOnSharedColumns(t *testing.T) {
	hRows := []txRow{{1, "a", 100}, {1, "b", 200}, {1, "c", 300}}
	lRows := []txRow{{1, "a", 100}, {1, "b", 200}}

	h := recordWith(t, SideHistorical, "transactions", hRows)
	l := recordWith(t, SideLive, "transactions", lRows)
	hSet := &RowSet{Tables: map[string][]Row{"transactions": Rows(hRows)}}
	lSet := &RowSet{Tables: map[string][]Row{"transactions": Rows(lRows)}}

	got := find(t, Compare(h, l, hSet, lSet), "transactions")
	if got.RowsOnlyHistorical == nil || *got.RowsOnlyHistorical != 1 {
		t.Fatalf("rows only historical = %v, want 1", got.RowsOnlyHistorical)
	}
	if got.RowsOnlyLive == nil || *got.RowsOnlyLive != 0 {
		t.Fatalf("rows only live = %v, want 0", got.RowsOnlyLive)
	}
}

func TestCompareWithRowsSamplesDifferingValues(t *testing.T) {
	hRows := []txRow{{1, "a", 100}}
	lRows := []txRow{{1, "a", 999}}

	got := find(t, Compare(
		recordWith(t, SideHistorical, "transactions", hRows),
		recordWith(t, SideLive, "transactions", lRows),
		&RowSet{Tables: map[string][]Row{"transactions": Rows(hRows)}},
		&RowSet{Tables: map[string][]Row{"transactions": Rows(lRows)}},
	), "transactions")

	if len(got.Samples) != 1 {
		t.Fatalf("got %d samples, want 1: %+v", len(got.Samples), got.Samples)
	}
	if got.Samples[0].Historical != "i:100" || got.Samples[0].Live != "i:999" {
		t.Fatalf("sample = %+v", got.Samples[0])
	}
}

// A fixture in which neither side emits a row proves nothing about the
// mapping: with no rows there are no columns, so two entirely different
// schemas compare equal. Reporting that as a pass is how a table ends up
// marked verified by a fixture that never exercised it.
func TestCompareDistinguishesEmptyFromEqual(t *testing.T) {
	type emptyHist struct{ A string }
	type emptyLive struct{ B int } // deliberately a different schema

	c := Compare(
		recordWith(t, SideHistorical, "trades", []emptyHist{}),
		recordWith(t, SideLive, "trades", []emptyLive{}),
		nil, nil)

	if got := find(t, c, "trades").Verdict; got != VerdictBothEmpty {
		t.Fatalf("verdict %q, want %q", got, VerdictBothEmpty)
	}
}

// Wall-clock columns are the loudest difference in a raw comparison and the
// least informative: both sides stamp time.Now(), so they can never match.
// Detecting them by observation rather than by name matters because the
// names do not predict it — created_at is the ledger close time on
// transactions and an ingestion clock on a dozen other tables.
func TestCompareSkipsColumnsObservedNonDeterministicOnEitherSide(t *testing.T) {
	type row struct {
		Hash      string
		CreatedAt int64 // stands in for a wall clock
	}
	runA := []row{{"a", 1000}}
	runB := []row{{"a", 2000}} // same input, different stamp

	h := NewRecord(SideHistorical, "f", nil)
	if err := h.AddTable("accounts", runA, runB); err != nil {
		t.Fatal(err)
	}
	// The live side runs the same probe and reaches the same conclusion.
	l := NewRecord(SideLive, "f", nil)
	if err := l.AddTable("accounts", []row{{"a", 5000}}, []row{{"a", 6000}}); err != nil {
		t.Fatal(err)
	}

	got := find(t, Compare(h, l, nil, nil), "accounts")
	if got.Verdict != VerdictEqual {
		t.Fatalf("verdict %q, want %q; differing=%v", got.Verdict, VerdictEqual, got.DifferingColumns)
	}
	if len(got.SkippedColumns) != 1 || got.SkippedColumns[0] != "CreatedAt" {
		t.Fatalf("skipped columns %v, want [CreatedAt]", got.SkippedColumns)
	}
	// A skipped column must stay visible. A column nobody compared is not a
	// column that matched.
	for _, c := range got.SharedColumns {
		if c == "CreatedAt" {
			t.Fatal("CreatedAt was reported as a compared shared column")
		}
	}
}

// One side stamping a wall clock where the other does not is still a
// finding, and it must not be reported as the column being absent.
func TestCompareSkipsColumnNonDeterministicOnOneSideOnly(t *testing.T) {
	type row struct {
		Hash      string
		CreatedAt int64
	}
	h := NewRecord(SideHistorical, "f", nil)
	if err := h.AddTable("accounts", []row{{"a", 1000}}, []row{{"a", 2000}}); err != nil {
		t.Fatal(err)
	}
	l := NewRecord(SideLive, "f", nil)
	if err := l.AddTable("accounts", []row{{"a", 7}}, []row{{"a", 7}}); err != nil {
		t.Fatal(err)
	}

	got := find(t, Compare(h, l, nil, nil), "accounts")
	if len(got.HistoricalOnlyColumns) != 0 || len(got.LiveOnlyColumns) != 0 {
		t.Fatalf("a non-deterministic column was reported as one-sided: hist-only %v live-only %v",
			got.HistoricalOnlyColumns, got.LiveOnlyColumns)
	}
	if len(got.SkippedColumns) != 1 || got.SkippedColumns[0] != "CreatedAt" {
		t.Fatalf("skipped columns %v, want [CreatedAt]", got.SkippedColumns)
	}
}

// A row count that moves between two extractions of the same ledger is a
// non-deterministic extractor, not a non-deterministic column. Absorbing it
// into the skip list would hide a far worse problem.
func TestSnapshotRunsDoesNotAbsorbVaryingRowCounts(t *testing.T) {
	type row struct{ Hash string }
	s := SnapshotRuns(Rows([]row{{"a"}, {"b"}}), Rows([]row{{"a"}}))
	if len(s.NonDeterministicColumns) != 0 {
		t.Fatalf("a changing row count was recorded as a non-deterministic column: %v", s.NonDeterministicColumns)
	}
}
