package parity

import (
	"sort"
	"strconv"
)

// Verdict is the harness's mechanical reading of one table.
//
// It is deliberately not the ticket's review vocabulary (equal /
// provenance-only / approved semantic difference / bug / unverified). The
// harness can observe that two columns hold different values; only a reviewer
// can decide whether that is a bug or an intended difference. Mapping one to
// the other is the discrepancy report's job.
type Verdict string

const (
	VerdictBothAbsent Verdict = "both-absent"

	// VerdictBothEmpty means both sides emitted the table with zero rows.
	//
	// It is kept apart from VerdictEqual because an empty table proves
	// nothing about the mapping: with no rows there are no columns to
	// compare, so two implementations with completely different schemas
	// still agree here. Reading it as a pass is how a table ends up
	// "verified" by a fixture that never exercised it.
	VerdictBothEmpty Verdict = "both-empty"

	VerdictEqual                Verdict = "equal"
	VerdictEqualOnSharedColumns Verdict = "equal-on-shared-columns"
	VerdictValuesDiffer         Verdict = "values-differ"
	VerdictRowCountDiffers      Verdict = "row-count-differs"
	VerdictOneSideAbsent        Verdict = "one-side-absent"
)

// severity orders verdicts worst-first so a report can rank tables by how
// much attention they need.
var severity = map[Verdict]int{
	VerdictOneSideAbsent:        5,
	VerdictRowCountDiffers:      4,
	VerdictValuesDiffer:         3,
	VerdictEqualOnSharedColumns: 2,
	VerdictEqual:                1,
	VerdictBothEmpty:            1,
	VerdictBothAbsent:           0,
}

// Severity returns the rank used to sort tables worst-first.
func (v Verdict) Severity() int { return severity[v] }

// TableComparison is the per-table result for one fixture.
type TableComparison struct {
	Table   string  `json:"table"`
	Verdict Verdict `json:"verdict"`

	HistoricalEmitted bool `json:"historical_emitted"`
	LiveEmitted       bool `json:"live_emitted"`

	HistoricalRowCount int `json:"historical_row_count"`
	LiveRowCount       int `json:"live_row_count"`

	SharedColumns         []string `json:"shared_columns"`
	HistoricalOnlyColumns []string `json:"historical_only_columns,omitempty"`
	LiveOnlyColumns       []string `json:"live_only_columns,omitempty"`

	// DifferingColumns are shared columns whose value multisets differ. This
	// is the field that turns "this table does not match" into a list of
	// specific mappings to investigate.
	DifferingColumns []string `json:"differing_columns,omitempty"`

	// SkippedColumns are shared columns left uncompared because at least one
	// side was observed to produce a different value for them on a repeat
	// extraction of the same ledger.
	//
	// They are listed rather than quietly dropped. A column nobody compared
	// is not a column that matched, and on this ticket several of them —
	// every table whose created_at is an ingestion wall clock — are a
	// reproducibility finding in their own right.
	SkippedColumns []string `json:"skipped_columns,omitempty"`

	// RowsOnlyHistorical and RowsOnlyLive are multiset differences computed
	// over the shared columns only, so a side's extra columns do not make
	// every row look unmatched. Populated only when both RowSets are
	// supplied; nil otherwise.
	RowsOnlyHistorical *int `json:"rows_only_historical,omitempty"`
	RowsOnlyLive       *int `json:"rows_only_live,omitempty"`

	// Samples are concrete differing values for differing columns, bounded so
	// the report stays readable. Populated only when both RowSets are
	// supplied.
	Samples []ValueSample `json:"samples,omitempty"`
}

// ValueSample is one concrete difference, truncated for readability.
type ValueSample struct {
	Column     string `json:"column"`
	Historical string `json:"historical"`
	Live       string `json:"live"`
	// Note explains a sample that is not a plain value-for-value pair, such
	// as one side having no corresponding row at that position.
	Note string `json:"note,omitempty"`
}

// FixtureComparison is the full result for one fixture.
type FixtureComparison struct {
	Fixture         string            `json:"fixture"`
	HistoricalPins  map[string]string `json:"historical_pins"`
	LivePins        map[string]string `json:"live_pins"`
	ExcludedColumns []string          `json:"excluded_columns"`

	// FormMismatch is set when the two sides canonicalized with different
	// rules. Every digest below is then meaningless, and the comparison must
	// be rerun rather than read.
	FormMismatch string `json:"form_mismatch,omitempty"`

	Tables []TableComparison `json:"tables"`
}

// Compare produces the per-table comparison for one fixture.
//
// rowsHistorical and rowsLive are optional; pass nil to get the digest-level
// comparison alone, which is the credential-free tier that runs in CI.
func Compare(h, l *Record, rowsHistorical, rowsLive *RowSet) FixtureComparison {
	out := FixtureComparison{
		Fixture:         h.Fixture,
		HistoricalPins:  h.Pins,
		LivePins:        l.Pins,
		ExcludedColumns: h.ExcludedColumns,
	}
	if h.CanonicalFormVersion != l.CanonicalFormVersion {
		out.FormMismatch = "historical canonical form " + h.CanonicalFormVersion +
			" != live canonical form " + l.CanonicalFormVersion
	}

	for _, table := range AllTables {
		hs := h.Tables[table]
		ls := l.Tables[table]
		tc := compareTable(table, hs, ls)

		if rowsHistorical != nil && rowsLive != nil && len(tc.SharedColumns) > 0 {
			annotateWithRows(&tc, rowsHistorical.Tables[table], rowsLive.Tables[table])
		}
		out.Tables = append(out.Tables, tc)
	}

	sort.SliceStable(out.Tables, func(i, j int) bool {
		si, sj := out.Tables[i].Verdict.Severity(), out.Tables[j].Verdict.Severity()
		if si != sj {
			return si > sj
		}
		return out.Tables[i].Table < out.Tables[j].Table
	})
	return out
}

func compareTable(table string, hs, ls TableSnapshot) TableComparison {
	tc := TableComparison{
		Table:              table,
		HistoricalEmitted:  hs.Emitted,
		LiveEmitted:        ls.Emitted,
		HistoricalRowCount: hs.RowCount,
		LiveRowCount:       ls.RowCount,
	}

	switch {
	case !hs.Emitted && !ls.Emitted:
		tc.Verdict = VerdictBothAbsent
		return tc
	case hs.Emitted != ls.Emitted:
		tc.Verdict = VerdictOneSideAbsent
		// Still report the present side's columns so the reader can see what
		// the other side would have to produce.
		tc.SharedColumns = nil
		if hs.Emitted {
			tc.HistoricalOnlyColumns = comparedColumns(hs)
		} else {
			tc.LiveOnlyColumns = comparedColumns(ls)
		}
		return tc
	}

	hCols := comparedColumns(hs)
	lCols := comparedColumns(ls)
	shared, histOnly, liveOnly := partition(hCols, lCols)
	tc.HistoricalOnlyColumns, tc.LiveOnlyColumns = histOnly, liveOnly

	// A column is uncomparable if either side was observed to produce a
	// different value for it on a repeat extraction of the same ledger.
	// Comparing one anyway guarantees a difference and says nothing.
	skip := map[string]bool{}
	for _, c := range hs.NonDeterministicColumns {
		skip[c] = true
	}
	for _, c := range ls.NonDeterministicColumns {
		skip[c] = true
	}

	for _, c := range shared {
		switch {
		case skip[c]:
			tc.SkippedColumns = append(tc.SkippedColumns, c)
		case hs.ColumnDigests[c] != ls.ColumnDigests[c]:
			tc.SharedColumns = append(tc.SharedColumns, c)
			tc.DifferingColumns = append(tc.DifferingColumns, c)
		default:
			tc.SharedColumns = append(tc.SharedColumns, c)
		}
	}

	switch {
	case hs.RowCount == 0 && ls.RowCount == 0:
		tc.Verdict = VerdictBothEmpty
	case hs.RowCount != ls.RowCount:
		tc.Verdict = VerdictRowCountDiffers
	case len(tc.DifferingColumns) > 0:
		tc.Verdict = VerdictValuesDiffer
	case len(tc.HistoricalOnlyColumns) > 0 || len(tc.LiveOnlyColumns) > 0:
		tc.Verdict = VerdictEqualOnSharedColumns
	default:
		tc.Verdict = VerdictEqual
	}
	return tc
}

// comparedColumns returns the columns that carry a digest, which is the
// column list minus the declared run-dependent exclusions. Columns observed
// to be non-deterministic still carry a digest and are filtered later, so
// that a column one side stamps with a wall clock is not mistaken for a
// column the other side does not have.
func comparedColumns(s TableSnapshot) []string {
	cols := make([]string, 0, len(s.ColumnDigests))
	for c := range s.ColumnDigests {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

func partition(a, b []string) (both, onlyA, onlyB []string) {
	inB := make(map[string]bool, len(b))
	for _, x := range b {
		inB[x] = true
	}
	inA := make(map[string]bool, len(a))
	for _, x := range a {
		inA[x] = true
		if inB[x] {
			both = append(both, x)
		} else {
			onlyA = append(onlyA, x)
		}
	}
	for _, x := range b {
		if !inA[x] {
			onlyB = append(onlyB, x)
		}
	}
	sort.Strings(both)
	sort.Strings(onlyA)
	sort.Strings(onlyB)
	return both, onlyA, onlyB
}

// maxSamplesPerTable bounds the drill-down. A busy ledger can differ in every
// row of a table; the first few differences identify the mapping, and the
// rest belong in the machine-readable artifact, not the report.
const maxSamplesPerTable = 5

// annotateWithRows adds the shared-column row multiset diff and concrete
// value samples.
func annotateWithRows(tc *TableComparison, hRows, lRows []Row) {
	hDigests := map[string]int{}
	for _, r := range hRows {
		hDigests[Digest(EncodeRow(r, tc.SharedColumns))]++
	}
	lDigests := map[string]int{}
	for _, r := range lRows {
		lDigests[Digest(EncodeRow(r, tc.SharedColumns))]++
	}

	onlyH, onlyL := 0, 0
	for d, n := range hDigests {
		if m := lDigests[d]; n > m {
			onlyH += n - m
		}
	}
	for d, n := range lDigests {
		if m := hDigests[d]; n > m {
			onlyL += n - m
		}
	}
	tc.RowsOnlyHistorical = &onlyH
	tc.RowsOnlyLive = &onlyL

	// Samples pair rows positionally. The two sides extract in different
	// orders, so a positional pair is a hint about which values differ, not a
	// claim that these two rows are the same logical row — per-table logical
	// keys are part of the canonical row contract still being decided, and
	// guessing one here would produce confident, wrong pairings.
	for _, col := range tc.DifferingColumns {
		if len(tc.Samples) >= maxSamplesPerTable {
			break
		}
		hv := sortedColumnValues(hRows, col)
		lv := sortedColumnValues(lRows, col)
		for i := 0; i < len(hv) || i < len(lv); i++ {
			if len(tc.Samples) >= maxSamplesPerTable {
				break
			}
			switch {
			case i >= len(hv):
				tc.Samples = append(tc.Samples, ValueSample{
					Column: col, Live: truncate(lv[i]), Note: "no historical value at this rank",
				})
			case i >= len(lv):
				tc.Samples = append(tc.Samples, ValueSample{
					Column: col, Historical: truncate(hv[i]), Note: "no live value at this rank",
				})
			case hv[i] != lv[i]:
				tc.Samples = append(tc.Samples, ValueSample{
					Column: col, Historical: truncate(hv[i]), Live: truncate(lv[i]),
					Note: "values compared by sorted rank, not by logical row key",
				})
			default:
				continue
			}
			break
		}
	}
}

func sortedColumnValues(rows []Row, col string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		v, ok := r[col]
		if !ok {
			v = tagNull
		}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// maxSampleValueBytes keeps a base64 transaction envelope from filling the
// report. The digest, not the sample, is the evidence; the sample only has to
// show what kind of difference it is.
const maxSampleValueBytes = 160

func truncate(s string) string {
	if len(s) <= maxSampleValueBytes {
		return s
	}
	return s[:maxSampleValueBytes] + "…(" + strconv.Itoa(len(s)) + " bytes total)"
}
