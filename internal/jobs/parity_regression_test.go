package jobs

// The hermetic, credential-free Ticket 04 tier. These tests need no live
// checkout, no database and no network: they run against the committed
// fixtures and the committed digest baselines, which is the tier the ticket
// asks to have in CI.

import (
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/parity"
)

// committedParityDir holds both sides' digest records at the pinned
// revisions. The live records are committed here because this repository is
// where the comparison is reviewed and because the live tree is not available
// to CI; the pins inside each record say which revision produced it.
const committedParityDir = "../../testdata/parity"

// committedParityMatrix is the rendered comparison kept under review.
//
// Two levels up, because the engine moved under internal/ingest while the
// document it is compared against stays in the repository's docs directory.
const committedParityMatrix = "../../docs/ticket-04-parity-matrix.md"

// TestHistoricalParityDigestsMatchCommitted re-extracts every fixture and
// compares against the committed historical baseline.
//
// This is the regression half of the ticket: once a discrepancy has been
// reviewed, a later change that moves the historical side has to show up as a
// diff in a reviewed file rather than silently invalidating the report.
func TestHistoricalParityDigestsMatchCommitted(t *testing.T) {
	if testing.Short() {
		t.Skip("re-extracts every fixture twice; skipped under -short")
	}

	for _, name := range fixtures.Names(t) {
		t.Run(name, func(t *testing.T) {
			var want parity.Record
			path := filepath.Join(committedParityDir, "historical", name+".digest.json")
			if err := ReadJSON(path, &want); err != nil {
				t.Fatalf("read committed baseline: %v", err)
			}

			meta, _ := fixtures.LoadMeta(t, name)
			ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), t.TempDir())
			got, _, err := BuildHistoricalParityRecord(ParityInput{
				Fixture:           name,
				NetworkPassphrase: meta.NetworkPassphrase,
				StartLedger:       meta.StartLedger,
				EndLedger:         meta.EndLedger,
			}, ledgerPath, nil)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}

			compareRecords(t, &want, got)
		})
	}
}

// compareRecords reports the specific table and column that moved. A bare
// "digest changed" tells a reader a regression exists but not where, and
// locating it by hand means re-running the export and diffing megabytes.
func compareRecords(t *testing.T, want, got *parity.Record) {
	t.Helper()

	if want.CanonicalFormVersion != got.CanonicalFormVersion {
		t.Fatalf("canonical form moved from %s to %s; every digest below is expected to change, "+
			"regenerate the baselines with `make parity-baseline-update` and review the diff",
			want.CanonicalFormVersion, got.CanonicalFormVersion)
	}

	for _, table := range parity.AllTables {
		w, g := want.Tables[table], got.Tables[table]
		switch {
		case w.Emitted != g.Emitted:
			t.Errorf("%s: emitted was %v, now %v", table, w.Emitted, g.Emitted)
			continue
		case !w.Emitted:
			continue
		case w.RowCount != g.RowCount:
			t.Errorf("%s: row count was %d, now %d", table, w.RowCount, g.RowCount)
		}

		if diff := columnSetDiff(w.Columns, g.Columns); diff != "" {
			t.Errorf("%s: column set changed: %s", table, diff)
		}
		if diff := columnSetDiff(w.NonDeterministicColumns, g.NonDeterministicColumns); diff != "" {
			// Worth its own message: a column that newly varies between two
			// extractions of one ledger is a reproducibility regression, and
			// it silently removes that column from every comparison.
			t.Errorf("%s: non-deterministic columns changed: %s", table, diff)
		}

		// Columns the probe found non-deterministic keep a digest — dropping
		// it would make them look absent in a cross-side comparison — but
		// that digest changes on every run by definition, so it is not a
		// regression signal. The set of such columns is compared above,
		// which is the part that can actually regress.
		skip := map[string]bool{}
		for _, c := range w.NonDeterministicColumns {
			skip[c] = true
		}
		for _, c := range g.NonDeterministicColumns {
			skip[c] = true
		}

		var moved []string
		for col, d := range w.ColumnDigests {
			if skip[col] {
				continue
			}
			if g.ColumnDigests[col] != d {
				moved = append(moved, col)
			}
		}
		sort.Strings(moved)
		if len(moved) > 0 {
			t.Errorf("%s: values changed in %v", table, moved)
		}
	}
}

func columnSetDiff(want, got []string) string {
	inGot := map[string]bool{}
	for _, c := range got {
		inGot[c] = true
	}
	inWant := map[string]bool{}
	var removed []string
	for _, c := range want {
		inWant[c] = true
		if !inGot[c] {
			removed = append(removed, c)
		}
	}
	var added []string
	for _, c := range got {
		if !inWant[c] {
			added = append(added, c)
		}
	}
	if len(added) == 0 && len(removed) == 0 {
		return ""
	}
	sort.Strings(added)
	sort.Strings(removed)
	var parts []string
	if len(removed) > 0 {
		parts = append(parts, "gone "+strings.Join(removed, ","))
	}
	if len(added) > 0 {
		parts = append(parts, "new "+strings.Join(added, ","))
	}
	return strings.Join(parts, "; ")
}

// TestParityMatrixMatchesCommittedDigests regenerates the reviewed matrix
// from the committed digests.
//
// It is what keeps the prose honest: the matrix in docs/ is the artifact a
// reviewer signs off on, and without this it can drift from the numbers it
// claims to summarize. Hermetic and instant — it reads two directories of
// JSON and renders.
func TestParityMatrixMatchesCommittedDigests(t *testing.T) {
	got := renderCommittedParityMatrix(t)

	want, err := os.ReadFile(committedParityMatrix)
	if err != nil {
		t.Fatalf("read %s: %v", committedParityMatrix, err)
	}
	if string(want) != got {
		t.Errorf("%s is out of date with the committed digests.\n"+
			"Regenerate it with `make parity-matrix` and review the diff.", committedParityMatrix)
	}
}

// TestParityMatrixUpdate rewrites the matrix from the committed digests.
//
//	go test -run TestParityMatrixUpdate -update .
func TestParityMatrixUpdate(t *testing.T) {
	if !*updateGolden {
		t.Skip("pass -update to regenerate the parity matrix")
	}
	if err := os.WriteFile(committedParityMatrix, []byte(renderCommittedParityMatrix(t)), 0o644); err != nil {
		t.Fatalf("write %s: %v", committedParityMatrix, err)
	}
	t.Logf("regenerated %s — review the diff", committedParityMatrix)
}

func renderCommittedParityMatrix(t *testing.T) string {
	t.Helper()

	digests, err := filepath.Glob(filepath.Join(committedParityDir, "historical", "*.digest.json"))
	if err != nil || len(digests) == 0 {
		t.Fatalf("no committed historical digests under %s: %v", committedParityDir, err)
	}
	sort.Strings(digests)

	var b strings.Builder
	b.WriteString("<!-- Generated by `make parity-matrix`; do not edit by hand. -->\n\n")
	b.WriteString("# Ticket 04 — per-fixture Bronze comparison matrix\n\n")
	b.WriteString(parityMatrixPreamble)

	for _, hPath := range digests {
		fixture := strings.TrimSuffix(filepath.Base(hPath), ".digest.json")

		var h, l parity.Record
		if err := ReadJSON(hPath, &h); err != nil {
			t.Fatalf("read %s: %v", hPath, err)
		}
		lPath := filepath.Join(committedParityDir, "live", fixture+".digest.json")
		if err := ReadJSON(lPath, &l); err != nil {
			t.Fatalf("read %s: %v", lPath, err)
		}

		c := parity.Compare(&h, &l, nil, nil)
		if c.FormMismatch != "" {
			t.Fatalf("%s: %s", fixture, c.FormMismatch)
		}
		b.WriteString(RenderParityMarkdown(c))
	}
	return b.String()
}

const parityMatrixPreamble = "" +
	"This file is the machine-generated evidence behind\n" +
	"[`ticket-04-discrepancies.md`](ticket-04-discrepancies.md). Both sides extract the\n" +
	"same committed authoritative XDR and canonicalize it identically; the digests\n" +
	"compared here are committed under `testdata/parity/`.\n\n" +
	"The **verdict** column is mechanical. It is not the ticket's review vocabulary —\n" +
	"deciding whether a difference is provenance-only, an approved semantic\n" +
	"difference or a bug is a reviewer's call, recorded in the discrepancy report.\n" +
	"Two verdicts are easy to misread:\n\n" +
	"- `both-empty` is **not** a pass. A table with no rows on either side has no\n" +
	"  columns to compare, so two entirely different schemas agree there.\n" +
	"- `equal` covers the compared columns only. Columns one side stamps with a\n" +
	"  wall clock are skipped, and this comparison is of extractor output, not of\n" +
	"  what each sink finally persists.\n\n"
