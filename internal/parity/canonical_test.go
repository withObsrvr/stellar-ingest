package parity

import (
	"strings"
	"testing"
	"time"
)

// The distinctions in this test are the ones Ticket 04 says must survive
// canonicalization. Each case is a difference that a naive encoder collapses
// and that would then be reported as parity.
func TestEncodeKeepsDistinctionsThatMatter(t *testing.T) {
	var nilStr *string
	empty := ""
	space := " "
	zero := int64(0)

	cases := []struct {
		name string
		a, b any
	}{
		{"nil pointer versus empty string", nilStr, &empty},
		{"empty string versus single space", &empty, &space},
		{"nil pointer versus zero int", (*int64)(nil), &zero},
		{"nil slice versus empty slice", []string(nil), []string{}},
		{"empty slice versus slice of empty string", []string{}, []string{""}},
		{"false versus nil bool", false, (*bool)(nil)},
		{"int 1 versus string \"1\"", int64(1), "1"},
		{"zero time versus unix epoch", time.Time{}, time.Unix(0, 0).UTC()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := Encode(tc.a), Encode(tc.b); got == want {
				t.Fatalf("values encode identically as %q, so the difference would be invisible", got)
			}
		})
	}
}

// int64 is the width of every TOID and stroop amount in Bronze. Routing the
// canonical form through a float64 — which is what encoding/json does —
// silently rounds values above 2^53, so two different ledger positions would
// compare equal.
func TestEncodePreservesInt64Precision(t *testing.T) {
	const a int64 = 9007199254740993 // 2^53 + 1
	const b int64 = 9007199254740992 // 2^53

	if Encode(a) == Encode(b) {
		t.Fatalf("2^53 and 2^53+1 both encode as %q", Encode(a))
	}
	if got, want := Encode(a), "i:9007199254740993"; got != want {
		t.Fatalf("Encode(%d) = %q, want %q", a, got, want)
	}
}

// A ledger close time read back in a different location, or carrying a
// monotonic reading, is the same instant. Treating it as a difference would
// make every table fail for a reason that has nothing to do with the ledger.
func TestEncodeNormalizesTimeZoneButNotInstant(t *testing.T) {
	utc := time.Date(2026, 10, 14, 12, 0, 0, 123456789, time.UTC)
	elsewhere := utc.In(time.FixedZone("UTC+7", 7*3600))

	if Encode(utc) != Encode(elsewhere) {
		t.Fatalf("same instant in two zones encoded differently:\n  %s\n  %s", Encode(utc), Encode(elsewhere))
	}
	if Encode(utc) == Encode(utc.Add(time.Nanosecond)) {
		t.Fatal("one nanosecond apart encoded identically; sub-second precision was dropped")
	}
}

// Without escaping, a string containing the composite delimiters can forge a
// field boundary, so two different rows produce one encoding.
func TestEncodeEscapesDelimiters(t *testing.T) {
	a := []string{"x,y"}
	b := []string{"x", "y"}
	if Encode(a) == Encode(b) {
		t.Fatalf("%q and %q collided at %q", a, b, Encode(a))
	}
	if !strings.Contains(Encode(a), `x\,y`) {
		t.Fatalf("comma not escaped in %q", Encode(a))
	}
}

// Ordered arrays are real Bronze content — call-graph hops, auth credential
// lists, claimants — where a reordering is a discrepancy, not noise.
func TestEncodePreservesListOrder(t *testing.T) {
	if Encode([]string{"a", "b"}) == Encode([]string{"b", "a"}) {
		t.Fatal("list order was normalized away")
	}
}

// Map iteration order in Go is randomized, so an unsorted map encoding would
// make a table's digest differ between two runs of the same code.
func TestEncodeMapIsStable(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6}
	first := Encode(m)
	for i := 0; i < 50; i++ {
		if got := Encode(m); got != first {
			t.Fatalf("map encoding is unstable:\n  %s\n  %s", first, got)
		}
	}
}

type sampleRow struct {
	LedgerSequence uint32
	Hash           string
	Memo           *string
	unexported     string
}

func TestRowOfFlattensExportedFieldsOnly(t *testing.T) {
	row := RowOf(sampleRow{LedgerSequence: 7, Hash: "abc", unexported: "hidden"})

	if _, ok := row["unexported"]; ok {
		t.Fatal("unexported field leaked into the row")
	}
	if got, want := row["LedgerSequence"], "u:7"; got != want {
		t.Fatalf("LedgerSequence = %q, want %q", got, want)
	}
	if got, want := row["Memo"], tagNull; got != want {
		t.Fatalf("nil Memo = %q, want %q", got, want)
	}
}

// Both sides must be able to run their 21 extractors in any order. Row order
// is not part of the contract under comparison; row content is.
func TestDigestMultisetIgnoresOrderButNotDuplicates(t *testing.T) {
	ab := DigestMultiset([]string{"a", "b"})
	ba := DigestMultiset([]string{"b", "a"})
	if ab != ba {
		t.Fatal("multiset digest depends on input order")
	}
	if ab == DigestMultiset([]string{"a", "a", "b"}) {
		t.Fatal("a duplicated row produced the same digest as a single row")
	}
}

// Length-prefixing is what stops two different splits of the same bytes from
// hashing alike. Without it {"ab"} and {"a","b"} collide.
func TestDigestMultisetResistsConcatenationCollision(t *testing.T) {
	if DigestMultiset([]string{"ab"}) == DigestMultiset([]string{"a", "b"}) {
		t.Fatal(`{"ab"} and {"a","b"} collided`)
	}
}

func TestSnapshotExcludesRunDependentColumnsButListsThem(t *testing.T) {
	era := "era-1"
	type row struct {
		Hash         string
		EraID        *string
		VersionLabel string
	}
	a := Snapshot(Rows([]row{{Hash: "h", EraID: &era, VersionLabel: "historical"}}))
	b := Snapshot(Rows([]row{{Hash: "h", EraID: nil, VersionLabel: "live"}}))

	if a.TableDigest != b.TableDigest {
		t.Fatal("run-dependent columns leaked into the table digest")
	}
	if _, compared := a.ColumnDigests["EraID"]; compared {
		t.Fatal("EraID was given a digest despite being run-dependent")
	}
	// The reader still has to be able to see the field exists.
	var sawEra bool
	for _, c := range a.Columns {
		if c == "EraID" {
			sawEra = true
		}
	}
	if !sawEra {
		t.Fatalf("EraID missing from the column list: %v", a.Columns)
	}
}

func TestSnapshotDistinguishesAbsentFromEmpty(t *testing.T) {
	type row struct{ Hash string }
	emptyTable := Snapshot(Rows([]row{}))
	if !emptyTable.Emitted {
		t.Fatal("a table emitted with zero rows must not report as absent")
	}

	rec := NewRecord(SideHistorical, "f", nil)
	if rec.Tables["trades"].Emitted {
		t.Fatal("a table never registered must report as absent")
	}
}

func TestAddTableRejectsUnknownTable(t *testing.T) {
	rec := NewRecord(SideLive, "f", nil)
	type row struct{ A int }
	if err := rec.AddTable("not_a_bronze_table", []row{}); err == nil {
		t.Fatal("expected an error for a table outside the Ticket 04 set")
	}
	if len(AllTables) != 21 {
		t.Fatalf("Ticket 04 covers 21 tables, AllTables has %d", len(AllTables))
	}
}
