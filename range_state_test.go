package main

// planRanges is the whole of Item 5's decision; RangeStateStore around it is
// storage. These cover the decision, because the failure modes are quiet:
// a range wrongly treated as frozen is silently never built, and a range
// wrongly treated as abandoned is built twice by two workers.

import (
	"testing"
	"time"
)

var (
	r1 = LedgerRange{Start: 1, End: 10000}
	r2 = LedgerRange{Start: 10001, End: 20000}
	r3 = LedgerRange{Start: 20001, End: 30000}
)

const testLease = 2 * time.Hour

func names(rs []LedgerRange) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.String()
	}
	return out
}

// A range the catalog has never seen is work to do. This is the whole
// backfill on the first run.
func TestUnknownRangesAreBuilt(t *testing.T) {
	now := time.Now()
	p := planRanges([]LedgerRange{r1, r2, r3}, map[LedgerRange]RangeState{}, testLease, now)

	if len(p.Todo) != 3 {
		t.Fatalf("todo = %v, want all three", names(p.Todo))
	}
	if len(p.Frozen) != 0 || len(p.Claimed) != 0 {
		t.Errorf("nothing should be frozen or claimed: frozen=%v claimed=%v", names(p.Frozen), names(p.Claimed))
	}
}

// The reason the table exists: a resumed run must not redo finished work.
// Getting this wrong turns a 4-day backfill into a restart from zero.
func TestFrozenRangesAreSkipped(t *testing.T) {
	now := time.Now()
	have := map[LedgerRange]RangeState{
		r1: {Range: r1, State: rangeFrozen, CompletedAt: now.Add(-24 * time.Hour)},
		r2: {Range: r2, State: rangeFrozen, CompletedAt: now.Add(-time.Hour)},
	}

	p := planRanges([]LedgerRange{r1, r2, r3}, have, testLease, now)

	if len(p.Todo) != 1 || p.Todo[0] != r3 {
		t.Fatalf("todo = %v, want only %s", names(p.Todo), r3)
	}
	if len(p.Frozen) != 2 {
		t.Errorf("frozen = %v, want both completed ranges", names(p.Frozen))
	}
}

// A claim whose lease has expired means the run holding it died. Re-deriving
// is safe because the DuckLake push deletes the range per table before
// inserting, so a half-finished push is overwritten rather than doubled.
// Without this, a crash leaves the range stuck forever.
func TestAbandonedClaimsAreRebuilt(t *testing.T) {
	now := time.Now()
	have := map[LedgerRange]RangeState{
		r1: {Range: r1, State: rangeFreezing, ClaimedAt: now.Add(-3 * time.Hour), AttemptID: "dead-run"},
	}

	p := planRanges([]LedgerRange{r1}, have, testLease, now)

	if len(p.Todo) != 1 || p.Todo[0] != r1 {
		t.Fatalf("todo = %v, want the abandoned range rebuilt", names(p.Todo))
	}
	if len(p.Claimed) != 0 {
		t.Errorf("an expired claim must not count as held: %v", names(p.Claimed))
	}
}

// A live claim belongs to another worker. Taking it would have two workers
// extracting the same ledgers and racing on the same push.
func TestLiveClaimsAreLeftAlone(t *testing.T) {
	now := time.Now()
	have := map[LedgerRange]RangeState{
		r1: {Range: r1, State: rangeFreezing, ClaimedAt: now.Add(-5 * time.Minute), AttemptID: "worker-a"},
	}

	p := planRanges([]LedgerRange{r1}, have, testLease, now)

	if len(p.Todo) != 0 {
		t.Fatalf("todo = %v, want nothing taken from a live claim", names(p.Todo))
	}
	if len(p.Claimed) != 1 {
		t.Errorf("claimed = %v, want the live claim reported", names(p.Claimed))
	}
}

// The boundary itself. Exactly at the lease the claim is abandoned, so a
// stuck range always eventually becomes reclaimable rather than sitting in
// a state no rule matches.
func TestClaimExpiresAtTheLeaseBoundary(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		age  time.Duration
		todo bool
	}{
		{"just inside the lease", testLease - time.Second, false},
		{"exactly at the lease", testLease, true},
		{"past the lease", testLease + time.Second, true},
	} {
		have := map[LedgerRange]RangeState{
			r1: {Range: r1, State: rangeFreezing, ClaimedAt: now.Add(-tc.age)},
		}
		p := planRanges([]LedgerRange{r1}, have, testLease, now)
		got := len(p.Todo) == 1
		if got != tc.todo {
			t.Errorf("%s: rebuilt=%v, want %v", tc.name, got, tc.todo)
		}
	}
}

// A frozen range stays skipped however old it is. Only `freezing` has a
// lease; applying one to `frozen` would silently rebuild the whole backfill
// after it aged past the threshold.
func TestFrozenRangesHaveNoLease(t *testing.T) {
	now := time.Now()
	have := map[LedgerRange]RangeState{
		r1: {Range: r1, State: rangeFrozen, ClaimedAt: now.Add(-900 * time.Hour), CompletedAt: now.Add(-899 * time.Hour)},
	}

	p := planRanges([]LedgerRange{r1}, have, testLease, now)

	if len(p.Todo) != 0 {
		t.Fatalf("a frozen range was rebuilt because it was old: %v", names(p.Todo))
	}
}

// Catalog rows outside the wanted set must not pull work in. The wanted set
// is the plan; the catalog only says what is already done.
func TestCatalogRowsOutsideTheWantedSetAreIgnored(t *testing.T) {
	now := time.Now()
	have := map[LedgerRange]RangeState{
		r1: {Range: r1, State: rangeFrozen},
		r3: {Range: r3, State: rangeFreezing, ClaimedAt: now.Add(-10 * time.Hour)},
	}

	p := planRanges([]LedgerRange{r1, r2}, have, testLease, now)

	if len(p.Todo) != 1 || p.Todo[0] != r2 {
		t.Fatalf("todo = %v, want only %s", names(p.Todo), r2)
	}
	for _, r := range append(append([]LedgerRange{}, p.Todo...), p.Claimed...) {
		if r == r3 {
			t.Error("r3 is not in the wanted set and must not appear in the plan")
		}
	}
}

func TestEnumerateRangesCoversTheSpanExactly(t *testing.T) {
	got := enumerateRanges(1, 25000, 10000)
	want := []LedgerRange{{1, 10000}, {10001, 20000}, {20001, 25000}}

	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", names(got), names(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("range %d = %s, want %s", i, got[i], want[i])
		}
	}
	// No gaps and no overlaps: every ledger in the span lands in exactly one
	// range. A gap silently drops ledgers from the backfill.
	next := uint32(1)
	for _, r := range got {
		if r.Start != next {
			t.Errorf("gap or overlap at %s: expected start %d", r, next)
		}
		next = r.End + 1
	}
	if next != 25001 {
		t.Errorf("coverage ends at %d, want 25001", next)
	}
}

func TestEnumerateRangesRejectsNonsense(t *testing.T) {
	if got := enumerateRanges(100, 50, 10); got != nil {
		t.Errorf("end before start should yield nothing, got %v", names(got))
	}
	if got := enumerateRanges(1, 100, 0); got != nil {
		t.Errorf("zero size should yield nothing rather than loop forever, got %v", names(got))
	}
}
