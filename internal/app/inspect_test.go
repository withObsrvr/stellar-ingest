package app

// The lifecycle mapping is the whole of this feature's correctness, so it is
// a pure function and these run without a catalog.
//
// The case that matters is `freezing` with an expired lease. The catalog has
// two words and §4 has five states, and the difference between "a worker
// owns this" and "the run died and this will be redone" is not in the row --
// it is in how old the claim is. Getting it backwards means either waiting
// forever on a dead claim or trampling a live one.

import (
	"testing"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/jobs"
)

func TestClassifyMapsCatalogStateOntoTheLifecycle(t *testing.T) {
	r := jobs.LedgerRange{Start: 64000000, End: 64000999}

	cases := []struct {
		name  string
		state jobs.RangeState
		want  string
	}{
		{
			name:  "absent from the catalog is queued",
			state: jobs.RangeState{},
			want:  LifecycleQueued,
		},
		{
			name:  "frozen is succeeded",
			state: jobs.RangeState{State: jobs.StateFrozen, ClaimedAt: time.Now().Add(-time.Hour), RowsPushed: 10123654},
			want:  LifecycleSucceeded,
		},
		{
			name:  "freezing with a live claim is running",
			state: jobs.RangeState{State: jobs.StateFreezing, ClaimedAt: time.Now().Add(-time.Minute)},
			want:  LifecycleRunning,
		},
		{
			name:  "freezing with an expired lease is failed",
			state: jobs.RangeState{State: jobs.StateFreezing, ClaimedAt: time.Now().Add(-jobs.DefaultClaimLease - time.Minute)},
			want:  LifecycleFailed,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classify(r, c.state)
			if got.Lifecycle != c.want {
				t.Errorf("lifecycle = %q, want %q", got.Lifecycle, c.want)
			}
			if got.Start != r.Start || got.End != r.End {
				t.Errorf("range = %d-%d, want %d-%d", got.Start, got.End, r.Start, r.End)
			}
		})
	}
}

// Exactly at the lease the claim is abandoned, matching how the claim logic
// itself treats the boundary. A disagreement here would have inspect call a
// range running that the next worker is about to take.
func TestClassifyTreatsTheLeaseBoundaryAsAbandoned(t *testing.T) {
	r := jobs.LedgerRange{Start: 1, End: 10}

	justInside := classify(r, jobs.RangeState{
		State:     jobs.StateFreezing,
		ClaimedAt: time.Now().Add(-jobs.DefaultClaimLease + time.Minute),
	})
	if justInside.Lifecycle != LifecycleRunning {
		t.Errorf("a claim one minute short of the lease is %q, want %q",
			justInside.Lifecycle, LifecycleRunning)
	}

	justOutside := classify(r, jobs.RangeState{
		State:     jobs.StateFreezing,
		ClaimedAt: time.Now().Add(-jobs.DefaultClaimLease - time.Minute),
	})
	if justOutside.Lifecycle != LifecycleFailed {
		t.Errorf("a claim one minute past the lease is %q, want %q",
			justOutside.Lifecycle, LifecycleFailed)
	}
}

// The raw catalog word travels alongside the mapped one: somebody debugging
// the catalog needs to see what the table actually holds.
func TestClassifyKeepsTheCatalogsOwnWords(t *testing.T) {
	claimed := time.Now().Add(-time.Minute)
	got := classify(jobs.LedgerRange{Start: 1, End: 10}, jobs.RangeState{
		State:     jobs.StateFreezing,
		AttemptID: "attempt-7",
		Version:   "1e2b97a",
		ClaimedAt: claimed,
	})
	if got.CatalogState != jobs.StateFreezing {
		t.Errorf("catalog_state = %q, want %q", got.CatalogState, jobs.StateFreezing)
	}
	if got.JobID != "attempt-7" {
		t.Errorf("job_id = %q; without it an event stream cannot be joined to the catalog", got.JobID)
	}
	if got.Version != "1e2b97a" {
		t.Errorf("version = %q", got.Version)
	}
	if got.ClaimedAt == nil || !got.ClaimedAt.Equal(claimed.UTC()) {
		t.Errorf("claimed_at = %v, want %v", got.ClaimedAt, claimed.UTC())
	}
}

// Timestamps that are not set must be absent rather than the zero time,
// which JSON renders as year 1 and a reader mistakes for a real date.
func TestClassifyOmitsUnsetTimestamps(t *testing.T) {
	got := classify(jobs.LedgerRange{Start: 1, End: 10}, jobs.RangeState{
		State:     jobs.StateFreezing,
		ClaimedAt: time.Now(),
	})
	if got.CompletedAt != nil {
		t.Errorf("completed_at = %v for an unfinished range, want nil", got.CompletedAt)
	}
}

func TestWaitConditions(t *testing.T) {
	cases := []struct {
		name      string
		result    InspectResult
		condition WaitCondition
		want      bool
	}{
		{"succeeded: all frozen", InspectResult{Ranges: make([]RangeStatus, 3), Succeeded: 3}, WaitSucceeded, true},
		{"succeeded: one still running", InspectResult{Ranges: make([]RangeStatus, 3), Succeeded: 2, Running: 1}, WaitSucceeded, false},
		{"succeeded: one failed", InspectResult{Ranges: make([]RangeStatus, 3), Succeeded: 2, Failed: 1}, WaitSucceeded, false},
		{"settled: nothing in flight", InspectResult{Ranges: make([]RangeStatus, 3), Succeeded: 2, Failed: 1}, WaitSettled, true},
		{"settled: one running", InspectResult{Ranges: make([]RangeStatus, 3), Succeeded: 2, Running: 1}, WaitSettled, false},
		{"settled: one queued", InspectResult{Ranges: make([]RangeStatus, 3), Succeeded: 2, Queued: 1}, WaitSettled, false},
		{"unknown condition never holds", InspectResult{Ranges: make([]RangeStatus, 1), Succeeded: 1}, WaitCondition("whenever"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := satisfied(&c.result, c.condition); got != c.want {
				t.Errorf("satisfied(%s) = %v, want %v", c.condition, got, c.want)
			}
		})
	}
}

// settled and succeeded must differ, or --for is a flag with one meaning.
// A span with a failed range is settled but not succeeded.
func TestSettledAndSucceededAreDifferentQuestions(t *testing.T) {
	withFailure := InspectResult{Ranges: make([]RangeStatus, 2), Succeeded: 1, Failed: 1}
	if !satisfied(&withFailure, WaitSettled) {
		t.Error("a span with nothing in flight is not settled")
	}
	if satisfied(&withFailure, WaitSucceeded) {
		t.Error("a span containing a failed range reported as succeeded; CI would pass on a broken backfill")
	}
}
