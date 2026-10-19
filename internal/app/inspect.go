package app

// §4's query and synchronisation primitives: inspect reads current state,
// wait blocks until a condition holds.
//
// "Provide inspect, events --follow, wait --for <condition> --timeout,
// cancel, and resume where the engine can support them." The qualifier is
// the important part, so here is exactly what this engine can support and
// what it cannot.
//
// CAN: inspect and wait, because the range catalog is durable shared state.
// It records, transactionally, that a range was claimed and later frozen,
// with the attempt id, the pipeline version, both timestamps and the row
// count. Two processes can read it, which is what makes a query meaningful
// across a process boundary. Resume already exists and is what --plan does.
//
// CANNOT, honestly: events --follow and cancel across processes. Following a
// live run's event stream from another process needs somewhere to put the
// stream that both can see, and cancelling needs somewhere to put the
// request. Both mean a daemon or a new table, and §2 says "Do not require a
// daemon merely because another product uses one". A run is cancelled with
// SIGINT or SIGTERM, which it now handles; its events are written to the
// stream --events names.
//
// LIFECYCLE. §4's vocabulary is "queued → running → succeeded | failed |
// cancelled". The catalog's two words map onto it:
//
//	not in the catalog          queued     wanted, never started
//	freezing, claim still live  running    a worker owns it
//	freezing, lease expired     failed     the run died; it will be redone
//	frozen                      succeeded  extracted and pushed
//
// cancelled never appears. Nothing records a deliberate stop: a cancelled
// run's claim simply expires and the range is re-derived, which is safe
// because the push deletes before inserting. Saying so is better than
// inventing a state the catalog cannot distinguish.

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/exit"
	"github.com/withObsrvr/stellar-ingest/internal/jobs"
)

// SchemaInspect identifies the inspect-result schema.
const SchemaInspect = "stellar-ingest.inspect/v1"

// Lifecycle states, from §4's vocabulary.
const (
	LifecycleQueued    = "queued"
	LifecycleRunning   = "running"
	LifecycleSucceeded = "succeeded"
	LifecycleFailed    = "failed"
)

// waitPollInterval is how often wait re-reads the catalog. Chosen against
// what it is watching: a chunk takes minutes, so polling faster buys
// nothing and costs a query per second against a production catalog.
const waitPollInterval = 5 * time.Second

// RangeStatus is one range's current state.
type RangeStatus struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
	// Lifecycle is §4's vocabulary, derived from the catalog's own words.
	Lifecycle string `json:"lifecycle"`
	// CatalogState is the raw value -- freezing or frozen -- kept because
	// it is what the table actually holds and a reader debugging the
	// catalog needs to see it.
	CatalogState string     `json:"catalog_state,omitempty"`
	JobID        string     `json:"job_id,omitempty"`
	Version      string     `json:"version,omitempty"`
	ClaimedAt    *time.Time `json:"claimed_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	Rows         int64      `json:"rows,omitempty"`
}

// InspectResult is the answer to "what is the state of this range".
type InspectResult struct {
	Schema string        `json:"schema"`
	EraID  string        `json:"era_id,omitempty"`
	Ranges []RangeStatus `json:"ranges"`
	// Totals let a caller branch without walking the list.
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}

// Inspect reports the lifecycle state of each range in the requested span.
func Inspect(ctx context.Context, req RunRequest, deps Deps) (*InspectResult, error) {
	if req.DlCatalog == "" {
		return nil, exit.Usagef("inspect requires a catalog DSN (DUCKLAKE_CATALOG_DSN)")
	}
	if req.Start == 0 || req.End == 0 || req.End < req.Start {
		return nil, exit.Usagef("inspect requires a valid inclusive --start/--end range")
	}

	store, err := jobs.NewRangeStateStore(ctx, req.DlCatalog, req.RangeStateTable, req.EraID)
	if err != nil {
		return nil, fmt.Errorf("range state: %w", err)
	}
	defer store.Close()

	known, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("range state: %w", err)
	}

	size := uint32(req.ChunkSize)
	if size == 0 {
		size = uint32(req.End) - uint32(req.Start) + 1
	}
	wanted := jobs.EnumerateRanges(uint32(req.Start), uint32(req.End), size)

	result := &InspectResult{Schema: SchemaInspect, EraID: req.EraID}
	for _, r := range wanted {
		result.Ranges = append(result.Ranges, classify(r, known[r]))
	}
	sort.Slice(result.Ranges, func(i, j int) bool { return result.Ranges[i].Start < result.Ranges[j].Start })

	for _, status := range result.Ranges {
		switch status.Lifecycle {
		case LifecycleQueued:
			result.Queued++
		case LifecycleRunning:
			result.Running++
		case LifecycleSucceeded:
			result.Succeeded++
		case LifecycleFailed:
			result.Failed++
		}
	}
	return result, nil
}

// classify maps one catalog row onto §4's lifecycle.
//
// Split out and pure so the mapping -- which is the whole of this feature's
// correctness -- can be tested without a catalog.
func classify(r jobs.LedgerRange, state jobs.RangeState) RangeStatus {
	status := RangeStatus{Start: r.Start, End: r.End}

	if state.State == "" {
		status.Lifecycle = LifecycleQueued
		return status
	}

	status.CatalogState = state.State
	status.JobID = state.AttemptID
	status.Version = state.Version
	status.Rows = state.RowsPushed
	if !state.ClaimedAt.IsZero() {
		claimed := state.ClaimedAt.UTC()
		status.ClaimedAt = &claimed
	}
	if !state.CompletedAt.IsZero() {
		completed := state.CompletedAt.UTC()
		status.CompletedAt = &completed
	}

	switch state.State {
	case jobs.StateFrozen:
		status.Lifecycle = LifecycleSucceeded
	case jobs.StateFreezing:
		// A live claim is a worker still working. An expired one is a run
		// that died: the range is not held by anybody and will be rebuilt,
		// which is a failure of that attempt even though nothing is wrong
		// with the catalog.
		if time.Since(state.ClaimedAt) < jobs.DefaultClaimLease {
			status.Lifecycle = LifecycleRunning
		} else {
			status.Lifecycle = LifecycleFailed
		}
	default:
		status.Lifecycle = LifecycleQueued
	}
	return status
}

// WaitCondition is what wait blocks on.
type WaitCondition string

const (
	// WaitSucceeded holds when every requested range is frozen.
	WaitSucceeded WaitCondition = "succeeded"
	// WaitSettled holds when no requested range is still running: every one
	// has either succeeded or failed. Useful in CI, where "stop when
	// nothing is in flight" is the real question.
	WaitSettled WaitCondition = "settled"
)

// WaitConditions lists what --for accepts.
func WaitConditions() []WaitCondition { return []WaitCondition{WaitSucceeded, WaitSettled} }

// Wait blocks until the condition holds, the context ends, or the deadline
// passes.
//
// The deadline is the context's: --timeout already bounds every operation,
// so a second timeout flag here would be two answers to one question.
func Wait(ctx context.Context, req RunRequest, deps Deps, condition WaitCondition) (*InspectResult, error) {
	switch condition {
	case WaitSucceeded, WaitSettled:
	default:
		return nil, exit.Usagef("--for %q is not a condition; want one of %s",
			condition, formatConditions())
	}

	log := deps.log()
	ticker := time.NewTicker(waitPollInterval)
	defer ticker.Stop()

	for {
		result, err := Inspect(ctx, req, deps)
		if err != nil {
			return nil, err
		}
		if satisfied(result, condition) {
			return result, nil
		}
		log.Info("waiting for %s: %d queued, %d running, %d succeeded, %d failed",
			condition, result.Queued, result.Running, result.Succeeded, result.Failed)

		select {
		case <-ctx.Done():
			// The timeout class, not a generic failure: a caller that set
			// --timeout wants to know the deadline passed rather than that
			// something broke.
			return result, fmt.Errorf("waiting for %s: %w", condition, ctx.Err())
		case <-ticker.C:
		}
	}
}

// satisfied reports whether a condition holds. Pure, so the rule is
// testable without a catalog or a clock.
func satisfied(result *InspectResult, condition WaitCondition) bool {
	switch condition {
	case WaitSucceeded:
		return result.Succeeded == len(result.Ranges)
	case WaitSettled:
		return result.Running == 0 && result.Queued == 0
	default:
		return false
	}
}

func formatConditions() string {
	names := make([]string, 0, len(WaitConditions()))
	for _, c := range WaitConditions() {
		names = append(names, string(c))
	}
	return fmt.Sprintf("%v", names)
}
