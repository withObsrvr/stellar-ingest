package app

// A range is frozen only after the DuckLake push has committed, so by the
// time freeze runs the rows are durable whatever happens next.
//
// Threading the run's own context into Freeze -- which §5 otherwise asks for,
// and which this change did everywhere else -- would mean a SIGTERM arriving
// in that window leaves the range "freezing" with its data already present.
// The next plan then waits out the claim lease and redoes work that was
// finished, which on a multi-day backfill is hours. So this one call gets a
// context that survives cancellation, and these tests hold it to that.

import (
	"context"
	"testing"
	"time"
)

func TestFreezeContextSurvivesParentCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	freezeCtx, cancel := freezeContext(parent)
	defer cancel()

	cancelParent()

	// The parent is cancelled; the freeze window must not be.
	select {
	case <-parent.Done():
	case <-time.After(time.Second):
		t.Fatal("parent did not cancel; the test is broken, not the code")
	}

	select {
	case <-freezeCtx.Done():
		t.Fatal("freeze context cancelled with its parent: a SIGTERM between the push landing " +
			"and the freeze would abandon a completed range and force it to be rebuilt")
	default:
	}
	if err := freezeCtx.Err(); err != nil {
		t.Errorf("freeze context has error %v after parent cancellation, want nil", err)
	}
}

// Uncancellable must not mean unbounded: a hung catalog would otherwise keep
// the process alive after everything else had stopped.
func TestFreezeContextStillHasADeadline(t *testing.T) {
	freezeCtx, cancel := freezeContext(context.Background())
	defer cancel()

	deadline, ok := freezeCtx.Deadline()
	if !ok {
		t.Fatal("freeze context has no deadline; a hung catalog would hold the process open")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > freezeGrace+time.Second {
		t.Errorf("deadline is %s away, want about %s", remaining, freezeGrace)
	}
}

// The returned cancel must work, or the caller's defer leaks a timer.
func TestFreezeContextCancelStops(t *testing.T) {
	freezeCtx, cancel := freezeContext(context.Background())
	cancel()
	select {
	case <-freezeCtx.Done():
	case <-time.After(time.Second):
		t.Error("cancel did not stop the freeze context")
	}
}
