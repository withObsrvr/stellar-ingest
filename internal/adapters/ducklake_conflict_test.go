package adapters

// Concurrent chunks write the same DuckLake tables for different ledger
// ranges, and DuckLake detects the concurrent delete at table granularity
// rather than row or partition granularity. The first concurrent dispatch
// lost a chunk to it 70 seconds in:
//
//	Transaction conflict - attempting to insert into table with index "14"
//	- but another transaction has deleted from it
//
// Retrying is safe because the transaction deletes its own range before
// inserting, so it converges rather than doubling. The risk is in the
// classification, not the retry: too broad and a real failure is retried
// five times before surfacing, too narrow and the conflict still kills the
// chunk.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestConflictClassification(t *testing.T) {
	// The exact error from the failed dispatch, wrapped the way Push wraps
	// it -- classification has to survive %w nesting.
	real := fmt.Errorf("commit DuckLake transaction for contract_events_stream_v1: %w",
		errors.New(`TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.
Transaction conflict - attempting to insert into table with index "14" - but another transaction has deleted from it`))

	retryable := []struct {
		name string
		err  error
	}{
		{"the error that actually happened", real},
		{"bare conflict text", errors.New("Transaction conflict - attempting to insert")},
		{"lowercased", errors.New("transaction conflict detected")},
	}
	for _, tc := range retryable {
		if !isDuckLakeCommitConflict(tc.err) {
			t.Errorf("%s: should be retried, was not: %v", tc.name, tc.err)
		}
	}

	// Everything here is permanent. Retrying wastes 15 seconds and reports
	// the same failure, and for the constraint case it could mask a genuine
	// data problem behind a timeout.
	permanent := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"schema mismatch", errors.New(`Binder Error: table "x" does not have a column named "y"`)},
		{"disk full", errors.New("IO Error: No space left on device")},
		{"constraint violation", errors.New("Constraint Error: duplicate key violates primary key constraint")},
		{"the word conflict alone", errors.New("Constraint Error: conflict on unique index")},
		{"connection lost", errors.New("connection reset by peer")},
	}
	for _, tc := range permanent {
		if isDuckLakeCommitConflict(tc.err) {
			t.Errorf("%s: must not be retried: %v", tc.name, tc.err)
		}
	}
}

// "conflict" alone is not enough. A unique-constraint violation contains the
// word and is permanent -- retrying it five times turns an immediate, clear
// failure into a slow one, and on a 6,440-chunk backfill that is hours.
func TestConstraintErrorsAreNotTreatedAsConflicts(t *testing.T) {
	constraint := errors.New("Constraint Error: duplicate key value violates unique constraint, conflict detected")
	if isDuckLakeCommitConflict(constraint) {
		t.Error("a constraint violation containing the word 'conflict' was classified as retryable; " +
			"the match must be on DuckDB's phrase 'transaction conflict', not on 'conflict'")
	}
}

var errConflict = errors.New("Transaction conflict - another transaction has deleted from it")

// A conflict clears as soon as the competing writer commits, so the common
// case is one retry.
func TestRetrySucceedsAfterAConflict(t *testing.T) {
	calls := 0
	err := retryOnCommitConflict(context.Background(), 5, time.Millisecond, "t", func() error {
		calls++
		if calls == 1 {
			return errConflict
		}
		return nil
	})
	if err != nil {
		t.Fatalf("should have succeeded on the retry: %v", err)
	}
	if calls != 2 {
		t.Errorf("attempted %d times, want 2", calls)
	}
}

// A permanent error must surface immediately. Retrying it costs the full
// backoff and reports the same failure -- across 6,440 chunks that is hours
// of waiting to be told something that was knowable at once.
func TestRetryDoesNotRetryPermanentErrors(t *testing.T) {
	permanent := errors.New("Binder Error: table does not have a column named x")
	calls := 0
	err := retryOnCommitConflict(context.Background(), 5, time.Millisecond, "t", func() error {
		calls++
		return permanent
	})
	if calls != 1 {
		t.Errorf("attempted %d times, want 1 -- permanent errors must not be retried", calls)
	}
	if !errors.Is(err, permanent) {
		t.Errorf("the original error must be returned unwrapped-through, got %v", err)
	}
}

// Attempts are bounded. A conflict that never clears means something else is
// wrong, and the range catalog will leave the chunk unclaimed for a later
// run rather than this one blocking forever.
func TestRetryGivesUpAfterTheAttemptLimit(t *testing.T) {
	calls := 0
	err := retryOnCommitConflict(context.Background(), 3, time.Millisecond, "t", func() error {
		calls++
		return errConflict
	})
	if calls != 3 {
		t.Errorf("attempted %d times, want exactly 3", calls)
	}
	if err == nil {
		t.Fatal("persistent conflict must fail")
	}
	if !errors.Is(err, errConflict) {
		t.Errorf("the final error should wrap the conflict: %v", err)
	}
}

// Cancellation during backoff must abort rather than sleep out the schedule.
// A backfill being shut down should stop promptly.
func TestRetryHonoursContextCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- retryOnCommitConflict(ctx, 5, time.Hour, "t", func() error {
			calls++
			cancel() // cancel while the first backoff is pending
			return errConflict
		})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("want context.Canceled, got %v", err)
		}
		if calls != 1 {
			t.Errorf("attempted %d times after cancellation, want 1", calls)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt the backoff")
	}
}
