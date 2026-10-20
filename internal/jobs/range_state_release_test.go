package jobs

// Release is the half of the claim lifecycle that was missing.
//
// Claim only takes over a 'freezing' row once DefaultClaimLease has expired,
// so a run stopped before it could freeze left its range blocked from
// redispatch for two hours. Making SIGINT stop a backfill in under a second
// is worth little if the ranges it was working stay locked for the rest of
// the afternoon.
//
// These need a real Postgres because the behaviour IS the SQL -- the two
// predicates on the DELETE are the whole guarantee, and a fake would be
// asserting against a reimplementation of them. `make integration-up` starts
// one; set RANGE_STATE_TEST_POSTGRES to its DSN.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func releaseStore(t *testing.T) *RangeStateStore {
	t.Helper()
	dsn := os.Getenv("RANGE_STATE_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set RANGE_STATE_TEST_POSTGRES to a catalog DSN (see `make integration-up`)")
	}
	// A fresh table per test, dropped afterwards. Reusing one name across
	// runs meant the second run's first Claim found a live claim from the
	// first and returned false -- the tests passed once and then failed
	// forever, which is worse than never having passed.
	ctx := context.Background()
	table := fmt.Sprintf("public.release_test_%s_%d", t.Name(), time.Now().UnixNano())

	store, err := NewRangeStateStore(ctx, dsn, table, "test-era")
	if err != nil {
		t.Fatalf("open range state store: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
		// Dropped on its own connection: the store's pool is closed above,
		// and leaving a table per test behind would accumulate in a catalog
		// a developer reuses across runs.
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Logf("cleanup: %v", err)
			return
		}
		defer pool.Close()
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Logf("cleanup: drop %s: %v", table, err)
		}
	})
	return store
}

// The point of the whole change: a released range is immediately claimable,
// without waiting out the lease.
func TestReleasedRangeIsImmediatelyClaimable(t *testing.T) {
	store := releaseStore(t)
	ctx := context.Background()
	r := LedgerRange{Start: 1, End: 10000}

	if ok, err := store.Claim(ctx, r, "attempt-a", "v1", DefaultClaimLease); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	// Before releasing, a second run must not be able to take it.
	if ok, err := store.Claim(ctx, r, "attempt-b", "v1", DefaultClaimLease); err != nil || ok {
		t.Fatalf("a live claim was stolen: ok=%v err=%v", ok, err)
	}

	released, err := store.Release(ctx, r, "attempt-a")
	if err != nil || !released {
		t.Fatalf("release: released=%v err=%v", released, err)
	}

	if ok, err := store.Claim(ctx, r, "attempt-b", "v1", DefaultClaimLease); err != nil || !ok {
		t.Fatalf("a released range was not claimable: ok=%v err=%v", ok, err)
	}
}

// A frozen range must survive any release attempt. The push landed and the
// rows are in DuckLake; that fact outlives whoever stopped the process.
func TestReleaseCannotUndoAFreeze(t *testing.T) {
	store := releaseStore(t)
	ctx := context.Background()
	r := LedgerRange{Start: 1, End: 10000}

	if ok, err := store.Claim(ctx, r, "attempt-a", "v1", DefaultClaimLease); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if err := store.Freeze(ctx, r, "attempt-a", 42); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	released, err := store.Release(ctx, r, "attempt-a")
	if err != nil {
		t.Fatalf("release returned an error for a frozen range: %v", err)
	}
	if released {
		t.Fatal("a frozen range was released; the push had already landed")
	}

	have, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state := have[r]; state.State != "frozen" {
		t.Errorf("range is %q after a release attempt, want frozen", state.State)
	}
}

// A run may only release its own claim. If our lease expired and somebody
// else took the range, releasing would hand their work to a third run.
func TestReleaseOnlyAffectsOurOwnClaim(t *testing.T) {
	store := releaseStore(t)
	ctx := context.Background()
	r := LedgerRange{Start: 1, End: 10000}

	// A lease short enough to expire inside the test. One second rather
	// than something smaller because Claim passes lease.String() straight
	// into Postgres as an interval, and Postgres rejects Go's sub-second
	// units -- "1ns" is a syntax error there, which this test found the
	// honest way.
	const shortLease = time.Second
	if ok, err := store.Claim(ctx, r, "attempt-a", "v1", shortLease); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	time.Sleep(shortLease + 200*time.Millisecond)
	if ok, err := store.Claim(ctx, r, "attempt-b", "v1", shortLease); err != nil || !ok {
		t.Fatalf("takeover claim: ok=%v err=%v", ok, err)
	}

	released, err := store.Release(ctx, r, "attempt-a")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released {
		t.Fatal("attempt-a released a claim that belongs to attempt-b")
	}

	have, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state := have[r]; state.AttemptID != "attempt-b" {
		t.Errorf("claim is held by %q, want attempt-b", state.AttemptID)
	}
}

// Releasing a range nobody claimed is a no-op, not an error. The caller is a
// deferred cleanup that cannot know whether a freeze landed first.
func TestReleaseOfAnUnclaimedRangeIsHarmless(t *testing.T) {
	store := releaseStore(t)
	released, err := store.Release(context.Background(),
		LedgerRange{Start: 99000001, End: 99010000}, "attempt-a")
	if err != nil {
		t.Errorf("release of an absent range errored: %v", err)
	}
	if released {
		t.Error("release reported having released a range that was never claimed")
	}
}
