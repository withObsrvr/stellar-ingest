package jobs

// Item 3: build partitions concurrently.
//
// The scheduler has to keep three properties the sequential loop had for
// free, and all three are easy to lose. The flusher lost and regained the
// same ones one level down, which is why this mirrors its tests.

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

func chunks(n int) []ledgerChunk {
	out := make([]ledgerChunk, n)
	for i := range out {
		out[i] = ledgerChunk{Start: uint32(i * 10000), End: uint32((i+1)*10000 - 1)}
	}
	return out
}

// A failing chunk must not stop the others. They are independent ledger
// ranges; an errgroup would cancel the siblings and throw away hours of
// completed work to report one failure sooner. With the range catalog the
// failed chunk is simply unclaimed and re-derived next run.
func TestOneFailingChunkDoesNotStopTheRest(t *testing.T) {
	var ran atomic.Int64
	results := runChunksConcurrently(chunks(6), 3, func(c ledgerChunk) (bool, error) {
		ran.Add(1)
		if c.Start == 20000 {
			return false, fmt.Errorf("boom")
		}
		return false, nil
	})

	if got := ran.Load(); got != 6 {
		t.Errorf("ran %d chunks, want all 6 attempted despite the failure", got)
	}
	var failed int
	for _, r := range results {
		if r.Err != nil {
			failed++
		}
	}
	if failed != 1 {
		t.Errorf("failed = %d, want exactly 1", failed)
	}
}

// Results are indexed by chunk, not by completion order, so a report reads
// chronologically however the work interleaved. A map or an append would
// scramble it.
func TestResultsStayInChunkOrder(t *testing.T) {
	in := chunks(5)
	// Finish in reverse: the earliest chunk sleeps longest.
	results := runChunksConcurrently(in, 5, func(c ledgerChunk) (bool, error) {
		time.Sleep(time.Duration(5-c.Start/10000) * 10 * time.Millisecond)
		return false, nil
	})

	for i, r := range results {
		if r.Chunk != in[i] {
			t.Errorf("result %d is chunk %d-%d, want %d-%d",
				i, r.Chunk.Start, r.Chunk.End, in[i].Start, in[i].End)
		}
	}
}

// The bound must actually bind. Memory scales with concurrency times
// --workers times --prefetch-buffer (200 ledgers by default), so a semaphore
// that let extra chunks through would OOM a backfill rather than slow it
// down -- the failure the first validation dispatch hit at 12 GB.
func TestConcurrencyBoundIsRespected(t *testing.T) {
	const limit = 3
	var mu sync.Mutex
	var inFlight, peak int

	runChunksConcurrently(chunks(20), limit, func(ledgerChunk) (bool, error) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()

		time.Sleep(5 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
		return false, nil
	})

	if peak > limit {
		t.Errorf("peak concurrency %d exceeded the limit of %d", peak, limit)
	}
	if peak < 2 {
		t.Errorf("peak concurrency %d suggests the work never actually overlapped", peak)
	}
}

// Concurrency above the chunk count must not spawn idle workers or panic on
// a short backfill.
func TestConcurrencyAboveChunkCountIsClamped(t *testing.T) {
	results := runChunksConcurrently(chunks(2), 50, func(ledgerChunk) (bool, error) { return false, nil })
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("chunk %d: %v", i, r.Err)
		}
	}
}

// Zero or negative must mean sequential rather than deadlocking on a
// zero-capacity semaphore.
func TestNonPositiveConcurrencyRunsSequentially(t *testing.T) {
	for _, n := range []int{0, -1} {
		done := make(chan []chunkResult, 1)
		go func() {
			done <- runChunksConcurrently(chunks(3), n, func(ledgerChunk) (bool, error) { return false, nil })
		}()
		select {
		case got := <-done:
			if len(got) != 3 {
				t.Errorf("concurrency %d: got %d results, want 3", n, len(got))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("concurrency %d deadlocked", n)
		}
	}
}

// A child builds exactly one chunk, so it must inherit neither the chunk
// geometry nor the concurrency. Leaving --chunk-concurrency set handed every
// child a flag with no --chunk-size, which the loader's own guard rejects:
// all five chunks of a real dispatch failed in 62ms.
func TestChunkArgsStripChunkingFlagsFromChildren(t *testing.T) {
	args := []string{
		"--start", "1", "--end", "40000",
		"--chunk-size", "10000", "--chunk-concurrency", "2",
		"--range-state", "--workers", "4", "--output", "/data/out",
	}
	got := rewriteChunkArgs(args, ledgerChunk{Start: 10000, End: 19999}, "/data/out/chunks/10000-19999")

	joined := strings.Join(got, " ")
	// --chunk-concurrency must be gone entirely. --chunk-size is not removed
	// but pinned to 0, which is how the child is told "you are the chunk":
	// inheriting the parent's size would re-chunk recursively.
	if strings.Contains(joined, "--chunk-concurrency") {
		t.Errorf("child inherited --chunk-concurrency; with --chunk-size 0 the loader rejects it "+
			"and every chunk fails in milliseconds: %s", joined)
	}
	if !strings.Contains(joined, "--chunk-size 0") {
		t.Errorf("child should be pinned to --chunk-size 0: %s", joined)
	}
	// What it must keep: its own range, and the flags that make concurrency
	// safe. Dropping --range-state would let two runs take the same chunk.
	for _, required := range []string{"--range-state", "--workers 4", "--start 10000", "--end 19999"} {
		if !strings.Contains(joined, required) {
			t.Errorf("child lost %q: %s", required, joined)
		}
	}
}

// A boundary-aligned chunker emits a single-ledger trailing chunk whenever
// the range ends one past a boundary. That is correct geometry, so the range
// validation has to accept it -- roadmap known gap 2.
func TestSplitEmitsSingleLedgerTrailingChunk(t *testing.T) {
	got := SplitLedgerChunks(64350000, 64354000, 1000)

	last := got[len(got)-1]
	if last.Start != 64354000 || last.End != 64354000 {
		t.Fatalf("last chunk = %d-%d, want the single ledger 64354000", last.Start, last.End)
	}
	if last.Start != last.End {
		t.Fatal("expected a one-ledger chunk; this test exists because the loader used to reject one")
	}
	// Coverage is still exact: no ledger dropped at the seam.
	next := uint32(64350000)
	for _, c := range got {
		if c.Start != next {
			t.Errorf("gap or overlap at %d-%d, expected start %d", c.Start, c.End, next)
		}
		next = c.End + 1
	}
	if next != 64354001 {
		t.Errorf("coverage ends at %d, want 64354001", next)
	}
}

// The range rule the chunker depends on. A single-ledger range is valid;
// only an inverted one is not.
func TestValidateLedgerRangeAcceptsASingleLedger(t *testing.T) {
	if err := domain.ValidateLedgerRange(64354000, 64354000); err != nil {
		t.Errorf("a one-ledger range must be valid -- a boundary-aligned chunker emits them: %v", err)
	}
	if err := domain.ValidateLedgerRange(1, 40000); err != nil {
		t.Errorf("ordinary range rejected: %v", err)
	}
	if err := domain.ValidateLedgerRange(100, 50); err == nil {
		t.Error("an inverted range must still be rejected")
	}
}

// Built and skipped mean opposite things about whether the backfill
// progressed, so the scheduler must keep them apart. A re-dispatch of five
// finished chunks reported "5/5 chunks succeeded in 87ms", which was true
// and read as a triumph.
func TestSkippedChunksAreNotCountedAsBuilt(t *testing.T) {
	results := runChunksConcurrently(chunks(4), 2, func(c ledgerChunk) (bool, error) {
		switch c.Start {
		case 0:
			return true, nil // already complete
		case 10000:
			return false, fmt.Errorf("boom")
		default:
			return false, nil // built
		}
	})

	var built, skipped, failed int
	for _, r := range results {
		switch {
		case r.Err != nil:
			failed++
		case r.Skipped:
			skipped++
		default:
			built++
		}
	}
	if built != 2 || skipped != 1 || failed != 1 {
		t.Errorf("built=%d skipped=%d failed=%d, want 2/1/1", built, skipped, failed)
	}
	// A skipped chunk is not an error: a resumed plan walking finished
	// ranges must not fail the run.
	if results[0].Err != nil {
		t.Errorf("skipped chunk reported an error: %v", results[0].Err)
	}
}
