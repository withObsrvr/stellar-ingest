package main

// Item 3: build partitions concurrently.
//
// The scheduler has to keep three properties the sequential loop had for
// free, and all three are easy to lose. The flusher lost and regained the
// same ones one level down, which is why this mirrors its tests.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	results := runChunksConcurrently(chunks(6), 3, func(c ledgerChunk) error {
		ran.Add(1)
		if c.Start == 20000 {
			return fmt.Errorf("boom")
		}
		return nil
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
	results := runChunksConcurrently(in, 5, func(c ledgerChunk) error {
		time.Sleep(time.Duration(5-c.Start/10000) * 10 * time.Millisecond)
		return nil
	})

	for i, r := range results {
		if r.Chunk != in[i] {
			t.Errorf("result %d is chunk %d-%d, want %d-%d",
				i, r.Chunk.Start, r.Chunk.End, in[i].Start, in[i].End)
		}
	}
}

// The bound must actually bind. Memory scales with concurrency times
// --workers times the 200-ledger prefetch buffer, so a semaphore that let
// extra chunks through would OOM a backfill rather than slow it down -- the
// failure the first validation dispatch hit at 12 GB.
func TestConcurrencyBoundIsRespected(t *testing.T) {
	const limit = 3
	var mu sync.Mutex
	var inFlight, peak int

	runChunksConcurrently(chunks(20), limit, func(ledgerChunk) error {
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
		return nil
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
	results := runChunksConcurrently(chunks(2), 50, func(ledgerChunk) error { return nil })
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
		go func() { done <- runChunksConcurrently(chunks(3), n, func(ledgerChunk) error { return nil }) }()
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
