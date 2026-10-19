package jobs

import (
	"bytes"
	"context"
	"fmt"

	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/logging"
)

type ledgerChunk struct {
	Start uint32
	End   uint32
}

// SplitLedgerChunks aligns chunk boundaries to multiples of chunkSize. This
// keeps --chunk-size 10000 aligned with the loader's range_XXXXXXXX layout,
// including when history starts at ledger 3.
func SplitLedgerChunks(start, end, chunkSize uint32) []ledgerChunk {
	if chunkSize == 0 || start > end {
		return nil
	}
	var chunks []ledgerChunk
	for current := start; ; {
		boundary := (uint64(current)/uint64(chunkSize) + 1) * uint64(chunkSize)
		chunkEnd64 := boundary - 1
		if chunkEnd64 > uint64(end) {
			chunkEnd64 = uint64(end)
		}
		chunkEnd := uint32(chunkEnd64)
		chunks = append(chunks, ledgerChunk{Start: current, End: chunkEnd})
		if chunkEnd == end {
			break
		}
		current = chunkEnd + 1
	}
	return chunks
}

// chunkResult is one chunk's outcome. Results are returned in chunk order
// regardless of completion order, so a report reads chronologically.
type chunkResult struct {
	Chunk    ledgerChunk
	Err      error
	Duration time.Duration
	// Skipped means the chunk was already complete, not that it built.
	Skipped bool
}

// prefixWriter tags each line with its chunk so concurrent children remain
// attributable. Buffering per chunk and printing on completion would be
// simpler, but a chunk can run for an hour and silence for an hour is not
// what you want from a multi-day backfill.
type prefixWriter struct {
	prefix string
	out    *os.File
	mu     *sync.Mutex
	buf    []byte
	// saw is set when the child reports it did no work. Read only after
	// command.Run returns, so the mutex covering writes is sufficient.
	saw *atomic.Bool
}

func (w *prefixWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := w.buf[:i]
		if w.saw != nil && bytes.Contains(line, []byte(RangeSkippedMarker)) {
			w.saw.Store(true)
		}
		fmt.Fprintf(w.out, "%s %s\n", w.prefix, line)
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

// runChunksConcurrently runs chunks under a semaphore.
//
// No errgroup, deliberately: a failing chunk must not cancel its siblings.
// They are independent ledger ranges, and a chunk that fails is re-derived
// later from the range catalog -- cancelling the others would throw away
// hours of completed work to report one failure sooner. The flusher learned
// this the same way.
func runChunksConcurrently(chunks []ledgerChunk, concurrency int, run func(ledgerChunk) (bool, error)) []chunkResult {
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(chunks) {
		concurrency = len(chunks)
	}

	results := make([]chunkResult, len(chunks))
	slots := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk ledgerChunk) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()

			started := time.Now()
			skipped, err := run(chunk)
			results[i] = chunkResult{Chunk: chunk, Err: err, Duration: time.Since(started), Skipped: skipped}
		}(i, chunk)
	}
	wg.Wait()
	return results
}

func RunChunkedCommand(ctx context.Context, originalArgs []string, start, end, chunkSize uint32, outputDir string, concurrency int, log *logging.Logger) error {
	chunks := SplitLedgerChunks(start, end, chunkSize)
	if len(chunks) == 0 {
		return fmt.Errorf("invalid chunk range %d-%d with size %d", start, end, chunkSize)
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}

	started := time.Now()
	log.Section("Chunked Backfill")
	log.Field("Range", "%d - %d", start, end)
	log.Field("Chunk size", "%d", chunkSize)
	log.Field("Chunks", "%d", len(chunks))
	log.Field("Concurrency", "%d", concurrency)
	log.Line()

	// One mutex across every child so a line is never torn by another
	// chunk's write.
	var outMu sync.Mutex
	var doneCount atomic.Int64

	results := runChunksConcurrently(chunks, concurrency, func(chunk ledgerChunk) (bool, error) {
		label := fmt.Sprintf("[%d-%d]", chunk.Start, chunk.End)
		chunkOutput := filepath.Join(outputDir, "chunks", fmt.Sprintf("%d-%d", chunk.Start, chunk.End))
		args := rewriteChunkArgs(originalArgs, chunk, chunkOutput)

		var saw atomic.Bool
		command := exec.CommandContext(ctx, executable, args...)
		command.Stdout = &prefixWriter{prefix: label, out: os.Stdout, mu: &outMu, saw: &saw}
		command.Stderr = &prefixWriter{prefix: label, out: os.Stderr, mu: &outMu, saw: &saw}
		command.Env = os.Environ()

		err := command.Run()
		skipped := err == nil && saw.Load()

		outMu.Lock()
		n := doneCount.Add(1)
		switch {
		case err != nil:
			log.Error("%s FAILED (%d/%d done): %v", label, n, len(chunks), err)
		case skipped:
			log.Info("%s skipped, already complete (%d/%d done)", label, n, len(chunks))
		default:
			log.Info("%s built (%d/%d done)", label, n, len(chunks))
		}
		outMu.Unlock()
		return skipped, err
	})

	var failed []chunkResult
	var built, skipped int
	for _, r := range results {
		switch {
		case r.Err != nil:
			failed = append(failed, r)
		case r.Skipped:
			skipped++
		default:
			built++
		}
	}

	// Built and skipped are reported apart because they mean opposite things
	// about whether the backfill progressed. "5/5 succeeded in 87ms" was all
	// five chunks skipping, which read as a triumph.
	log.Line()
	log.Info("Chunked backfill: %d built, %d skipped (already complete), %d failed, in %s",
		built, skipped, len(failed), time.Since(started).Round(time.Millisecond))

	if len(failed) > 0 {
		// Every failure is listed rather than just the first. With the range
		// catalog these chunks are simply unclaimed and re-derived on the
		// next run, so the useful output is the whole set, not the earliest.
		for _, r := range failed {
			log.Error("  failed: %d-%d after %s: %v",
				r.Chunk.Start, r.Chunk.End, r.Duration.Round(time.Millisecond), r.Err)
		}
		return fmt.Errorf("%d of %d chunks failed", len(failed), len(chunks))
	}
	return nil
}

func rewriteChunkArgs(args []string, chunk ledgerChunk, outputDir string) []string {
	// chunk-concurrency is stripped alongside chunk-size: a child builds one
	// chunk and must not re-chunk or re-parallelise. Leaving it set gave every
	// child "--chunk-concurrency 2" with no --chunk-size, which the loader's
	// own guard rejects -- all five chunks failed in 62ms.
	// Both spellings of the directory flag are stripped. A child that
	// inherited the parent's --output-dir would write every chunk into the
	// same place and the per-chunk manifests would overwrite each other;
	// --output is stripped too because a path there still resolves to the
	// directory for one more release.
	remove := map[string]bool{"start": true, "end": true,
		"output": true, "output-dir": true,
		"chunk-size": true, "chunk-concurrency": true}
	result := make([]string, 0, len(args)+4)
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if !strings.HasPrefix(arg, "--") {
			result = append(result, arg)
			continue
		}
		name := strings.TrimPrefix(arg, "--")
		if equals := strings.IndexByte(name, '='); equals >= 0 {
			name = name[:equals]
			if remove[name] {
				continue
			}
			result = append(result, arg)
			continue
		}
		if remove[name] {
			if index+1 < len(args) && !strings.HasPrefix(args[index+1], "--") {
				index++
			}
			continue
		}
		result = append(result, arg)
	}
	return append(result,
		"--start", strconv.FormatUint(uint64(chunk.Start), 10),
		"--end", strconv.FormatUint(uint64(chunk.End), 10),
		"--output-dir", outputDir,
		"--chunk-size", "0",
	)
}
