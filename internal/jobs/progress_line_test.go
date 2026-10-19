package jobs

// The progress line, held to being one line.
//
// It used to be five Fprintf calls to os.Stderr followed by a Println to
// STDOUT, so the newline that terminated the line landed on the other
// stream. On stderr the lines then ran together, which is visible in the
// mainnet validation logs:
//
//	[Progress] 0 ledgers processed | 0.0 ledgers/sec | elapsed 5s[Progress] 0 ledgers...
//
// Nothing failed, so nothing caught it for as long as the two streams were
// both being read by a human. Writing the whole line at once also stops a
// concurrent chunk interleaving into the middle of it.

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/logging"
)

func trackerFor(buf *bytes.Buffer, level logging.Level) *ProgressTracker {
	return &ProgressTracker{
		totalLedgers:      200,
		processedByWorker: map[int]uint64{0: 120, 1: 80},
		startTime:         time.Now().Add(-30 * time.Second),
		expectedLedgers:   1000,
		log:               logging.New(logging.Options{Level: level, Err: buf}),
	}
}

func TestProgressStatusIsExactlyOneLine(t *testing.T) {
	var buf bytes.Buffer
	trackerFor(&buf, logging.LevelNormal).PrintStatus()

	out := buf.String()
	if out == "" {
		t.Fatal("no progress reported")
	}
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("progress line does not end in a newline, so the next one will run into it: %q", out)
	}
	if got := strings.Count(out, "\n"); got != 1 {
		t.Errorf("progress emitted %d newlines, want 1: %q", got, out)
	}
	if strings.Contains(strings.TrimSuffix(out, "\n"), "\n") {
		t.Errorf("progress line is split across lines: %q", out)
	}
}

// Two consecutive reports must not concatenate, which is the exact symptom
// seen in the validation logs.
func TestConsecutiveProgressLinesDoNotRunTogether(t *testing.T) {
	var buf bytes.Buffer
	tracker := trackerFor(&buf, logging.LevelNormal)
	tracker.PrintStatus()
	tracker.PrintStatus()

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines from two reports, want 2: %q", len(lines), buf.String())
	}
	for i, line := range lines {
		if strings.Count(line, "[Progress]") != 1 {
			t.Errorf("line %d carries %d progress reports, want 1: %q",
				i, strings.Count(line, "[Progress]"), line)
		}
	}
}

func TestProgressCarriesThroughputAndPerWorkerCounts(t *testing.T) {
	var buf bytes.Buffer
	trackerFor(&buf, logging.LevelNormal).PrintStatus()

	out := buf.String()
	for _, want := range []string{"200 ledgers processed", "ledgers/sec", "per-worker:", "w0=120", "w1=80"} {
		if !strings.Contains(out, want) {
			t.Errorf("progress line is missing %q: %q", want, out)
		}
	}
}

// §3: --quiet suppresses progress. Before the line went through the logger
// it wrote to os.Stderr directly and no flag could silence it.
func TestQuietSuppressesProgress(t *testing.T) {
	var buf bytes.Buffer
	trackerFor(&buf, logging.LevelQuiet).PrintStatus()
	if got := buf.String(); got != "" {
		t.Errorf("--quiet still reported progress: %q", got)
	}
}

// A tracker with no logger must not panic: the orchestrator config's Log is
// optional and nil is the documented default.
func TestProgressWithoutALoggerIsSafe(t *testing.T) {
	tracker := &ProgressTracker{
		totalLedgers:      1,
		processedByWorker: map[int]uint64{0: 1},
		startTime:         time.Now().Add(-time.Second),
	}
	tracker.PrintStatus()
}
