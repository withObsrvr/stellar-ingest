package jobs

// The orchestrator used to build its own root context:
//
//	ctx, cancel := context.WithCancel(context.Background())
//
// so the signal context main.go threads through app.Run died at the call
// site and every worker got a context nothing could cancel. A SIGINT during
// extraction -- the long part of any backfill -- was ignored until the range
// finished. Measured against the live archive before the fix: a run survived
// 60 seconds of SIGINT and had to be killed, which loses the checkpoint and
// leaves the range claim held until its lease expires.
//
// CLI-11 requires the opposite: stop starting new work, finish or roll back
// the current unit, write the checkpoint, exit 130 or 143.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"
)

func cancellationConfig(t *testing.T, root string) domain.OrchestratorConfig {
	t.Helper()
	meta, fixturePath := fixtures.LoadMeta(t, "pubnet-genesis-p1")
	return domain.OrchestratorConfig{
		StartLedger: meta.StartLedger, EndLedger: meta.EndLedger,
		NumWorkers: 2, BatchSize: 2, OutputDir: root,
		NetworkPassphrase: meta.NetworkPassphrase,
		StorageType:       "XDR", Bucket: fixturePath,
	}
}

// A context cancelled before Run is the sharpest form of the bug: with a
// Background context inside, this run would ignore the cancellation and
// extract the whole range.
func TestRunStopsOnACancelledContext(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := NewOrchestrator(cancellationConfig(t, root)).Run(ctx)
	if err == nil {
		t.Fatal("a cancelled context produced a successful run; the caller's cancellation " +
			"is not reaching the workers")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error does not report cancellation, so main cannot exit 130: %v", err)
	}
}

// A stopped run must not leave a manifest behind. The manifest is the claim
// that the attempt is complete, and a resumed backfill reads it as one.
func TestCancelledRunPublishesNoManifest(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	o := NewOrchestrator(cancellationConfig(t, root))
	_ = o.Run(ctx)

	if _, err := os.Stat(o.ManifestPath()); err == nil {
		t.Errorf("a cancelled run published %s; a resumed backfill would read that as "+
			"a completed attempt", o.ManifestPath())
	}
}

// Cancelling mid-flight must be reported as cancellation rather than as
// "N worker(s) failed" -- a scheduler reading the latter retries a stop the
// operator asked for.
func TestMidFlightCancellationIsReportedAsCancellation(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	err := NewOrchestrator(cancellationConfig(t, root)).Run(ctx)
	// The fixture is five ledgers, so the run may legitimately finish first.
	// What must never happen is finishing with a generic failure.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("mid-flight cancellation surfaced as %v, not as a cancellation", err)
	}
	if err == nil {
		t.Skip("run completed before the cancellation landed; timing, not a regression")
	}
	if _, statErr := os.Stat(filepath.Join(root, "manifest.json")); statErr == nil {
		t.Error("a cancelled run published a top-level manifest")
	}
}
