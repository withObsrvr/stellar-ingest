package app

// What a flowctl control plane actually sees when stellar-ingest runs.
//
// These drive the real app.Run against the stub in flowctl_stub_test.go and
// a committed pubnet fixture -- no network, no credentials, no catalog.

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/exit"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"

	flowctlpb "github.com/withobsrvr/flowctl/proto"
)

// flowctlRequest is a run small enough to be hermetic and real enough to
// report every phase an extraction goes through.
func flowctlRequest(t *testing.T) RunRequest {
	t.Helper()
	meta, path := fixtures.LoadMeta(t, "pubnet-genesis-p1")
	return RunRequest{
		Start:             uint(meta.StartLedger),
		End:               uint(meta.EndLedger),
		Workers:           1,
		BatchSize:         1,
		ChunkConcurrency:  1,
		PrefetchBuffer:    1,
		OutputDir:         t.TempDir(),
		StorageType:       "XDR",
		Bucket:            path,
		NetworkPassphrase: meta.NetworkPassphrase,
	}
}

// The component must appear to the control plane as a SOURCE, carrying
// enough metadata to tell two concurrent backfills apart. A watcher that
// cannot see the ledger range cannot tell which chunk is which.
func TestRegistersAsASourceWithItsRange(t *testing.T) {
	stub := &stubControlPlane{}
	enableFlowctl(t, startStubControlPlane(t, stub))

	if _, err := Run(context.Background(), flowctlRequest(t), Deps{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	registered, _ := stub.snapshot()
	if len(registered) != 1 {
		t.Fatalf("registered %d times, want exactly 1", len(registered))
	}
	info := registered[0]
	if info.ServiceType != flowctlpb.ServiceType_SERVICE_TYPE_SOURCE {
		t.Errorf("registered as %v, want SERVICE_TYPE_SOURCE", info.ServiceType)
	}
	if info.ComponentId != "stellar-ingest-test" {
		t.Errorf("component id = %q, want the configured one", info.ComponentId)
	}
	for _, key := range []string{"component", "version", "network", "storage_type", "range_start", "range_end"} {
		if info.Metadata[key] == "" {
			t.Errorf("registration metadata is missing %q; a watcher cannot tell runs apart without it", key)
		}
	}
}

// The phases are the control plane's view of progress. Reporting completion
// without the phases before it would make a long backfill look like a long
// silence followed by a result.
func TestReportsEveryExtractionPhase(t *testing.T) {
	stub := &stubControlPlane{}
	enableFlowctl(t, startStubControlPlane(t, stub))

	if _, err := Run(context.Background(), flowctlRequest(t), Deps{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	got := strings.Join(stub.phases(), ",")
	for _, want := range []string{"extract", "extract_complete"} {
		if !strings.Contains(got, want) {
			t.Errorf("no %q phase was reported; phases were [%s]", want, got)
		}
	}

	_, chunks := stub.snapshot()
	if len(chunks) == 0 {
		t.Fatal("no chunk runs reported at all")
	}
	for _, c := range chunks {
		if c.ChunkStart == 0 || c.ChunkEnd == 0 {
			t.Errorf("chunk reported without a range: start=%d end=%d", c.ChunkStart, c.ChunkEnd)
		}
	}
	// The last word must be terminal. A chunk left RUNNING is how a
	// scheduler waits forever on work that already finished.
	last := chunks[len(chunks)-1]
	if last.Status == flowctlpb.ChunkStatus_CHUNK_STATUS_RUNNING {
		t.Errorf("final report left the chunk RUNNING (phase %q)", last.Phase)
	}
}

// A failed run must tell the control plane WHY in a form it can act on.
// An unclassified failure is retried like any other, which is how a bad
// ledger range becomes an infinite loop.
func TestFailureIsReportedWithAClassification(t *testing.T) {
	stub := &stubControlPlane{}
	enableFlowctl(t, startStubControlPlane(t, stub))

	req := flowctlRequest(t)
	req.Bucket = "testdata/fixtures/does-not-exist.xdr"

	if _, err := Run(context.Background(), req, Deps{}); err == nil {
		t.Fatal("a missing source was accepted")
	}

	_, chunks := stub.snapshot()
	var failed *flowctlpb.ChunkRun
	for _, c := range chunks {
		if c.Status == flowctlpb.ChunkStatus_CHUNK_STATUS_FAILED {
			failed = c
		}
	}
	if failed == nil {
		t.Fatal("the run failed but no FAILED chunk was reported; the control plane sees the record simply stop")
	}
	// UNKNOWN is the classifier's fallback. Reaching it means the error text
	// matched no bucket, and a scheduler is left guessing whether to retry.
	if failed.FailureClass == flowctlpb.FailureClass_FAILURE_CLASS_UNKNOWN {
		t.Errorf("failure classified UNKNOWN; a scheduler cannot tell retryable from poison. error was: %q",
			failed.Error)
	}
	if failed.Error == "" {
		t.Error("failure reported without any error text")
	}
	if failed.RecommendedAction == "" {
		t.Error("failure reported without a recommended action")
	}
}

// The documented contract (README, "flowctl"): enabling the integration is
// fail-closed, and registration failure aborts the run. That is a real
// operational trade -- a control plane that is down stops ingestion that
// would otherwise have succeeded -- so it should be a decision somebody
// made on purpose, pinned, rather than something rediscovered by running
// the binary.
func TestUnreachableControlPlaneStopsTheRunBeforeAnyWork(t *testing.T) {
	// A port nothing is listening on.
	t.Setenv("ENABLE_FLOWCTL", "true")
	t.Setenv("FLOWCTL_ENDPOINT", "127.0.0.1:1")
	t.Setenv("FLOWCTL_COMPONENT_ID", "stellar-ingest-test")

	req := flowctlRequest(t)
	_, err := Run(context.Background(), req, Deps{})
	if err == nil {
		t.Fatal("the run proceeded with an unreachable control plane; " +
			"the README documents this as fail-closed")
	}
	if !strings.Contains(err.Error(), "flowctl") {
		t.Errorf("error does not say flowctl was the problem: %v", err)
	}
	// Nothing should have been produced: the point of failing closed is to
	// fail before doing work, not after.
	entries, readErr := os.ReadDir(req.OutputDir)
	if readErr == nil && len(entries) > 0 {
		t.Errorf("a fail-closed run produced %d output entries", len(entries))
	}
}

// A control plane that is up but rejecting the component is a different
// failure from one that is down, and must also stop the run rather than
// proceeding unregistered -- an unregistered component reports nothing,
// so the run would be invisible for its whole duration.
func TestRejectedRegistrationStopsTheRun(t *testing.T) {
	stub := &stubControlPlane{registerErr: errors.New("component not provisioned")}
	enableFlowctl(t, startStubControlPlane(t, stub))

	_, err := Run(context.Background(), flowctlRequest(t), Deps{})
	if err == nil {
		t.Fatal("the run proceeded after the control plane refused registration")
	}
	if got := exit.ClassOf(err); got == exit.OK {
		t.Errorf("rejected registration produced exit class %v", got)
	}
}
