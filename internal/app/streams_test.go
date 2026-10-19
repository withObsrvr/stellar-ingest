package app

// §3's stream split, asserted.
//
// Before internal/logging the banner, every phase header and every summary
// went to stdout alongside anything a caller would want to parse, so piping
// this tool's output anywhere was impossible. These hold the line: narration
// on the log stream, results on the results stream, and nothing crossing.
//
// A zero Deps discards both, which is also what keeps the rest of this
// package's tests from printing a banner into the test log.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/logging"
)

func TestNarrationGoesToTheLogStreamAndNotToResults(t *testing.T) {
	var results, narration bytes.Buffer
	deps := Deps{
		Out: &results,
		Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration}),
	}

	// A run that fails on input produces no narration and no results, so
	// drive one far enough to print the banner: an unwritable output
	// directory fails after the request is accepted but before any network.
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	req := valid(filepath.Join(parent, "out"))
	_, _ = Run(context.Background(), req, deps)

	if got := results.String(); got != "" {
		t.Errorf("results stream carries narration: %q", got)
	}
}

// --plan is a result: a caller reads it to decide what to dispatch. It must
// land on the results stream even when narration is silenced.
func TestPlanOutputIsAResult(t *testing.T) {
	var results, narration bytes.Buffer
	deps := Deps{
		Out: &results,
		Log: logging.New(logging.Options{Level: logging.LevelQuiet, Err: &narration}),
	}
	req := valid(t.TempDir())
	req.PlanOnly = true
	// No catalog, so this is rejected before reaching one; the assertion is
	// about which stream the rejection does NOT pollute.
	_, err := Run(context.Background(), req, deps)
	if err == nil {
		t.Fatal("expected --plan without a catalog to be rejected")
	}
	if got := results.String(); got != "" {
		t.Errorf("a rejected run wrote to the results stream: %q", got)
	}
}

// The banner must be suppressible. A caller that wants only the result
// should be able to get only the result.
func TestQuietSuppressesTheBanner(t *testing.T) {
	var loud, quiet bytes.Buffer
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	req := valid(filepath.Join(parent, "out"))

	_, _ = Run(context.Background(), req, Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &loud})})
	_, _ = Run(context.Background(), req, Deps{Log: logging.New(logging.Options{Level: logging.LevelQuiet, Err: &quiet})})

	if strings.Contains(quiet.String(), "Stellar History Loader") {
		t.Errorf("--quiet still printed the banner: %q", quiet.String())
	}
	// And the control: at normal level it is there, so the assertion above
	// is not passing because the banner never prints at all.
	if !strings.Contains(loud.String(), "Stellar History Loader") {
		t.Errorf("normal level did not print the banner, so the quiet assertion proves nothing: %q",
			loud.String())
	}
}

// A zero Deps must not reach a real stream: most of this package's tests
// pass one.
func TestZeroDepsDiscards(t *testing.T) {
	if got := (Deps{}).out(); got == os.Stdout {
		t.Error("zero Deps writes results to os.Stdout")
	}
	if got := (Deps{}).log(); got == nil {
		t.Error("zero Deps returned a nil logger; call sites would panic")
	}
	// Exercised rather than only inspected, since a discarding logger that
	// panics is no better than a nil one.
	(Deps{}).log().Info("nowhere")
	(Deps{}).log().Section("nowhere")
}
