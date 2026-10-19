package logging

// The properties that matter, in the order they would bite:
//
//  1. --quiet must not suppress warnings or errors. A quiet flag that hides
//     failures turns a broken backfill into a silent one.
//  2. Nothing may reach stdout. The whole reason this package exists is
//     §3's stream split, and a Logger that writes to stdout re-breaks it.
//  3. A nil Logger must be safe. Operations take one from their caller and
//     tests pass none.
//  4. --no-color must leave no escape sequences, including in the level
//     prefixes, which is where they survived a first attempt.

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func lines(buf *bytes.Buffer) []string {
	trimmed := strings.TrimSuffix(buf.String(), "\n")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func TestQuietSuppressesProgressButNeverFailures(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: LevelQuiet, Err: &buf})

	log.Info("extracting 1-100")
	log.Detail("opened shard 3")
	log.Section("DuckLake Push")
	log.Field("Workers", "8")
	log.Line()
	if got := buf.String(); got != "" {
		t.Errorf("--quiet emitted progress: %q", got)
	}

	log.Warn("hot buffer load had errors")
	log.Error("push failed")
	out := buf.String()
	if !strings.Contains(out, "hot buffer load had errors") {
		t.Error("--quiet suppressed a warning; an invisible warning does not exist")
	}
	if !strings.Contains(out, "push failed") {
		t.Error("--quiet suppressed an error; a silent failure is worse than a noisy one")
	}
}

func TestNormalEmitsProgressButNotDetail(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: LevelNormal, Err: &buf})

	log.Info("extracting 1-100")
	log.Detail("opened shard 3")

	out := buf.String()
	if !strings.Contains(out, "extracting 1-100") {
		t.Error("normal level dropped progress")
	}
	if strings.Contains(out, "opened shard 3") {
		t.Error("normal level emitted --verbose detail")
	}
}

func TestVerboseEmitsDetail(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: LevelVerbose, Err: &buf})
	log.Detail("opened shard 3")
	if !strings.Contains(buf.String(), "opened shard 3") {
		t.Error("--verbose dropped detail")
	}
}

// Attributes are structure, and structure is what --verbose asks for. At
// normal level a progress line should read as a sentence.
func TestAttributesAppearOnlyWhenVerbose(t *testing.T) {
	var normal, verbose bytes.Buffer
	New(Options{Level: LevelNormal, Err: &normal}).Slog().Info("push", "table", "ledgers_row_v2", "rows", 2001)
	New(Options{Level: LevelVerbose, Err: &verbose}).Slog().Info("push", "table", "ledgers_row_v2", "rows", 2001)

	if strings.Contains(normal.String(), "ledgers_row_v2") {
		t.Errorf("normal level rendered attributes: %q", normal.String())
	}
	if !strings.Contains(verbose.String(), "table=ledgers_row_v2") || !strings.Contains(verbose.String(), "rows=2001") {
		t.Errorf("--verbose did not render attributes: %q", verbose.String())
	}
}

// slog.TextHandler would have produced time=... level=INFO msg="..." which
// makes an aligned banner unreadable. The message must come through as the
// line a person reads.
func TestProgressIsNotRenderedAsKeyValue(t *testing.T) {
	var buf bytes.Buffer
	New(Options{Level: LevelNormal, Err: &buf}).Info("Ledger range:      1 - 100")

	got := buf.String()
	for _, forbidden := range []string{"level=", "msg=", "time="} {
		if strings.Contains(got, forbidden) {
			t.Errorf("progress line carries %q, so the banner is unreadable: %q", forbidden, got)
		}
	}
	if got != "Ledger range:      1 - 100\n" {
		t.Errorf("line = %q, want the message verbatim", got)
	}
}

func TestNoColorLeavesNoEscapeSequences(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: LevelVerbose, Color: false, Err: &buf})

	log.Section("DuckLake Push")
	log.Field("Workers", "8")
	log.Info("plain")
	log.Detail("detail")
	log.Warn("careful")
	log.Error("broken")

	if strings.Contains(buf.String(), "\033") {
		t.Errorf("colour disabled but output carries an escape sequence: %q", buf.String())
	}
}

func TestColorStylesSectionsAndLevels(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: LevelVerbose, Color: true, Err: &buf})
	log.Section("DuckLake Push")
	log.Warn("careful")
	log.Error("broken")

	out := buf.String()
	if !strings.Contains(out, bold) {
		t.Error("colour enabled but the section header is unstyled")
	}
	if !strings.Contains(out, yellow) {
		t.Error("colour enabled but the warning prefix is unstyled")
	}
	if !strings.Contains(out, red) {
		t.Error("colour enabled but the error prefix is unstyled")
	}
}

// Warnings and errors must be identifiable; progress must not be prefixed,
// or every banner line reads "info:".
func TestOnlyFailuresAreLabelled(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: LevelNormal, Err: &buf})
	log.Info("Workers:  8")
	log.Warn("careful")
	log.Error("broken")

	got := lines(&buf)
	if len(got) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(got), got)
	}
	if strings.HasPrefix(got[0], "info:") || strings.Contains(got[0], ":  8") == false {
		t.Errorf("progress line is labelled or mangled: %q", got[0])
	}
	if !strings.HasPrefix(got[1], "warning: ") {
		t.Errorf("warning is not labelled: %q", got[1])
	}
	if !strings.HasPrefix(got[2], "error: ") {
		t.Errorf("error is not labelled: %q", got[2])
	}
}

// A zero Options must not write to a real stream, or any caller that builds
// a Logger without supplying one prints into whatever stream happened to be
// there. Asserted on the handler directly: writing and observing nothing
// proves nothing, since the output would have gone to stderr either way.
func TestZeroOptionsWritesToDiscard(t *testing.T) {
	log := New(Options{})

	handler, ok := log.Slog().Handler().(*humanHandler)
	if !ok {
		t.Fatalf("handler is %T, want *humanHandler; this test needs rewriting", log.Slog().Handler())
	}
	if handler.out != io.Discard {
		t.Errorf("zero Options writes to %T, want io.Discard: a Logger nobody configured must "+
			"not reach a real stream", handler.out)
	}
	if got := log.Level(); got != LevelQuiet {
		t.Errorf("zero Options level = %v, want %v", got, LevelQuiet)
	}
}

func TestNilLoggerIsSafe(t *testing.T) {
	var log *Logger
	// Each of these is called from the run path, so any one panicking takes
	// down an ingestion.
	log.Info("a")
	log.Detail("b")
	log.Warn("c")
	log.Error("d")
	log.Line()
	log.Section("e")
	log.Field("f", "g")
	if got := log.Level(); got != LevelQuiet {
		t.Errorf("nil Logger level = %v, want %v", got, LevelQuiet)
	}
	if log.Slog() == nil {
		t.Error("nil Logger returned a nil slog.Logger; callers would panic")
	}
	log.Slog().Info("h")
}

func TestDiscardIsUsable(t *testing.T) {
	log := Discard()
	log.Info("x")
	log.Error("y")
	if got := log.Level(); got != LevelQuiet {
		t.Errorf("Discard level = %v, want %v", got, LevelQuiet)
	}
}

func TestFieldAlignsLabels(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: LevelNormal, Err: &buf})
	log.Field("Workers", "8")
	log.Field("Output directory", "/tmp/bronze")

	got := lines(&buf)
	if len(got) != 2 {
		t.Fatalf("got %d lines, want 2", len(got))
	}
	// The values must start at the same column, which is the entire reason
	// the width lives here rather than at each call site.
	first := strings.Index(got[0], "8")
	second := strings.Index(got[1], "/tmp/bronze")
	if first != second {
		t.Errorf("values start at columns %d and %d; the banner is misaligned:\n%q\n%q",
			first, second, got[0], got[1])
	}
}

func TestLevelNames(t *testing.T) {
	for _, c := range []struct {
		level Level
		want  string
	}{{LevelQuiet, "quiet"}, {LevelNormal, "normal"}, {LevelVerbose, "verbose"}} {
		if got := c.level.String(); got != c.want {
			t.Errorf("%d.String() = %q, want %q", int(c.level), got, c.want)
		}
	}
	if got := Level(99).String(); !strings.HasPrefix(got, "unknown") {
		t.Errorf("unknown level = %q", got)
	}
}
