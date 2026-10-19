package cli

import (
	"bytes"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/exit"
	"github.com/withObsrvr/stellar-ingest/internal/logging"
)

func TestLevelFromFlags(t *testing.T) {
	cases := []struct {
		name    string
		quiet   bool
		verbose bool
		want    logging.Level
	}{
		{"default", false, false, logging.LevelNormal},
		{"quiet", true, false, logging.LevelQuiet},
		{"verbose", false, true, logging.LevelVerbose},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts, err := resolveLoggingOptions(c.quiet, c.verbose, false, Terminal{}, &bytes.Buffer{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if opts.Level != c.want {
				t.Errorf("level = %v, want %v", opts.Level, c.want)
			}
		})
	}
}

// Preferring one silently would make a script that passes both behave
// differently depending on which won.
func TestQuietAndVerboseTogetherIsRejected(t *testing.T) {
	_, err := resolveLoggingOptions(true, true, false, Terminal{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("--quiet --verbose was accepted; one of them is being silently ignored")
	}
	if got := exit.ClassOf(err); got != exit.Usage {
		t.Errorf("exit class = %v, want %v", got, exit.Usage)
	}
}

func TestColorRequiresATerminalAndNoOptOut(t *testing.T) {
	cases := []struct {
		name    string
		noColor bool
		term    Terminal
		want    bool
	}{
		{"terminal, nothing set", false, Terminal{IsTTY: true}, true},
		{"--no-color wins over a terminal", true, Terminal{IsTTY: true}, false},
		{"NO_COLOR wins over a terminal", false, Terminal{IsTTY: true, NoColorEnv: "1"}, false},
		{"NO_COLOR counts when present and non-empty", false, Terminal{IsTTY: true, NoColorEnv: "anything"}, false},
		{"empty NO_COLOR does not count", false, Terminal{IsTTY: true, NoColorEnv: ""}, true},
		{"redirected stderr gets no colour", false, Terminal{IsTTY: false}, false},
		{"redirected and opted out", true, Terminal{IsTTY: false}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := colorEnabled(c.noColor, c.term); got != c.want {
				t.Errorf("colorEnabled = %v, want %v", got, c.want)
			}
		})
	}
}

// Nomad captures this tool's stderr. Escape sequences in a captured stream
// are noise in every log line, which is why a non-terminal is enough on its
// own to turn colour off.
func TestNonTerminalIsEnoughToDisableColor(t *testing.T) {
	opts, err := resolveLoggingOptions(false, false, false, Terminal{IsTTY: false}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Color {
		t.Error("colour enabled for a redirected stderr; captured logs would carry escape codes")
	}
}

// The resolved logger must write where the caller said, not to os.Stderr.
func TestResolvedLoggerWritesToTheGivenStream(t *testing.T) {
	var buf bytes.Buffer
	opts, err := resolveLoggingOptions(false, false, true, Terminal{}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	logging.New(opts).Info("hello")
	if got := buf.String(); got != "hello\n" {
		t.Errorf("stream got %q, want %q", got, "hello\n")
	}
}
