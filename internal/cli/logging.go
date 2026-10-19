package cli

// Deciding how to narrate is the adapter's job.
//
// Whether colour is appropriate depends on three things that have nothing to
// do with ingesting ledgers: a flag, an environment convention, and whether
// stderr is a terminal. internal/logging deliberately knows none of them --
// §2 puts presentation decisions here, and a package the application layer
// depends on should not reach for os.Stderr to answer one.

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/exit"
	"github.com/withObsrvr/stellar-ingest/internal/logging"
	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// Terminal is what the environment says about the output stream, gathered so
// the decision below can be tested without one.
type Terminal struct {
	// IsTTY reports whether stderr is a character device.
	IsTTY bool
	// NoColorEnv is the raw NO_COLOR value. Per no-color.org the variable
	// counts when it is present and non-empty, whatever it contains.
	NoColorEnv string
}

// ResolveLogging builds the logger for this invocation from the real
// environment.
func ResolveLogging(v *Values, errOut io.Writer) (*logging.Logger, error) {
	opts, err := resolveLoggingOptions(v.Quiet, v.Verbose, v.NoColor, Terminal{
		IsTTY:      status.StderrIsTerminal(),
		NoColorEnv: os.Getenv("NO_COLOR"),
	}, errOut)
	if err != nil {
		return nil, err
	}
	return logging.New(opts), nil
}

// resolveLoggingOptions is the decision, with the environment passed in.
func resolveLoggingOptions(quiet, verbose, noColor bool, term Terminal, errOut io.Writer) (logging.Options, error) {
	// Contradictory input, and silently preferring one would make a script
	// that passes both behave differently depending on which won. §3 wants
	// invalid input rejected, not reconciled.
	if quiet && verbose {
		return logging.Options{}, exit.Usagef("--quiet and --verbose cannot both be set")
	}

	level := logging.LevelNormal
	switch {
	case quiet:
		level = logging.LevelQuiet
	case verbose:
		level = logging.LevelVerbose
	}

	return logging.Options{
		Level: level,
		Color: colorEnabled(noColor, term),
		Err:   errOut,
	}, nil
}

// colorEnabled answers the presentation question.
//
// Any one of the three says no: the flag is explicit, NO_COLOR is the
// cross-tool convention, and a redirected stream means escape sequences
// would land in a file or a log aggregator rather than on a screen. That
// last one matters here specifically: this tool's stderr is captured by
// Nomad, where colour codes become noise in every log line.
func colorEnabled(noColorFlag bool, term Terminal) bool {
	if noColorFlag {
		return false
	}
	if term.NoColorEnv != "" {
		return false
	}
	return term.IsTTY
}

// WithTimeout bounds a run when --timeout was given.
//
// Zero means no limit, which is the default and what every deployed
// invocation relies on: a backfill chunk can legitimately take hours and a
// surprise deadline would abandon it mid-push. A cancel function is returned
// in both cases so the caller's defer is unconditional.
func WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}
