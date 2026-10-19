package app

// Deps is what an operation needs from its caller that is not data.
//
// RunRequest says what to do; Deps says where to say it and who to tell. They
// are separate because a request is serialisable and a writer is not: an API
// or a UI can build the same RunRequest this CLI builds, and supply its own
// streams.
//
// Both streams default to io.Discard and the logger to a discarding one, for
// the same reason internal/logging does: an operation nobody configured must
// not write to a stream it was never given. Every accessor is safe on a zero
// Deps, so a test that does not care about output passes Deps{}.

import (
	"io"

	"github.com/withObsrvr/stellar-ingest/internal/logging"
	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// Deps carries the operation's streams and reporters.
type Deps struct {
	// Out receives results: the things a caller would parse or redirect.
	// §3: "stdout contains results".
	Out io.Writer
	// Log receives narration: progress, phase headers, warnings,
	// diagnostics. §3: "stderr contains logs, progress, warnings,
	// diagnostics".
	Log *logging.Logger
	// Status reports OSC 7501 program status, for a terminal or a UI that
	// renders it.
	Status *status.Emitter
}

// log returns a usable logger, discarding when none was supplied.
func (d Deps) log() *logging.Logger {
	if d.Log == nil {
		return logging.Discard()
	}
	return d.Log
}

// out returns the results stream, discarding when none was supplied.
func (d Deps) out() io.Writer {
	if d.Out == nil {
		return io.Discard
	}
	return d.Out
}
