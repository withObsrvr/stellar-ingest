package cli

// Opening the event stream is an adapter decision: whether "-" means stdout,
// whether a file is appended to, and what happens when neither is asked for.

import (
	"fmt"
	"io"
	"os"

	"github.com/withObsrvr/stellar-ingest/internal/events"
	"github.com/withObsrvr/stellar-ingest/internal/jobs"
)

// OpenEventStream builds the emitter for this invocation.
//
// Returns a closer the caller defers. An empty path emits nothing, which is
// the default: an event stream nobody asked for is a file nobody deletes.
//
// Files are APPENDED to, not truncated. A thousand-chunk backfill re-invokes
// this binary per chunk, all of them pointing at the same path; truncating
// would leave only the last chunk's events, which is the opposite of what an
// append-only log is for.
func OpenEventStream(path, jobID string, stdout io.Writer) (*events.Emitter, func(), error) {
	switch path {
	case "":
		return nil, func() {}, nil
	case "-":
		// stdout already carries the result. Sharing the stream is the
		// caller's explicit choice, and NDJSON plus NDJSON is still NDJSON.
		return events.NewEmitter(jobID, stdout, nil), func() {}, nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open event stream %s: %w", path, err)
	}
	return events.NewEmitter(jobID, file, nil), func() { _ = file.Close() }, nil
}

// NewJobID mints the correlation id for a run.
//
// The same generator the range catalog uses for attempt_id, so an event
// stream and the durable record can be joined on one value rather than two
// that have to be matched by timestamp.
func NewJobID() string { return jobs.NewAttemptID() }
