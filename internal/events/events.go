// Package events is §4's third primitive.
//
// §4 distinguishes three things: "command (request a mutation), query (read
// current state), and event (immutable notification about something that
// happened)". This tool had the first two and nothing of the third -- a run's
// history was its stderr, so the only way to know when the DuckLake push
// started was to watch a log line go past.
//
// An event carries what §4 asks for: "type, version, resource ID,
// correlation/job ID, timestamp, payload, and an ordered cursor or sequence
// where resumable watching is offered".
//
// DELIVERY IS AT-MOST-ONCE, and the standard asks that this be stated rather
// than left to be discovered. Events are written to a stream as they happen;
// if the process is killed, the events it had not yet written are gone, and
// nothing replays them. That is honest for a one-shot CLI: there is no
// broker and no acknowledgement, so promising at-least-once would be a lie
// that a consumer would build retries against.
//
// What IS durable is the range catalog, which records a range's claim and
// its freeze transactionally. A consumer that needs a guarantee should read
// that -- `stellar-ingest inspect` -- and use events for liveness.
package events

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Schema identifies the event schema. Carried on every event so a consumer
// can tell it is reading something newer rather than silently misreading a
// renamed field.
const Schema = "stellar-ingest.event/v1"

// Event types. One constant each, so a typo in an emitter is a compile
// error rather than an event nobody is subscribed to.
const (
	// TypeRunStarted is emitted once, before any work.
	TypeRunStarted = "run.started"
	// TypeRunSucceeded and TypeRunFailed are terminal, and exactly one of
	// them is emitted for a run that reaches an end.
	TypeRunSucceeded = "run.succeeded"
	TypeRunFailed    = "run.failed"
	// TypeRunSkipped means the range was already frozen or claimed.
	TypeRunSkipped = "run.skipped"
	// TypeRangeClaimed and TypeRangeFrozen bracket the durable transitions
	// the catalog records.
	TypeRangeClaimed = "range.claimed"
	TypeRangeFrozen  = "range.frozen"
	// TypeExtractCompleted reports the Parquet phase.
	TypeExtractCompleted = "extract.completed"
	// TypeValidateCompleted reports the quality checks.
	TypeValidateCompleted = "validate.completed"
	// TypePushCompleted reports the DuckLake push.
	TypePushCompleted = "ducklake.pushed"
	// TypeChunkCompleted reports one chunk of a chunked backfill.
	TypeChunkCompleted = "chunk.completed"
)

// Event is an immutable notification that something happened.
type Event struct {
	Schema string `json:"schema"`
	// Sequence orders events within a job, starting at 1. It is the cursor
	// §4 asks for: a consumer that has seen 7 knows it is missing 8.
	Sequence uint64 `json:"sequence"`
	Type     string `json:"type"`
	// JobID correlates every event from one run, and is the same identifier
	// the range catalog stores as attempt_id -- so an event stream and the
	// durable record can be joined.
	JobID string `json:"job_id"`
	// Resource names what the event is about, e.g. "range/64000000-64000999".
	Resource string         `json:"resource,omitempty"`
	Time     time.Time      `json:"time"`
	Payload  map[string]any `json:"payload,omitempty"`
}

// Emitter assigns sequence numbers and writes events.
//
// Nil-safe in every method, like the logger: an operation takes one from its
// caller and most callers do not want events.
type Emitter struct {
	mu       sync.Mutex
	out      io.Writer
	jobID    string
	sequence uint64
	now      func() time.Time
}

// NewEmitter builds an emitter writing NDJSON to out.
//
// now is injected so a test can assert on timestamps; pass nil for the real
// clock.
func NewEmitter(jobID string, out io.Writer, now func() time.Time) *Emitter {
	if now == nil {
		now = time.Now
	}
	return &Emitter{out: out, jobID: jobID, now: now}
}

// JobID reports the correlation id these events carry.
func (e *Emitter) JobID() string {
	if e == nil {
		return ""
	}
	return e.jobID
}

// Emit writes one event.
//
// Errors are returned rather than swallowed, but every caller in the run
// path ignores them deliberately: a full disk on the event stream must not
// fail an ingestion that is otherwise working. The signature keeps the
// choice at the call site rather than making it here.
func (e *Emitter) Emit(eventType, resource string, payload map[string]any) error {
	if e == nil || e.out == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	e.sequence++
	event := Event{
		Schema:   Schema,
		Sequence: e.sequence,
		Type:     eventType,
		JobID:    e.jobID,
		Resource: resource,
		Time:     e.now().UTC(),
		Payload:  payload,
	}
	// One Encode call per event, under the lock: NDJSON is only NDJSON if a
	// concurrent emitter cannot interleave into the middle of a line.
	return json.NewEncoder(e.out).Encode(event)
}

// Count reports how many events have been emitted, for tests and for a
// final summary.
func (e *Emitter) Count() uint64 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sequence
}
