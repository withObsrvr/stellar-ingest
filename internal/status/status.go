// Package status reports what the program is doing, in a form a terminal or
// a UI can consume, using the OSC 7501 program-status sequence.
//
// The point is that the program already knows its own state. A supervising
// terminal, dashboard or "agentic inbox" otherwise has to infer it by
// scraping output or watching window titles, which is why tools that do that
// accumulate per-version rules that break. A program that reports directly
// needs no such rules, and the report travels over the PTY, so it works
// through SSH and inside containers with no socket, SDK or environment
// variable.
//
// This is also what lets one implementation serve both a command line and a
// UI, which is the Obsrvr CLI standard's first principle: the same operation,
// reported the same way, whoever is watching.
//
// Reference: https://mitchellh.com/writing/program-status-osc7501
package status

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
)

// State is the required field of every report.
type State string

const (
	// Idle means awaiting instruction, with no work in flight.
	Idle State = "idle"
	// Working means running. Carry Progress when it is known.
	Working State = "working"
	// Done means finished with a result ready. Survives process exit.
	Done State = "done"
	// Blocked means a person has to act before work continues. Requires Kind.
	Blocked State = "blocked"
	// Failed means stopped on an error. Survives process exit.
	//
	// The wire value is "error"; the Go name is Failed because a package-level
	// identifier called Error next to an error type reads as the type.
	Failed State = "error"
	// Clear removes a previously reported record.
	Clear State = "clear"
)

// Kind says why a Blocked report is blocked.
type Kind string

const (
	// KindPermission: waiting for approval to proceed.
	KindPermission Kind = "permission"
	// KindQuestion: waiting for an answer.
	KindQuestion Kind = "question"
	// KindAuth: waiting for credentials.
	KindAuth Kind = "auth"
)

// Report is one program-status record.
type Report struct {
	State State
	// ID is a path, like "extract/chunk-3", giving records a parent-child
	// relationship so one process can report overall progress while its
	// parts report their own. Empty means the root record.
	ID string
	// App identifies the program. Children inherit it from an ancestor.
	App string
	// Title is a short human-readable label.
	Title string
	// Msg is a one-line explanation, base64-encoded on the wire so it can
	// contain anything except a newline.
	Msg string
	// Progress is a percentage, 0 to 100. Only meaningful while Working.
	Progress *int
	// Kind is required when State is Blocked.
	Kind Kind
}

// idPattern is the record-ID grammar: path segments of word characters,
// separated by "/". A report whose ID does not match is ignored by the
// consumer, so this package rejects it rather than emitting something that
// will be silently dropped.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$`)

// Validate reports why this record cannot be sent, if it cannot.
func (r Report) Validate() error {
	switch r.State {
	case Idle, Working, Done, Blocked, Failed, Clear:
	case "":
		return fmt.Errorf("status: state is required")
	default:
		return fmt.Errorf("status: unknown state %q", r.State)
	}
	if r.State == Blocked && r.Kind == "" {
		return fmt.Errorf("status: a blocked report needs a kind (permission, question or auth)")
	}
	if r.ID != "" && !idPattern.MatchString(r.ID) {
		return fmt.Errorf("status: id %q is not a path of word segments; a consumer would ignore it", r.ID)
	}
	if r.Progress != nil && (*r.Progress < 0 || *r.Progress > 100) {
		return fmt.Errorf("status: progress %d is outside 0-100", *r.Progress)
	}
	if strings.ContainsAny(r.Title, "\n\r") || strings.ContainsAny(r.Msg, "\n\r") {
		return fmt.Errorf("status: title and msg must be single-line")
	}
	return nil
}

// Encode renders the OSC 7501 sequence for this record.
//
//	ESC ] 7501 ; key=value:key=value ESC \
//
// Fields are ordered state, id, app, title, progress, kind, msg so the output
// is comparable in a test rather than depending on map iteration.
func (r Report) Encode() string {
	var fields []string
	add := func(k, v string) {
		if v != "" {
			fields = append(fields, k+"="+v)
		}
	}
	add("state", string(r.State))
	add("id", r.ID)
	add("app", r.App)
	add("title", r.Title)
	if r.Progress != nil {
		add("progress", fmt.Sprintf("%d", *r.Progress))
	}
	add("kind", string(r.Kind))
	if r.Msg != "" {
		// Base64 so the message cannot contain the separators or terminate
		// the sequence early.
		add("msg", base64.StdEncoding.EncodeToString([]byte(r.Msg)))
	}
	return "\x1b]7501;" + strings.Join(fields, ":") + "\x1b\\"
}

// Emitter writes reports to a stream.
//
// Reports go to stderr, not stdout. The CLI standard reserves stdout for
// results so a pipeline keeps working, and an escape sequence written into a
// result stream corrupts it -- a JSON consumer would choke on it. stderr is
// the same PTY when a person is watching, which is who these are for.
type Emitter struct {
	mu      sync.Mutex
	w       io.Writer
	app     string
	enabled bool
}

// NewEmitter returns an emitter writing to w, labelling reports with app.
//
// Disabled when enabled is false, in which case every call is a no-op and
// nothing is written. Callers decide: the default is on when stderr is a
// terminal, because a sequence written to a log file or a pipe is noise.
func NewEmitter(w io.Writer, app string, enabled bool) *Emitter {
	return &Emitter{w: w, app: app, enabled: enabled}
}

// Enabled says whether reports are being written.
func (e *Emitter) Enabled() bool { return e != nil && e.enabled }

// Report sends one record, filling in App when the caller left it empty.
//
// Errors are returned rather than swallowed so a test can see them, but a
// caller on a hot path can ignore the result: a status report that fails to
// write must never fail the work it is describing.
func (e *Emitter) Report(r Report) error {
	if !e.Enabled() {
		return nil
	}
	if r.App == "" {
		r.App = e.app
	}
	if err := r.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, err := io.WriteString(e.w, r.Encode())
	return err
}

// Working reports progress on a record. Pass a negative percent when it is
// not known.
func (e *Emitter) Working(id, title string, percent int) error {
	r := Report{State: Working, ID: id, Title: title}
	if percent >= 0 {
		r.Progress = &percent
	}
	return e.Report(r)
}

// Done reports a finished record. The consumer keeps it after exit.
func (e *Emitter) Done(id, msg string) error {
	return e.Report(Report{State: Done, ID: id, Msg: msg})
}

// Failed reports a record that stopped on an error.
func (e *Emitter) Failed(id, msg string) error {
	return e.Report(Report{State: Failed, ID: id, Msg: msg})
}

// Blocked reports a record waiting on a person.
func (e *Emitter) Blocked(id string, kind Kind, msg string) error {
	return e.Report(Report{State: Blocked, ID: id, Kind: kind, Msg: msg})
}

// StderrIsTerminal reports whether stderr looks like a terminal.
//
// Checked through the file mode rather than a terminal library: a character
// device is the distinction that matters here, and it avoids a dependency for
// one bit of information.
func StderrIsTerminal() bool {
	info, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
