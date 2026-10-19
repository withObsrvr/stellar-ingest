package status

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

// The exact bytes matter: a consumer parses this, and a sequence that is
// nearly right is a sequence that is ignored.
func TestEncodeProducesTheOSC7501Sequence(t *testing.T) {
	p := 42
	got := Report{
		State: Working, ID: "extract/chunk-3", App: "stellar-ingest",
		Title: "Extracting", Progress: &p,
	}.Encode()

	const want = "\x1b]7501;state=working:id=extract/chunk-3:app=stellar-ingest:title=Extracting:progress=42\x1b\\"
	if got != want {
		t.Errorf("\n got %q\nwant %q", got, want)
	}
}

// msg is base64 on the wire, so it cannot smuggle a separator or terminate
// the sequence early.
func TestMessageIsBase64Encoded(t *testing.T) {
	got := Report{State: Done, Msg: "Apply 3 to add, 1 to change"}.Encode()

	want := base64.StdEncoding.EncodeToString([]byte("Apply 3 to add, 1 to change"))
	if !strings.Contains(got, "msg="+want) {
		t.Errorf("sequence does not carry the encoded message: %q", got)
	}
	// The raw text must not appear: a colon or an ESC in a message would
	// otherwise break the record.
	if strings.Contains(got, "Apply 3 to add") {
		t.Errorf("the message was written in the clear: %q", got)
	}
}

func TestEncodeOmitsEmptyFields(t *testing.T) {
	got := Report{State: Idle}.Encode()
	if got != "\x1b]7501;state=idle\x1b\\" {
		t.Errorf("got %q; only state should appear", got)
	}
}

// Validation exists because an invalid record is silently dropped by the
// consumer, which is indistinguishable from the program never reporting.
func TestValidateRejectsRecordsAConsumerWouldIgnore(t *testing.T) {
	p101 := 101
	pNeg := -1
	for _, tc := range []struct {
		name string
		r    Report
	}{
		{"no state", Report{}},
		{"unknown state", Report{State: "running"}},
		{"blocked without a kind", Report{State: Blocked, Msg: "approve?"}},
		{"id with a space", Report{State: Working, ID: "extract chunk"}},
		{"id with a leading slash", Report{State: Working, ID: "/extract"}},
		{"id with an empty segment", Report{State: Working, ID: "extract//chunk"}},
		{"progress above 100", Report{State: Working, Progress: &p101}},
		{"negative progress", Report{State: Working, Progress: &pNeg}},
		{"multiline msg", Report{State: Done, Msg: "line\nline"}},
		{"multiline title", Report{State: Done, Title: "a\rb"}},
	} {
		if err := tc.r.Validate(); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}

	// And the shapes that must be accepted.
	for _, tc := range []struct {
		name string
		r    Report
	}{
		{"root working", Report{State: Working}},
		{"nested id", Report{State: Working, ID: "extract/chunk-3/push"}},
		{"id with dots and dashes", Report{State: Done, ID: "range.64000000-64010000"}},
		{"blocked with a kind", Report{State: Blocked, Kind: KindPermission, Msg: "ok?"}},
		{"clear", Report{State: Clear}},
	} {
		if err := tc.r.Validate(); err != nil {
			t.Errorf("%s: rejected: %v", tc.name, err)
		}
	}
}

func TestEmitterFillsInTheAppName(t *testing.T) {
	var buf bytes.Buffer
	e := NewEmitter(&buf, "stellar-ingest", true)
	if err := e.Working("extract", "Extracting", 10); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "app=stellar-ingest") {
		t.Errorf("app not filled in: %q", buf.String())
	}
}

// A disabled emitter must write nothing at all. This is the path taken when
// stderr is a pipe or a log file, where a sequence is noise.
func TestDisabledEmitterWritesNothing(t *testing.T) {
	var buf bytes.Buffer
	e := NewEmitter(&buf, "stellar-ingest", false)
	for _, err := range []error{
		e.Working("a", "t", 1),
		e.Done("a", "done"),
		e.Failed("a", "bad"),
		e.Blocked("a", KindAuth, "creds"),
		e.Report(Report{State: Idle}),
	} {
		if err != nil {
			t.Errorf("disabled emitter returned an error: %v", err)
		}
	}
	if buf.Len() != 0 {
		t.Errorf("disabled emitter wrote %q", buf.String())
	}
}

// A nil emitter is usable, so callers need no nil check on a hot path.
func TestNilEmitterIsSafe(t *testing.T) {
	var e *Emitter
	if e.Enabled() {
		t.Error("a nil emitter reports enabled")
	}
	if err := e.Working("a", "t", 1); err != nil {
		t.Errorf("a nil emitter returned an error: %v", err)
	}
}

// Negative percent means "unknown", and must not emit progress=-1, which is
// outside the protocol's range.
func TestUnknownProgressOmitsTheField(t *testing.T) {
	var buf bytes.Buffer
	e := NewEmitter(&buf, "app", true)
	if err := e.Working("a", "Working", -1); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "progress=") {
		t.Errorf("emitted a progress field for unknown progress: %q", buf.String())
	}
}
