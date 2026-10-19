package events

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func fixedClock() func() time.Time {
	t := time.Date(2026, 10, 19, 16, 39, 18, 0, time.UTC)
	return func() time.Time { return t }
}

func decodeAll(t *testing.T, buf *bytes.Buffer) []Event {
	t.Helper()
	var out []Event
	for _, line := range strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line is not valid JSON: %v (%q)", err, line)
		}
		out = append(out, e)
	}
	return out
}

// The cursor §4 asks for: a consumer that has seen 7 knows it is missing 8.
// Starting at 1 matters -- a zero would be indistinguishable from an
// unset field on a consumer that decodes into a struct.
func TestSequenceStartsAtOneAndIncrementsByOne(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewEmitter("job-1", &buf, fixedClock())

	for i := 0; i < 4; i++ {
		if err := emitter.Emit(TypeRunStarted, "range/1-10", nil); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}

	got := decodeAll(t, &buf)
	if len(got) != 4 {
		t.Fatalf("got %d events, want 4", len(got))
	}
	for i, event := range got {
		if want := uint64(i + 1); event.Sequence != want {
			t.Errorf("event %d has sequence %d, want %d", i, event.Sequence, want)
		}
	}
}

// Every event carries what §4 lists, or a consumer cannot correlate them.
func TestEventCarriesTheRequiredFields(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewEmitter("job-abc", &buf, fixedClock())
	if err := emitter.Emit(TypePushCompleted, "range/64000000-64000999", map[string]any{"rows": 10123654}); err != nil {
		t.Fatalf("emit: %v", err)
	}

	got := decodeAll(t, &buf)[0]
	if got.Schema != Schema {
		t.Errorf("schema = %q, want %q", got.Schema, Schema)
	}
	if got.Type != TypePushCompleted {
		t.Errorf("type = %q", got.Type)
	}
	if got.JobID != "job-abc" {
		t.Errorf("job_id = %q; without it events from concurrent chunks cannot be told apart", got.JobID)
	}
	if got.Resource != "range/64000000-64000999" {
		t.Errorf("resource = %q", got.Resource)
	}
	if got.Time.IsZero() {
		t.Error("event has no timestamp")
	}
	if got.Time.Location() != time.UTC {
		t.Errorf("timestamp is in %v, want UTC; mixed zones make an event stream unsortable", got.Time.Location())
	}
	if got.Payload["rows"] != float64(10123654) {
		t.Errorf("payload lost its contents: %v", got.Payload)
	}
}

// NDJSON is only NDJSON if every event is exactly one line.
func TestEachEventIsOneLine(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewEmitter("job-1", &buf, fixedClock())
	_ = emitter.Emit(TypeRunStarted, "range/1-10", map[string]any{"nested": map[string]any{"a": 1}})

	out := buf.String()
	if got := strings.Count(out, "\n"); got != 1 {
		t.Errorf("one event produced %d lines, want 1: %q", got, out)
	}
}

// Concurrent chunks share an emitter. A torn line is an unparseable stream.
func TestConcurrentEmitsDoNotInterleave(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewEmitter("job-1", &buf, fixedClock())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = emitter.Emit(TypeChunkCompleted, "range/1-10", map[string]any{"payload": strings.Repeat("x", 200)})
		}()
	}
	wg.Wait()

	got := decodeAll(t, &buf)
	if len(got) != 50 {
		t.Fatalf("got %d events, want 50", len(got))
	}
	seen := map[uint64]bool{}
	for _, event := range got {
		if seen[event.Sequence] {
			t.Errorf("sequence %d emitted twice", event.Sequence)
		}
		seen[event.Sequence] = true
	}
	for i := uint64(1); i <= 50; i++ {
		if !seen[i] {
			t.Errorf("sequence %d missing", i)
		}
	}
}

func TestNilEmitterIsSafe(t *testing.T) {
	var emitter *Emitter
	if err := emitter.Emit(TypeRunStarted, "r", nil); err != nil {
		t.Errorf("nil emitter returned %v", err)
	}
	if got := emitter.Count(); got != 0 {
		t.Errorf("nil emitter counted %d", got)
	}
	if got := emitter.JobID(); got != "" {
		t.Errorf("nil emitter job id = %q", got)
	}
}

// An emitter with no stream is the default in the run path; it must be free
// and silent rather than an error on every event.
func TestEmitterWithNoWriterIsSilent(t *testing.T) {
	emitter := NewEmitter("job-1", nil, fixedClock())
	if err := emitter.Emit(TypeRunStarted, "r", nil); err != nil {
		t.Errorf("emit with no writer returned %v", err)
	}
	if got := emitter.Count(); got != 0 {
		t.Errorf("count = %d, want 0: nothing was emitted", got)
	}
}

func TestCountTracksEmissions(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewEmitter("job-1", &buf, fixedClock())
	for i := 0; i < 3; i++ {
		_ = emitter.Emit(TypeRunStarted, "r", nil)
	}
	if got := emitter.Count(); got != 3 {
		t.Errorf("count = %d, want 3", got)
	}
}
