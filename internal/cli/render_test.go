package cli

// Golden tests for the three output formats.
//
// §10's release gate: "JSON responses and NDJSON event format covered by
// golden tests." They exist because the schema is a contract -- a consumer
// pins stellar-ingest.run/v1 and a renamed field breaks it silently, which
// is the failure mode a golden file turns into a diff.
//
// The fixture is fixed, including the timestamp: a result that embeds
// time.Now() cannot be compared to anything, and making the clock an input
// is cheaper than teaching every assertion to ignore three fields.
//
// Regenerate with: go test ./internal/cli/ -update

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/app"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func goldenPath(name string) string {
	return filepath.Join("..", "..", "testdata", "golden", "render", name)
}

func checkGolden(t *testing.T, name, got string) {
	t.Helper()
	path := goldenPath(name)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run go test ./internal/cli/ -update to create it)", path, err)
	}
	if got != string(want) {
		t.Errorf("%s differs from the golden file.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}

// fullResult exercises every optional section at once, so a field that stops
// being rendered shows up in the diff rather than in production.
func fullResult() *app.RunResult {
	return &app.RunResult{
		Schema:     app.SchemaRun,
		Operation:  "run",
		Outcome:    app.OutcomeSucceeded,
		Version:    "1e2b97a",
		StartedAt:  time.Date(2026, 10, 19, 16, 39, 18, 0, time.UTC),
		DurationMS: 112540,
		Range:      &app.RangeResult{Start: 64000000, End: 64000999, Ledgers: 1000, Frozen: true, Rows: 10123654},
		Extract: &app.ExtractResult{
			OutputDir:             "/data/output/attempts/20261019T163918Z-abc123",
			Ledgers:               1000,
			DurationMS:            56000,
			Throughput:            17.9,
			CallGraphEdgesSkipped: 4,
		},
		Validate: &app.ValidateResult{Passed: 3, Failed: 0},
		DuckLake: &app.DuckLakeResult{
			DataPath:   "s3://obsrvr-lake-mainnet/_validation_v4/",
			TotalRows:  10123654,
			DurationMS: 48000,
			RowCounts: map[string]int64{
				"transactions_row_v2":       385984,
				"contract_events_stream_v1": 4891233,
				"ledgers_row_v2":            1000,
			},
		},
		HotBuffer: &app.HotBufferResult{Host: "db.example", TailLedgers: 100000, DurationMS: 2100, Loaded: true},
	}
}

func TestGoldenTable(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderResult(&buf, FormatTable, fullResult()); err != nil {
		t.Fatalf("render: %v", err)
	}
	checkGolden(t, "run_table.txt", buf.String())
}

func TestGoldenJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderResult(&buf, FormatJSON, fullResult()); err != nil {
		t.Fatalf("render: %v", err)
	}
	checkGolden(t, "run_json.json", buf.String())
}

func TestGoldenNDJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderResult(&buf, FormatNDJSON, fullResult()); err != nil {
		t.Fatalf("render: %v", err)
	}
	checkGolden(t, "run_ndjson.ndjson", buf.String())
}

func TestGoldenChunkedBackfill(t *testing.T) {
	result := &app.RunResult{
		Schema: app.SchemaRun, Operation: "run", Outcome: app.OutcomeSucceeded,
		Version: "1e2b97a", StartedAt: time.Date(2026, 10, 19, 16, 39, 18, 0, time.UTC),
		DurationMS: 112540,
		Range:      &app.RangeResult{Start: 64000000, End: 64002000, Ledgers: 2001},
		Chunks:     &app.ChunksResult{Total: 3, Built: 3, Skipped: 0, Failed: 0, Size: 1000, Concurrency: 2, DurationMS: 112540},
	}
	var buf bytes.Buffer
	if err := RenderResult(&buf, FormatTable, result); err != nil {
		t.Fatalf("render: %v", err)
	}
	checkGolden(t, "chunks_table.txt", buf.String())
}

func TestGoldenSkipped(t *testing.T) {
	result := &app.RunResult{
		Schema: app.SchemaRun, Operation: "run", Outcome: app.OutcomeSkipped,
		Version: "1e2b97a", StartedAt: time.Date(2026, 10, 19, 16, 39, 18, 0, time.UTC),
		DurationMS: 42,
		Range:      &app.RangeResult{Start: 64000000, End: 64000999, Ledgers: 1000},
	}
	var buf bytes.Buffer
	if err := RenderResult(&buf, FormatTable, result); err != nil {
		t.Fatalf("render: %v", err)
	}
	checkGolden(t, "skipped_table.txt", buf.String())
}

// NDJSON must be exactly one line, or it is not NDJSON and appending a
// second result produces a file nothing can read line by line.
func TestNDJSONIsOneLine(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderResult(&buf, FormatNDJSON, fullResult()); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Error("NDJSON line does not end in a newline, so appending the next one concatenates them")
	}
	if got := strings.Count(out, "\n"); got != 1 {
		t.Errorf("NDJSON produced %d lines, want 1", got)
	}
}

// Appending results must give a file that reads line by line, which is the
// whole point of the format for a thousand-chunk backfill.
func TestNDJSONAppendsCleanly(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		if err := RenderResult(&buf, FormatNDJSON, fullResult()); err != nil {
			t.Fatalf("render: %v", err)
		}
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines from 3 results, want 3", len(lines))
	}
	for i, line := range lines {
		var decoded app.RunResult
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Errorf("line %d is not valid JSON: %v", i, err)
		}
	}
}

// The schema travels in the payload so a consumer can tell it is reading
// something newer rather than silently misreading a renamed field.
func TestSchemaIsCarriedInTheDocument(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderResult(&buf, FormatJSON, fullResult()); err != nil {
		t.Fatalf("render: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if got := decoded["schema"]; got != app.SchemaRun {
		t.Errorf("schema = %v, want %q", got, app.SchemaRun)
	}
}

// A failed operation returns no result; rendering nil must write nothing
// rather than an empty table that looks like success.
func TestNilResultRendersNothing(t *testing.T) {
	for _, format := range []OutputFormat{FormatTable, FormatJSON, FormatNDJSON} {
		var buf bytes.Buffer
		if err := RenderResult(&buf, format, nil); err != nil {
			t.Errorf("%s: %v", format, err)
		}
		if buf.Len() != 0 {
			t.Errorf("%s rendered %q for a nil result; a failed run must not print a result",
				format, buf.String())
		}
	}
}

func TestUnknownFormatIsRejected(t *testing.T) {
	var buf bytes.Buffer
	err := RenderResult(&buf, OutputFormat("yaml"), fullResult())
	if err == nil {
		t.Fatal("unknown format accepted")
	}
}

// Row counts come from a map, so without sorting two runs of the same range
// render differently and a diff between them means nothing.
func TestTableRowCountsAreSorted(t *testing.T) {
	var first, second bytes.Buffer
	if err := RenderResult(&first, FormatTable, fullResult()); err != nil {
		t.Fatal(err)
	}
	if err := RenderResult(&second, FormatTable, fullResult()); err != nil {
		t.Fatal(err)
	}
	if first.String() != second.String() {
		t.Error("two renders of the same result differ; map iteration order is leaking into output")
	}
}
