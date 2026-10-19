package cli

// Rendering a result.
//
// §2 puts this in the adapter: an operation returns a typed RunResult and
// this decides whether it becomes a human table, a JSON document or a line
// of NDJSON. §6's layout says the same -- "internal/cli # flags, parsing,
// rendering only".
//
// The flag has accepted json and ndjson since --output changed meaning;
// nothing rendered them, because there was no result to render. A run's
// outcome existed only as prose on stdout, so the only way to learn that a
// chunk pushed 9,348,037 rows was to parse a sentence.

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/withObsrvr/stellar-ingest/internal/app"
	"github.com/withObsrvr/stellar-ingest/internal/exit"
)

// RenderResult writes a result in the requested format.
//
// A nil result writes nothing: operations that fail return no result, and a
// caller should see the error rather than an empty table claiming success.
func RenderResult(w io.Writer, format OutputFormat, result *app.RunResult) error {
	if result == nil {
		return nil
	}
	switch format {
	case FormatJSON:
		return renderJSON(w, result)
	case FormatNDJSON:
		return renderNDJSON(w, result)
	case FormatTable:
		return renderTable(w, result)
	default:
		return exit.Usagef("unknown output format %q; want one of %s", format, OutputFormatNames())
	}
}

// renderJSON writes one indented document. Indented because a single result
// is usually read by a person even when it is parsed by a machine, and jq
// does not care either way.
func renderJSON(w io.Writer, result *app.RunResult) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// renderNDJSON writes one compact line.
//
// A run produces one result, so NDJSON here is a single line -- which is
// exactly what makes it appendable: a thousand-chunk backfill writing one
// line per chunk gives a file that streams, and `jq -s` still reads the lot.
func renderNDJSON(w io.Writer, result *app.RunResult) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(result)
}

// renderTable writes the human form: the default, and the only one anybody
// got before this.
func renderTable(w io.Writer, result *app.RunResult) error {
	var b strings.Builder

	row := func(label string, format string, args ...any) {
		fmt.Fprintf(&b, "%-24s %s\n", label, fmt.Sprintf(format, args...))
	}

	row("Operation", "%s", result.Operation)
	row("Outcome", "%s", result.Outcome)
	row("Version", "%s", result.Version)
	row("Duration", "%s", formatMS(result.DurationMS))

	if r := result.Range; r != nil {
		row("Ledger range", "%d - %d (%d ledgers)", r.Start, r.End, r.Ledgers)
		if r.Frozen {
			row("Range frozen", "yes (%d rows)", r.Rows)
		}
	}
	if e := result.Extract; e != nil {
		row("Extracted to", "%s", e.OutputDir)
		row("Extract throughput", "%.1f ledgers/sec in %s", e.Throughput, formatMS(e.DurationMS))
		if e.CallGraphEdgesSkipped > 0 {
			row("Call-graph edges skipped", "%d", e.CallGraphEdgesSkipped)
		}
	}
	if v := result.Validate; v != nil {
		row("Validation", "%d passed, %d failed", v.Passed, v.Failed)
	}
	if d := result.DuckLake; d != nil {
		row("DuckLake rows", "%d in %s", d.TotalRows, formatMS(d.DurationMS))
		row("DuckLake path", "%s", d.DataPath)
		// Sorted, so two runs of the same range render identically and a
		// diff between them means something.
		names := make([]string, 0, len(d.RowCounts))
		width := 0
		for name := range d.RowCounts {
			names = append(names, name)
			if len(name) > width {
				width = len(name)
			}
		}
		sort.Strings(names)
		// Aligned among themselves rather than against the outer gutter:
		// table names are longer than any label above them, so sharing the
		// outer width pushed every count out of its column.
		for _, name := range names {
			fmt.Fprintf(&b, "  %-*s  %d\n", width, name, d.RowCounts[name])
		}
	}
	if h := result.HotBuffer; h != nil {
		state := "failed"
		if h.Loaded {
			state = "loaded"
		}
		row("Hot buffer", "%s (%d tail ledgers) in %s", state, h.TailLedgers, formatMS(h.DurationMS))
	}
	if c := result.Chunks; c != nil {
		// Built and skipped stay apart: "5/5 succeeded in 87ms" was five
		// chunks skipping, which read as a triumph.
		row("Chunks", "%d built, %d skipped, %d failed of %d",
			c.Built, c.Skipped, c.Failed, c.Total)
		row("Chunk size", "%d (concurrency %d)", c.Size, c.Concurrency)
	}
	if r := result.Repair; r != nil {
		row("Repair", "dry_run=%t candidates=%d decoded=%d updated=%d skipped=%d",
			r.DryRun, r.Candidates, r.Decoded, r.Updated, r.SkippedRows)
	}
	if result.Plan != "" {
		b.WriteString("\n")
		b.WriteString(result.Plan)
	}

	_, err := io.WriteString(w, b.String())
	return err
}

// formatMS renders a millisecond count the way a person reads a duration.
func formatMS(ms int64) string {
	switch {
	case ms < 1000:
		return fmt.Sprintf("%dms", ms)
	case ms < 60_000:
		return fmt.Sprintf("%.1fs", float64(ms)/1000)
	default:
		return fmt.Sprintf("%dm%ds", ms/60_000, (ms%60_000)/1000)
	}
}
