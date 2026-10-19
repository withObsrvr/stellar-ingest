package app

// The typed result of an operation.
//
// §3 asks for "human tables by default; --output json for single results;
// --output ndjson for event streams. Version schemas." The flag already
// accepted all three formats; nothing rendered them, because there was no
// result to render -- the run's outcome existed only as prose on stdout, so
// the only way to find out whether a chunk pushed 9,348,037 rows was to
// parse a sentence.
//
// This is the data. §2 puts the rendering in the adapter: an operation
// returns a result and internal/cli decides whether it becomes a table, a
// JSON document or a line of NDJSON. The same struct is what an API or a UI
// would serve.
//
// SCHEMA is versioned and carried in the payload. A consumer that pins
// "stellar-ingest.run/v1" can be told it is reading something newer rather
// than silently misreading a renamed field, which is what §5 means by
// "Version schemas" and what §10's compatibility gate asks to be documented.

import "time"

// SchemaRun identifies the run-result schema. Bump the version when a field
// changes meaning or disappears; adding an optional field is compatible.
const SchemaRun = "stellar-ingest.run/v1"

// Outcome is the terminal state of an operation, from §4's lifecycle
// vocabulary: "queued → running → succeeded | failed | cancelled".
//
// A result only exists for a run that finished, so queued and running never
// appear here; failed arrives as an error rather than a result, since a
// caller that ignores the error must not read a result that says "failed".
type Outcome string

const (
	// OutcomeSucceeded means every requested phase completed.
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeSkipped means there was nothing to do: the range was already
	// frozen or claimed by another run. Distinct from succeeded because
	// reporting five skipped chunks as five successes read as a triumph
	// when the backfill had not moved.
	OutcomeSkipped Outcome = "skipped"
)

// RunResult is what an operation produces.
type RunResult struct {
	Schema    string `json:"schema"`
	Operation string `json:"operation"`
	// JobID correlates this result with the event stream and with the
	// range catalog, which stores the same value as attempt_id.
	JobID     string    `json:"job_id,omitempty"`
	Outcome   Outcome   `json:"outcome"`
	Version   string    `json:"version"`
	StartedAt time.Time `json:"started_at"`
	// DurationMS is milliseconds rather than a Go duration string, so a
	// consumer can do arithmetic without parsing "1m52.54s".
	DurationMS int64 `json:"duration_ms"`

	Range *RangeResult `json:"range,omitempty"`

	Extract   *ExtractResult   `json:"extract,omitempty"`
	Validate  *ValidateResult  `json:"validate,omitempty"`
	DuckLake  *DuckLakeResult  `json:"ducklake,omitempty"`
	HotBuffer *HotBufferResult `json:"hot_buffer,omitempty"`
	Chunks    *ChunksResult    `json:"chunks,omitempty"`
	Repair    *RepairResult    `json:"repair,omitempty"`
	// Plan is the answer to --plan: what still needs building. Carried as
	// text because jobs.FormatPlan is the authority on that rendering and
	// duplicating it here would let the two drift.
	Plan string `json:"plan,omitempty"`
}

// RangeResult is the ledger range the operation covered.
type RangeResult struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
	// Ledgers is inclusive of both ends, which is how every other count in
	// this tool is reported.
	Ledgers uint32 `json:"ledgers"`
	// Frozen reports that the range was recorded as complete in the range
	// catalog. A range is frozen when the push lands, not when extraction
	// finishes.
	Frozen bool `json:"frozen"`
	// Rows is the row count the freeze recorded, zero when not frozen.
	Rows int64 `json:"rows,omitempty"`
}

// ExtractResult reports the extraction phase.
type ExtractResult struct {
	OutputDir  string  `json:"output_dir"`
	Ledgers    uint32  `json:"ledgers"`
	DurationMS int64   `json:"duration_ms"`
	Throughput float64 `json:"ledgers_per_second"`
	// CallGraphEdgesSkipped counts edges dropped for a non-contract or
	// unencodable address. Surfaced because a silent nonzero here means the
	// call graph is incomplete.
	CallGraphEdgesSkipped int64 `json:"call_graph_edges_skipped,omitempty"`
}

// ValidateResult reports the quality checks.
type ValidateResult struct {
	Passed int `json:"passed"`
	Failed int `json:"failed"`
}

// DuckLakeResult reports the push.
type DuckLakeResult struct {
	DataPath   string           `json:"data_path"`
	TotalRows  int64            `json:"total_rows"`
	RowCounts  map[string]int64 `json:"row_counts,omitempty"`
	DurationMS int64            `json:"duration_ms"`
}

// HotBufferResult reports the PostgreSQL tail load.
type HotBufferResult struct {
	Host        string `json:"host"`
	TailLedgers uint32 `json:"tail_ledgers"`
	DurationMS  int64  `json:"duration_ms"`
	// Loaded is false when the load was attempted and reported errors. The
	// run does not fail on it, so the result has to say so.
	Loaded bool `json:"loaded"`
}

// ChunksResult reports a chunked backfill.
//
// Built and skipped are separate because they mean opposite things about
// whether the backfill progressed: "5/5 succeeded in 87ms" was five chunks
// skipping, which read as a triumph.
type ChunksResult struct {
	Total       int    `json:"total"`
	Built       int    `json:"built"`
	Skipped     int    `json:"skipped"`
	Failed      int    `json:"failed"`
	Size        uint32 `json:"size"`
	Concurrency int    `json:"concurrency"`
	DurationMS  int64  `json:"duration_ms"`
}

// RepairResult reports a contract-balance enrichment.
type RepairResult struct {
	DryRun      bool  `json:"dry_run"`
	Candidates  int64 `json:"candidates"`
	Decoded     int64 `json:"decoded"`
	Updated     int64 `json:"updated"`
	SkippedRows int64 `json:"skipped"`
}

// newRunResult starts a result for an operation.
func newRunResult(operation, version string, started time.Time) *RunResult {
	return &RunResult{
		Schema:    SchemaRun,
		Operation: operation,
		Outcome:   OutcomeSucceeded,
		Version:   version,
		StartedAt: started.UTC(),
	}
}

// finish stamps the duration. Called on the way out of every path that
// returns a result, so no result can report a zero-length run.
func (r *RunResult) finish(started time.Time) *RunResult {
	if r == nil {
		return nil
	}
	r.DurationMS = time.Since(started).Milliseconds()
	return r
}
