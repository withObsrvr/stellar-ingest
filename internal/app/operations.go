package app

// The named use cases, in the vocabulary §9 of the CLI Engineering Standard
// proposes: extract, validate, load, run.
//
// Each is a preset over RunRequest rather than a separate pipeline, and that
// is deliberate. The flat form has always performed one sequence -- acquire,
// extract, push, populate -- selected by flags, and splitting it into four
// independent pipelines would be a behavioural change rather than a surface
// change. What these add is intent: a caller says what it wants done instead
// of assembling a combination of booleans, and a reader of a script can see
// which operation a command performs.
//
// Narrowing each operation's accepted inputs is the obvious next step and is
// deliberately not taken here: deciding which of 45 flags belongs to which
// verb is a compatibility decision, and getting it wrong silently ignores a
// flag a deployed job passes. The flags stay shared until each operation has
// a reason to reject one.

import (
	"context"

	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// Extract acquires ledgers and writes the 21 Bronze datasets as Parquet. It
// does not push to DuckLake or populate the hot buffer.
func Extract(ctx context.Context, req RunRequest, emitter *status.Emitter) error {
	req.RunDuckLake = false
	req.PgHost = ""
	return Run(ctx, req, emitter)
}

// Load pushes an existing output directory into DuckLake. Extraction is
// skipped, which the engine decides by finding no source to read from.
func Load(ctx context.Context, req RunRequest, emitter *status.Emitter) error {
	req.Bucket = ""
	req.RunDuckLake = true
	return Run(ctx, req, emitter)
}

// Validate checks an existing output directory and reports, without pushing
// anywhere or writing anything new.
func Validate(ctx context.Context, req RunRequest, emitter *status.Emitter) error {
	req.Bucket = ""
	req.RunValidate = true
	req.RunDuckLake = false
	req.PgHost = ""
	return Run(ctx, req, emitter)
}

// Repair backfills missing Bronze contract balance fields in place. It is the
// one operation that was already a mode switch rather than a phase, and it
// returns without extracting.
func Repair(ctx context.Context, req RunRequest, emitter *status.Emitter) error {
	req.EnrichContractBalances = true
	return Run(ctx, req, emitter)
}
