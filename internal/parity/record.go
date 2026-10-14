package parity

import (
	"fmt"
	"sort"
)

// Side names one of the two Bronze implementations under comparison.
type Side string

const (
	// SideHistorical is stellar-ingest's extractBronzeLedger path.
	SideHistorical Side = "historical"
	// SideLive is stellar-postgres-ingester's WriteBatch extraction path.
	SideLive Side = "live"
)

// AllTables is the Bronze table set Ticket 04 must account for, in the order
// the investigation matrix lists them.
//
// The harness reports on every name here even when a side emits nothing for
// it. A table missing from a report is indistinguishable from a table that
// was never checked, and the ticket's exit gate does not accept a silent
// exclusion.
var AllTables = []string{
	"transactions",
	"operations",
	"effects",
	"trades",
	"accounts",
	"offers",
	"trustlines",
	"account_signers",
	"claimable_balances",
	"liquidity_pools",
	"config_settings",
	"ttl",
	"evicted_keys",
	"contract_events",
	"contract_data",
	"contract_code",
	"native_balances",
	"restored_keys",
	"contract_creations",
	"ledgers",
	"token_transfers",
}

// DeclaredRunDependentColumns are adapter fields known in advance to carry a
// property of the run rather than of the ledger.
//
// This list cannot be derived by observation: era_id and version_label are
// fixed for the lifetime of a process, so two extractions in one run produce
// the same value and the probe in SnapshotRuns sees nothing. Everything else
// that varies per row — the wall-clock timestamps — is detected rather than
// declared, because which tables stamp a wall clock turned out not to match
// which ones the column name suggests.
//
// Excluding them is a comparison concession, not a judgment that they do not
// matter: era_id and version_label are deliberate provenance, and the
// timestamps are the reproducibility problem recorded in
// docs/ticket-02-progress.md.
var DeclaredRunDependentColumns = map[string]bool{
	"EraID":        true,
	"VersionLabel": true,
}

// TableSnapshot is one side's output for one table on one fixture.
type TableSnapshot struct {
	// Emitted distinguishes a table the side produced (possibly with zero
	// rows) from one it has no mapping for at all. The ticket requires the
	// difference to be visible: "absent" and "emitted empty" are different
	// findings with different owners.
	Emitted bool `json:"emitted"`

	RowCount int      `json:"row_count"`
	Columns  []string `json:"columns"`

	// ColumnDigests is a per-column multiset digest. It localizes a
	// difference to the column that moved without carrying row payloads: a
	// single fixture's transaction rows are mostly base64 XDR and run to tens
	// of megabytes.
	//
	// Columns observed to be non-deterministic keep their digest. Dropping
	// them here would make a column that one side stamps with a wall clock
	// look like a column the other side does not have. Declared
	// run-dependent columns are dropped, which is safe because both sides
	// declare the same list.
	ColumnDigests map[string]string `json:"column_digests,omitempty"`

	// NonDeterministicColumns were observed to change between two
	// extractions of the same authoritative XDR in the same process.
	//
	// This is measured rather than declared because the column names do not
	// predict it: created_at is the ledger close time on transactions and
	// operations, and an ingestion wall clock on a dozen other tables. A
	// hand-written exclusion list would either hide the real differences on
	// the first group or drown them in noise from the second.
	NonDeterministicColumns []string `json:"non_deterministic_columns,omitempty"`

	// TableDigest covers the whole row multiset over the compared columns.
	//
	// Per-row digests are deliberately not stored. They would dominate the
	// file — one 64-character hash per row, which is several megabytes for a
	// busy fixture — and they are not comparable across sides anyway,
	// because each side hashes over its own column set. The row-level
	// multiset diff is computed from the RowSet tier instead, projected onto
	// the columns the two sides actually share.
	TableDigest string `json:"table_digest"`
}

// Record is one side's complete output for one fixture.
type Record struct {
	CanonicalFormVersion string `json:"canonical_form_version"`
	Side                 Side   `json:"side"`
	Fixture              string `json:"fixture"`

	// Pins record the revisions the numbers came from. A discrepancy report
	// without them cannot be reproduced or re-checked after either tree
	// moves, and the handoff is explicit that branch names are not pins.
	Pins map[string]string `json:"pins"`

	// ExcludedColumns lists the declared run-dependent fields left out of
	// every table's digest, so a reader never has to guess whether a column
	// was equal or skipped. Columns detected as non-deterministic are
	// reported per table instead, in TableSnapshot.
	ExcludedColumns []string `json:"excluded_columns"`

	Tables map[string]TableSnapshot `json:"tables"`
}

// RowSet carries one side's full canonical rows for a fixture.
//
// It is kept separate from Record because the two artifacts have different
// lifetimes: a Record is small enough to commit and review, while a RowSet
// for a single busy pubnet ledger runs to tens of megabytes of base64 XDR and
// is build output. Comparison needs the Record to localize a difference and
// the RowSet only to explain one.
type RowSet struct {
	CanonicalFormVersion string           `json:"canonical_form_version"`
	Side                 Side             `json:"side"`
	Fixture              string           `json:"fixture"`
	Tables               map[string][]Row `json:"tables"`
}

// NewRecord returns an empty record with every Ticket 04 table pre-marked as
// not emitted, so a side that forgets to register a table reports as absent
// rather than vanishing from the comparison.
func NewRecord(side Side, fixture string, pins map[string]string) *Record {
	tables := make(map[string]TableSnapshot, len(AllTables))
	for _, t := range AllTables {
		tables[t] = TableSnapshot{Emitted: false}
	}
	excluded := make([]string, 0, len(DeclaredRunDependentColumns))
	for c := range DeclaredRunDependentColumns {
		excluded = append(excluded, c)
	}
	sort.Strings(excluded)

	return &Record{
		CanonicalFormVersion: CanonicalFormVersion,
		Side:                 side,
		Fixture:              fixture,
		Pins:                 pins,
		ExcludedColumns:      excluded,
		Tables:               tables,
	}
}

// AddTable snapshots one table into the record.
//
// Each argument is one extraction of the same input, as the adapter's own row
// slice; it is flattened by reflection so that neither side has to restate
// its schema by hand and drift from it. Pass two runs to have
// non-deterministic columns detected.
func (r *Record) AddTable(table string, runs ...any) error {
	if _, known := r.Tables[table]; !known {
		return fmt.Errorf("parity: %q is not one of the %d Ticket 04 tables", table, len(AllTables))
	}
	flattened := make([][]Row, len(runs))
	for i, run := range runs {
		flattened[i] = Rows(run)
	}
	r.Tables[table] = SnapshotRuns(flattened...)
	return nil
}

// Snapshot builds a TableSnapshot from a single extraction.
//
// With one run there is nothing to compare against, so no column can be
// observed to be non-deterministic. Prefer SnapshotRuns.
func Snapshot(rows []Row) TableSnapshot {
	return SnapshotRuns(rows)
}

// SnapshotRuns builds a TableSnapshot from one or more extractions of the
// same authoritative input.
//
// When more than one run is supplied, any column whose value multiset differs
// between runs is recorded as non-deterministic and left out of the table
// digest. The first run supplies the reported rows and digests; the rest are
// used only as the control.
func SnapshotRuns(runs ...[]Row) TableSnapshot {
	if len(runs) == 0 {
		return TableSnapshot{Emitted: true}
	}
	rows := runs[0]
	all := Columns(rows)

	digestsFor := func(rs []Row, cols []string) map[string]string {
		values := make(map[string][]string, len(cols))
		for _, r := range rs {
			for _, c := range cols {
				v, ok := r[c]
				if !ok {
					v = tagNull
				}
				values[c] = append(values[c], v)
			}
		}
		out := make(map[string]string, len(cols))
		for _, c := range cols {
			out[c] = DigestMultiset(values[c])
		}
		return out
	}

	// Declared run-dependent columns get no digest at all. Both sides take
	// that list from the same source file, so dropping them is symmetric and
	// cannot make a column look one-sided. Columns found non-deterministic
	// by the probe below do keep a digest, because the two sides can disagree
	// about which those are.
	digestable := make([]string, 0, len(all))
	for _, c := range all {
		if !DeclaredRunDependentColumns[c] {
			digestable = append(digestable, c)
		}
	}
	colDigests := digestsFor(rows, digestable)

	nonDeterministic := map[string]bool{}
	for _, control := range runs[1:] {
		// A row count that moves between runs is not a non-deterministic
		// column — it is a non-deterministic extractor, and silently
		// absorbing it would hide a far worse problem than a wall clock.
		if len(control) != len(rows) {
			continue
		}
		for c, d := range digestsFor(control, digestable) {
			if colDigests[c] != d {
				nonDeterministic[c] = true
			}
		}
	}

	compared := make([]string, 0, len(digestable))
	for _, c := range digestable {
		if nonDeterministic[c] {
			continue
		}
		compared = append(compared, c)
	}

	rowDigests := make([]string, 0, len(rows))
	for _, row := range rows {
		rowDigests = append(rowDigests, Digest(EncodeRow(row, compared)))
	}
	sort.Strings(rowDigests)

	observed := make([]string, 0, len(nonDeterministic))
	for c := range nonDeterministic {
		observed = append(observed, c)
	}
	sort.Strings(observed)

	return TableSnapshot{
		Emitted: true,
		// Columns is the full list including the excluded ones: the reader
		// needs to see that a side has an era_id field even though its value
		// is not compared.
		RowCount:                len(rows),
		Columns:                 all,
		ColumnDigests:           colDigests,
		NonDeterministicColumns: observed,
		TableDigest:             DigestMultiset(rowDigests),
	}
}
