package main

// Test-support helpers for the golden Bronze snapshot suite. These live in a
// non-test file so they can be reused by the fixture regeneration tool.

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// nonDeterministicColumns are Bronze columns whose values change between runs
// over identical ledger input. They are excluded from golden hashes and dumps.
//
// ingestion_timestamp, created_at and updated_at are wall-clock values stamped
// at extraction time. version_label carries the build's -ldflags version.
// era_id is operator-supplied via --era-id.
//
// Excluding them here is a workaround, not an endorsement: a Bronze row that
// cannot be reproduced from the same ledger is a data-model problem worth
// deciding on separately. See docs/ticket-02-progress.md.
var nonDeterministicColumns = map[string]bool{
	"ingestion_timestamp": true,
	"created_at":          true,
	"updated_at":          true,
	"version_label":       true,
	"era_id":              true,
}

// provenanceTables are output directories that record how a run happened
// rather than what the ledger contained. Every column is run-specific
// (attempt ids, timestamps, host details), so they are excluded from golden
// comparison entirely instead of being column-filtered.
var provenanceTables = map[string]bool{
	"_meta_lineage": true,
}

// TableDigest is the golden record for a single Bronze table.
type TableDigest struct {
	RowCount    int64    `json:"row_count"`
	ContentHash string   `json:"content_hash"`
	Columns     []string `json:"columns"`
}

// FixtureDigest is the golden record for one fixture: every table it emitted.
//
// Tables are keyed by output directory name. A table absent from this map was
// not emitted at all, which is deliberately distinct from a table present with
// RowCount 0 — the writer creates no directory for a table with no rows, so
// "absent" and "empty" would otherwise be indistinguishable and a regression
// that silently stopped emitting a table would read as a pass.
type FixtureDigest struct {
	Fixture string                 `json:"fixture"`
	Tables  map[string]TableDigest `json:"tables"`
}

// findAttemptDir resolves the timestamped attempt directory the orchestrator
// writes into. The name embeds a UTC timestamp and a random id, so it cannot
// be predicted and must be discovered.
func findAttemptDir(outputDir string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(outputDir, "attempts", "*", "bronze"))
	if err != nil {
		return "", err
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no attempt directory under %s", outputDir)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("expected exactly one attempt directory under %s, found %d", outputDir, len(matches))
	}
}

// listTableDirs returns the emitted table directory names, sorted.
func listTableDirs(bronzeDir string) ([]string, error) {
	entries, err := os.ReadDir(bronzeDir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// parquetColumns returns the column names of a Parquet glob, in file order.
func parquetColumns(db *sql.DB, glob string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet(%s)", quoteLiteral(glob)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var names []string
	for rows.Next() {
		scan := make([]any, len(cols))
		holders := make([]sql.NullString, len(cols))
		for i := range scan {
			scan[i] = &holders[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return nil, err
		}
		names = append(names, holders[0].String)
	}
	return names, rows.Err()
}

// digestTable computes the row count and an order-independent content hash for
// one Bronze table.
//
// The hash is built from a per-row MD5 of the row rendered as JSON, then
// aggregated over the row hashes in sorted order. Sorting the hashes rather
// than the rows makes the result independent of Parquet row ordering, which is
// not a contract and varies with worker count and shard boundaries. Rendering
// as JSON rather than concatenating values means a column rename or a type
// change also moves the hash, so schema drift is caught alongside value drift.
func digestTable(db *sql.DB, glob string) (TableDigest, error) {
	all, err := parquetColumns(db, glob)
	if err != nil {
		return TableDigest{}, fmt.Errorf("describe %s: %w", glob, err)
	}

	var kept []string
	for _, c := range all {
		if !nonDeterministicColumns[strings.ToLower(c)] {
			kept = append(kept, c)
		}
	}
	if len(kept) == 0 {
		return TableDigest{}, fmt.Errorf("every column of %s is non-deterministic", glob)
	}

	projection := make([]string, len(kept))
	for i, c := range kept {
		projection[i] = quoteIdent(c)
	}

	query := fmt.Sprintf(`
		SELECT count(*) AS n,
		       coalesce(md5(string_agg(h, '' ORDER BY h)), '') AS content_hash
		FROM (
		  SELECT md5(CAST(to_json(t) AS VARCHAR)) AS h
		  FROM (SELECT %s FROM read_parquet(%s)) t
		)`,
		strings.Join(projection, ", "), quoteLiteral(glob))

	var d TableDigest
	if err := db.QueryRow(query).Scan(&d.RowCount, &d.ContentHash); err != nil {
		return TableDigest{}, fmt.Errorf("digest %s: %w", glob, err)
	}
	d.Columns = kept
	return d, nil
}

// digestFixtureOutput digests every table emitted under a bronze directory.
func digestFixtureOutput(db *sql.DB, bronzeDir, fixtureName string) (FixtureDigest, error) {
	tables, err := listTableDirs(bronzeDir)
	if err != nil {
		return FixtureDigest{}, err
	}

	out := FixtureDigest{Fixture: fixtureName, Tables: map[string]TableDigest{}}
	for _, t := range tables {
		if provenanceTables[t] {
			continue
		}
		glob := filepath.Join(bronzeDir, t, "**", "*.parquet")
		d, err := digestTable(db, glob)
		if err != nil {
			return FixtureDigest{}, err
		}
		out.Tables[t] = d
	}
	return out, nil
}

// quoteLiteral renders a SQL string literal.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// quoteIdent renders a SQL identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
