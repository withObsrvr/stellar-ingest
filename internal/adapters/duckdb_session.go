package adapters

// Every DuckDB connection this process opens goes through OpenDuckDB, which
// pins the session timezone to UTC.
//
// DuckDB's default TimeZone is not a constant. 1.5.4 defaults to UTC;
// 2.0.0 defaults to the host's zone, so the same query returns a different
// string on a machine set to America/New_York than on one set to UTC:
//
//	1.5.4      to_json -> {"v":"2026-01-02 03:04:05+00"}
//	2.0.0      to_json -> {"v":"2026-01-01 22:04:05-05"}
//	2.0.0+UTC  to_json -> {"v":"2026-01-02 03:04:05+00"}
//
// Bronze stores plain TIMESTAMP, which is not rendered through the session
// zone, so nothing in cold shifts. What does shift is anything read back as
// TIMESTAMP WITH TIME ZONE -- which is what Parquet timestamp columns come
// back as, since the logical type is UTC-adjusted -- and so anything that
// renders or compares one as text. The golden digests do exactly that, which
// is how this surfaced: they moved under 2.0.0 while every column digest
// still matched, meaning the data was identical and only the rendering had
// changed.
//
// Pinning it costs nothing and removes two variables at once: which DuckDB is
// linked, and which zone the host happens to be set to.

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

// duckDBSessionTimeZone is the zone every connection is pinned to. Bronze is
// UTC throughout -- closed_at comes from the ledger header's close time, which
// is a Unix timestamp -- so any other value would be a rendering that
// disagrees with the data.
const duckDBSessionTimeZone = "UTC"

// OpenDuckDB opens an in-memory DuckDB with the session timezone pinned.
func OpenDuckDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	if err := pinDuckDBSession(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// pinDuckDBSession applies the session settings to an already-open handle,
// for the callers that build their connection themselves.
func pinDuckDBSession(db *sql.DB) error {
	if _, err := db.Exec("SET TimeZone='" + duckDBSessionTimeZone + "'"); err != nil {
		return fmt.Errorf("pin duckdb session timezone to %s: %w", duckDBSessionTimeZone, err)
	}
	return nil
}
