package main

// DuckDB's default session timezone is not stable across versions: 1.5.4
// defaults to UTC, 2.0.0 defaults to the host's zone. Anything read back as
// TIMESTAMP WITH TIME ZONE then renders differently depending on which DuckDB
// is linked and which machine it runs on -- and a Parquet timestamp column
// comes back as exactly that, because its logical type is UTC-adjusted.
//
// Bronze is UTC throughout, so the only correct session zone is UTC.

import (
	"strings"
	"testing"
)

func TestOpenDuckDBPinsUTC(t *testing.T) {
	db, err := openDuckDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var tz string
	if err := db.QueryRow("SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
		t.Fatal(err)
	}
	// Asserted against the literal, not against duckDBSessionTimeZone. Comparing
	// the setting to the same constant used to set it passes for any value of
	// that constant, which is a test that cannot fail -- as a mutation to
	// "America/New_York" demonstrated.
	if tz != "UTC" {
		t.Errorf("session TimeZone = %q, want UTC; Bronze is UTC throughout, and rendering of "+
			"any timestamp with a zone otherwise depends on the host and the DuckDB version", tz)
	}
}

// The rendering itself, which is what actually moved.
//
// Worth knowing what this does and does not prove on the current pin: DuckDB
// 1.5.4 renders to_json of a TIMESTAMPTZ in UTC whatever the session is set
// to, so on 1.5.4 this passes even with the session pinned to another zone --
// setting the constant to America/New_York does not fail it. It has teeth
// under 2.0.0, which honours the session.
//
// The evidence that the pin is what fixes 2.0.0 is not this test but the
// golden suite: under the 2.0.0 preview every golden content hash moved while
// every column digest matched, and with the session pinned to UTC the whole
// suite passes unchanged.
func TestTimestampWithZoneRendersAsUTC(t *testing.T) {
	db, err := openDuckDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var got string
	if err := db.QueryRow(
		`SELECT CAST(to_json(t) AS VARCHAR) FROM (SELECT TIMESTAMPTZ '2026-01-02 03:04:05+00' AS v) t`,
	).Scan(&got); err != nil {
		t.Fatal(err)
	}

	const want = `{"v":"2026-01-02 03:04:05+00"}`
	if got != want {
		t.Errorf("to_json rendered %s, want %s. Under DuckDB 2.0.0 with an unpinned session this "+
			"becomes local time, which moves every golden digest without any data changing", got, want)
	}
	if strings.Contains(got, "-05") || strings.Contains(got, "-04") {
		t.Errorf("the rendering picked up a local offset: %s", got)
	}
}

// A handle somebody opened themselves can be pinned too, which is what the
// callers that build their own connection need.
func TestPinDuckDBSessionAppliesToAnOpenHandle(t *testing.T) {
	db, err := openDuckDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("SET TimeZone='America/New_York'"); err != nil {
		t.Fatal(err)
	}
	if err := pinDuckDBSession(db); err != nil {
		t.Fatal(err)
	}
	var tz string
	if err := db.QueryRow("SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
		t.Fatal(err)
	}
	if tz != duckDBSessionTimeZone {
		t.Errorf("TimeZone = %q after pinning, want %q", tz, duckDBSessionTimeZone)
	}
}
