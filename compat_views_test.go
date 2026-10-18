package main

// The presentation views are the whole compatibility story for the storage
// contract: they are what lets Bronze drop ~2.86 TB without any of the 65
// identity call sites across six services moving. If a view's column set,
// column order, or rendered value differs from what the table held before,
// consumers break on the re-ingest -- and the re-ingest rewrites every file,
// so there is nothing to roll back to.
//
// testdata/compat/legacy_columns.json is the column order captured from the
// schema before the storage changes. Asserting against a captured record
// rather than against the current schema is the point: the current schema is
// what changed.
//
// Run against plain DuckDB rather than DuckLake. The SQL is identical and
// this keeps the test hermetic -- no extension download, no catalog, no
// credentials. What DuckLake adds is persistence of the views across
// processes, which is a property of DuckLake rather than of this SQL.

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"
)

// compatLegacyColumns is the pre-change column order, by table.
func compatLegacyColumns(t *testing.T) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile("testdata/compat/legacy_columns.json")
	if err != nil {
		t.Fatalf("read captured legacy column order: %v", err)
	}
	var out map[string][]string
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse captured legacy column order: %v", err)
	}
	if len(out) != 21 {
		t.Fatalf("captured %d tables, Bronze has 21; the record is stale", len(out))
	}
	return out
}

// buildCompatSchema creates the physical tables and the views over them,
// using the production rewrite and statement splitter rather than a copy.
func buildCompatSchema(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// "memory" is the catalog name DuckDB gives an in-memory database, so the
	// production three-part qualification works unchanged.
	p := &DuckLakePusher{config: DuckLakeConfig{
		CatalogName:    "memory",
		SchemaName:     "bronze_store",
		ViewSchemaName: "bronze",
	}}

	for _, schema := range []string{"bronze_store", "bronze"} {
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS memory." + schema); err != nil {
			t.Fatalf("create schema %s: %v", schema, err)
		}
	}
	for _, stmt := range splitSQLStatements(p.qualifySchemaSQL(embeddedBronzeSchema)) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create table: %v\nstatement: %s", err, stmt)
		}
	}
	// Views are not tolerated as warnings: a view that fails to create is a
	// consumer reading nothing.
	for _, stmt := range splitSQLStatements(p.qualifySchemaSQL(embeddedBronzeViews)) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create view: %v\nstatement: %s", err, stmt)
		}
	}
	return db
}

// Every one of the 21 tables must be readable under the schema consumers
// already use, with exactly the columns it had before, in the same order.
// Order matters because a consumer doing SELECT * reads by position.
func TestEveryViewReproducesTheLegacyColumnOrder(t *testing.T) {
	db := buildCompatSchema(t)
	legacy := compatLegacyColumns(t)

	for table, want := range legacy {
		t.Run(table, func(t *testing.T) {
			rows, err := db.Query("SELECT column_name FROM (DESCRIBE memory.bronze." + table + ")")
			if err != nil {
				t.Fatalf("describe view: %v", err)
			}
			defer rows.Close()

			var got []string
			for rows.Next() {
				var c string
				if err := rows.Scan(&c); err != nil {
					t.Fatal(err)
				}
				got = append(got, c)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}

			if len(got) != len(want) {
				t.Fatalf("view has %d columns, the table had %d\n got: %v\nwant: %v",
					len(got), len(want), got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("column %d is %q, was %q", i, got[i], want[i])
				}
			}
		})
	}
}

// The three columns that are no longer stored have to come back out of the
// views byte-identical, because consumers compare and join on them.
func TestViewsRenderTheRemovedIdentityColumns(t *testing.T) {
	db := buildCompatSchema(t)

	const (
		hash      = "a2069f36e0e430c44cf60d416ca2a8679155a28f695f98d959772dc103294f52"
		txID      = int64(264141029869883392)
		partition = int64(6436)
		eventIdx  = 118
	)

	for _, stmt := range []string{
		`INSERT INTO memory.bronze_store.transactions_row_v2 (transaction_hash, transaction_id, ledger_range)
		 VALUES (from_hex('` + hash + `'), ?, ?)`,
		`INSERT INTO memory.bronze_store.contract_events_stream_v1 (transaction_id, event_index, ledger_range)
		 VALUES (?, ` + "118" + `, ?)`,
		`INSERT INTO memory.bronze_store.operations_row_v2 (transaction_id, ledger_range)
		 VALUES (?, ?)`,
	} {
		if _, err := db.Exec(stmt, txID, partition); err != nil {
			t.Fatalf("insert: %v\nstatement: %s", err, stmt)
		}
	}

	t.Run("transactions_row_v2.transaction_hash", func(t *testing.T) {
		var got string
		if err := db.QueryRow(`SELECT transaction_hash FROM memory.bronze.transactions_row_v2`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != hash {
			t.Errorf("view renders %q, stored hash is %q", got, hash)
		}
	})

	t.Run("operations_row_v2.transaction_hash", func(t *testing.T) {
		var got string
		if err := db.QueryRow(`SELECT transaction_hash FROM memory.bronze.operations_row_v2`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != hash {
			t.Errorf("view renders %q, want %q", got, hash)
		}
	})

	t.Run("contract_events_stream_v1.event_id and transaction_hash", func(t *testing.T) {
		var gotID, gotHash string
		if err := db.QueryRow(
			`SELECT event_id, transaction_hash FROM memory.bronze.contract_events_stream_v1`,
		).Scan(&gotID, &gotHash); err != nil {
			t.Fatal(err)
		}
		// Exactly the format stellar-extract produced: "%s:%d".
		if want := hash + ":118"; gotID != want {
			t.Errorf("event_id renders %q, want %q", gotID, want)
		}
		if gotHash != hash {
			t.Errorf("transaction_hash renders %q, want %q", gotHash, hash)
		}
	})
	_ = eventIdx
}

// A child row whose parent has not arrived yet must stay visible.
//
// The two sides do not advance together: the flusher keeps a per-table
// checkpoint, so contract_events can be flushed past the point
// transactions_row_v2 has reached, and a child row can genuinely have no
// parent row for a while. Under an inner join that row is absent and nothing
// says so. Under a left join it is present with a null transaction_hash --
// wrong in a way somebody can see. The same applies to a child whose parent
// sits in another partition, which the join also requires.
func TestChildRowsSurviveAParentThatHasNotArrived(t *testing.T) {
	db := buildCompatSchema(t)

	// An event whose parent transaction is nowhere in the store.
	if _, err := db.Exec(`INSERT INTO memory.bronze_store.contract_events_stream_v1
		(transaction_id, event_index, ledger_range) VALUES (99, 0, 6437)`); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM memory.bronze.contract_events_stream_v1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the view returned %d rows; an orphaned child must stay visible rather than "+
			"disappearing until its parent is flushed", n)
	}

	var hash, eventID *string
	if err := db.QueryRow(
		`SELECT transaction_hash, event_id FROM memory.bronze.contract_events_stream_v1`,
	).Scan(&hash, &eventID); err != nil {
		t.Fatal(err)
	}
	if hash != nil {
		t.Errorf("transaction_hash = %q, want null: there is no parent to render it from", *hash)
	}
	if eventID != nil {
		t.Errorf("event_id = %q, want null", *eventID)
	}
}

// The partition equality is still part of the join, so a parent in a
// different partition does not satisfy it. Pinned because the join would
// otherwise quietly start matching across partitions if someone simplified
// it, and because the pruning argument depends on the clause being there.
func TestChildRowsDoNotMatchAParentInAnotherPartition(t *testing.T) {
	db := buildCompatSchema(t)

	const hash = "b2069f36e0e430c44cf60d416ca2a8679155a28f695f98d959772dc103294f52"
	if _, err := db.Exec(`INSERT INTO memory.bronze_store.transactions_row_v2
		(transaction_hash, transaction_id, ledger_range) VALUES (from_hex(?), 99, 6436)`, hash); err != nil {
		t.Fatal(err)
	}
	// Same transaction_id, a different partition than the parent.
	if _, err := db.Exec(`INSERT INTO memory.bronze_store.contract_events_stream_v1
		(transaction_id, event_index, ledger_range) VALUES (99, 0, 6437)`); err != nil {
		t.Fatal(err)
	}

	var rendered *string
	if err := db.QueryRow(
		`SELECT transaction_hash FROM memory.bronze.contract_events_stream_v1`,
	).Scan(&rendered); err != nil {
		t.Fatal(err)
	}
	if rendered != nil {
		t.Errorf("a parent in another partition was joined anyway, rendering %q; the partition "+
			"equality is not doing what this test documents", *rendered)
	}
}
