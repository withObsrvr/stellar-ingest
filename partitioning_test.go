package main

// Rule 6 of the Bronze storage contract: 10,000-ledger ledger_range buckets.
// The flusher has always applied it; the loader never did, so a catalog the
// loader creates -- which is what a re-ingest produces -- came out
// unpartitioned. A validation run showed distinct_partitions = 0 on all 17
// tables that had files.
//
// Pruning still worked, via DuckLake's per-file min/max statistics, which is
// why this went unnoticed. Statistics are not a layout: they prune when the
// optimizer derives a usable filter, where a partition prunes structurally and
// can be operated on as a unit.

import (
	"strings"
	"testing"
)

// Every Bronze table carries ledger_range, so every one is partitioned. The
// list is derived from the schema rather than written down, and this is the
// check that the derivation agrees with the schema.
func TestEveryBronzeTableIsPartitionable(t *testing.T) {
	tables := partitionableTables(embeddedBronzeSchema)

	if len(tables) != 21 {
		t.Errorf("partitionableTables found %d tables, Bronze has 21:\n  %s",
			len(tables), strings.Join(tables, "\n  "))
	}

	// Cross-check against the tables the schema actually declares, so this
	// cannot pass by finding 21 of the wrong things.
	var declared int
	for _, stmt := range splitSQLStatements(embeddedBronzeSchema) {
		if _, _, ok := parseCreateTable(stmt); ok {
			declared++
		}
	}
	if declared != len(tables) {
		t.Errorf("the schema declares %d tables but only %d carry %s; a table without it is "+
			"never partitioned and nothing says so", declared, len(tables), duckLakePartitionColumn)
	}
}

// The parser underneath it. A CREATE TABLE it fails to read is a table that
// silently goes unpartitioned, so its own failure modes are worth pinning.
func TestParseCreateTable(t *testing.T) {
	name, cols, ok := parseCreateTable(`CREATE TABLE IF NOT EXISTS lake.bronze_store.ledgers_row_v2 (
    sequence BIGINT,
    ledger_hash TEXT,
    ledger_range BIGINT
)`)
	if !ok {
		t.Fatal("failed to parse a well-formed CREATE TABLE")
	}
	if name != "ledgers_row_v2" {
		t.Errorf("name = %q, want ledgers_row_v2 (the qualifier must be stripped)", name)
	}
	if len(cols) != 3 || cols[0] != "sequence" || cols[2] != "ledger_range" {
		t.Errorf("columns = %v", cols)
	}

	if _, _, ok := parseCreateTable(`CREATE OR REPLACE VIEW bronze.x AS SELECT 1`); ok {
		t.Error("a view parsed as a table")
	}
	if _, _, ok := parseCreateTable(`SELECT 1`); ok {
		t.Error("a bare SELECT parsed as a table")
	}
}

// A table missing the column must be reported rather than quietly skipped.
func TestTablesWithoutTheColumnAreNotPartitioned(t *testing.T) {
	schema := `
CREATE TABLE IF NOT EXISTS bronze_store.has_it (
    a BIGINT,
    ledger_range BIGINT
);
CREATE TABLE IF NOT EXISTS bronze_store.lacks_it (
    a BIGINT,
    b TEXT
);`
	got := partitionableTables(schema)
	if len(got) != 1 || got[0] != "has_it" {
		t.Errorf("got %v, want [has_it]", got)
	}
}

// An empty result must be an error, not a no-op: it means the parser stopped
// matching the schema, and the symptom would be a silently unpartitioned lake.
func TestSetPartitioningRejectsAnEmptyTableList(t *testing.T) {
	p := &DuckLakePusher{config: DuckLakeConfig{
		CatalogName: "lake", SchemaName: "bronze_store", ViewSchemaName: "bronze",
	}}
	if err := p.setPartitioning(nil, nil); err == nil {
		t.Error("an empty table list was accepted; a broken parser would read as success")
	}
}
