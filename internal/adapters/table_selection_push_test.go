package adapters

// The push-side half of table selection. It reaches into DuckLakePusher's own
// configuration, so it can only live here; the worker-side half stays in jobs
// for the same reason about Worker. One file could not be inside both
// packages.

import (
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

func TestDuckLakeOnlyTablesPushesOnlySelectedTable(t *testing.T) {
	pusher := &DuckLakePusher{config: DuckLakeConfig{OnlyTables: domain.ParseCSVSet("contract_events_stream_v1")}}

	if pusher.shouldSkipTable("contract_events", "contract_events_stream_v1") {
		t.Fatal("contract_events should not be skipped")
	}
	if !pusher.shouldSkipTable("transactions", "transactions_row_v2") {
		t.Fatal("transactions should be skipped")
	}
}

func TestDuckLakeSkipTablesWinsOverOnlyTables(t *testing.T) {
	pusher := &DuckLakePusher{config: DuckLakeConfig{
		OnlyTables: domain.ParseCSVSet("contract_events_stream_v1"),
		SkipTables: domain.ParseCSVSet("contract_events"),
	}}

	if !pusher.shouldSkipTable("contract_events", "contract_events_stream_v1") {
		t.Fatal("skip table should win over selected table")
	}
}
