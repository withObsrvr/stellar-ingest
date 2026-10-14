package main

import "testing"

func TestOnlyTablesSelectsExtractorBySourceOrDuckLakeName(t *testing.T) {
	for _, selected := range []string{"contract_events", "contract_events_stream_v1"} {
		t.Run(selected, func(t *testing.T) {
			worker := &Worker{config: OrchestratorConfig{OnlyTables: parseCSVSet(selected)}}
			if !worker.shouldExtractTable("contract_events") {
				t.Fatalf("contract_events should be selected by %q", selected)
			}
			if worker.shouldExtractTable("transactions") {
				t.Fatalf("transactions should not be selected by %q", selected)
			}
		})
	}
}

func TestDuckLakeOnlyTablesPushesOnlySelectedTable(t *testing.T) {
	pusher := &DuckLakePusher{config: DuckLakeConfig{OnlyTables: parseCSVSet("contract_events_stream_v1")}}

	if pusher.shouldSkipTable("contract_events", "contract_events_stream_v1") {
		t.Fatal("contract_events should not be skipped")
	}
	if !pusher.shouldSkipTable("transactions", "transactions_row_v2") {
		t.Fatal("transactions should be skipped")
	}
}

func TestDuckLakeSkipTablesWinsOverOnlyTables(t *testing.T) {
	pusher := &DuckLakePusher{config: DuckLakeConfig{
		OnlyTables: parseCSVSet("contract_events_stream_v1"),
		SkipTables: parseCSVSet("contract_events"),
	}}

	if !pusher.shouldSkipTable("contract_events", "contract_events_stream_v1") {
		t.Fatal("skip table should win over selected table")
	}
}
