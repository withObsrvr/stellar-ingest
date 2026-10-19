package jobs

import (
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

func TestOnlyTablesSelectsExtractorBySourceOrDuckLakeName(t *testing.T) {
	for _, selected := range []string{"contract_events", "contract_events_stream_v1"} {
		t.Run(selected, func(t *testing.T) {
			worker := &Worker{config: domain.OrchestratorConfig{OnlyTables: domain.ParseCSVSet(selected)}}
			if !worker.shouldExtractTable("contract_events") {
				t.Fatalf("contract_events should be selected by %q", selected)
			}
			if worker.shouldExtractTable("transactions") {
				t.Fatalf("transactions should not be selected by %q", selected)
			}
		})
	}
}
