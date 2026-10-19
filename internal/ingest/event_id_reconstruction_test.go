package ingest

// Rule 2 of the storage contract drops contract_events_stream_v1.event_id --
// 800 GB, the largest single column in the lake -- on the claim that the row
// can reproduce it from transaction_hash and event_index. That claim is a
// string format in stellar-extract, not something the schema enforces, so it
// is checked here against real fixture events before the column is removed.
//
// If this is off by anything -- a different index, zero padding, a separator
// -- then dropping the column loses data that cannot be recovered, because
// the re-ingest rewrites the files.

import (
	"fmt"
	"strings"
	"testing"
)

func TestEventIDIsExactlyReconstructible(t *testing.T) {
	if testing.Short() {
		t.Skip("extracts every fixture; skipped under -short")
	}

	events, _, _ := eventsWithTransactions(t)
	if len(events) == 0 {
		t.Fatal("no fixture produced contract events; this test is verifying nothing")
	}

	var bad int
	for _, ev := range events {
		// Exactly the expression the compatibility view will use:
		//   lower(hex(transaction_hash)) || ':' || event_index
		want := fmt.Sprintf("%s:%d", strings.ToLower(ev.TransactionHash), ev.EventIndex)
		if ev.EventID != want {
			if bad < 3 {
				t.Errorf("event_id = %q, view expression yields %q", ev.EventID, want)
			}
			bad++
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d event_ids are not reproducible from their row; rule 2 cannot drop "+
			"this column without losing data", bad, len(events))
	}
	t.Logf("all %d event_ids reproduce exactly from transaction_hash and event_index", len(events))
}
