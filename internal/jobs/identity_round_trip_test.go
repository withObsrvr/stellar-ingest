package jobs

// The round trip over real fixture hashes lives here rather than in domain,
// because it needs eventsWithTransactions -- which drives a real extraction.
// A test helper cannot cross a package boundary, and the encoding is only
// interesting over real digests. domain keeps the pure rejection test, which
// needs nothing.

import (
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

// Every transaction hash in the fixture corpus must survive the trip into
// bytes and back out through the view's expression.
func TestTransactionHashRoundTripsThroughBytes(t *testing.T) {
	if testing.Short() {
		t.Skip("extracts every fixture; skipped under -short")
	}

	_, txs, _ := eventsWithTransactions(t)
	if len(txs) == 0 {
		t.Fatal("no fixture transactions; this test is verifying nothing")
	}

	var bad, empty int
	for _, tx := range txs {
		raw := domain.DecodeHashBytes(tx.TransactionHash)
		if len(raw) == 0 {
			empty++
			continue
		}
		if len(raw) != domain.HashBytesLen {
			t.Errorf("%s decoded to %d bytes, want %d", tx.TransactionHash, len(raw), domain.HashBytesLen)
		}
		// Exactly what bronze.transactions_row_v2 computes.
		if got := domain.RenderHashBytes(raw); got != strings.ToLower(tx.TransactionHash) {
			if bad < 3 {
				t.Errorf("round trip changed the hash: stored %q, view renders %q",
					tx.TransactionHash, got)
			}
			bad++
		}
	}
	if empty > 0 {
		t.Errorf("%d of %d transaction hashes failed to decode and would be stored as null",
			empty, len(txs))
	}
	if bad > 0 {
		t.Errorf("%d of %d hashes do not round trip", bad, len(txs))
	}
	t.Logf("%d transaction hashes round trip exactly, 32 bytes each", len(txs)-empty-bad)
}
