package ingest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// The mapping entry point can run from decoded XDR without a Worker,
// destination, checkpoint, or CLI. Full 21-table output is pinned by goldens.
func TestExtractBronzeLedgerWithoutWorker(t *testing.T) {
	meta, _ := loadFixtureMeta(t, "pubnet-auto-restore-p25")
	path := materializeLedgers(t, filepath.Join("testdata", "fixtures", meta.Name), t.TempDir())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	rows, err := extractBronzeLedger(LedgerMeta{
		LCM: lcm, LedgerSequence: meta.StartLedger,
		ClosedAt: time.Unix(0, 0), LedgerRange: meta.StartLedger,
	}, meta.NetworkPassphrase, map[string]bool{"restored_keys": true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.RestoredKeys) != 22 {
		t.Fatalf("restored keys = %d, want 22", len(rows.RestoredKeys))
	}
	if len(rows.Transactions) != 0 || len(rows.Trades) != 0 || len(rows.Ledgers) != 0 {
		t.Fatal("unselected tables were extracted")
	}
}
