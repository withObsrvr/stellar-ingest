package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	extract "github.com/withObsrvr/stellar-extract"
)

// This pubnet ledger restored entries automatically during contract invocation:
// a scan for RestoreFootprint operations cannot find these rows.
func TestRestoredKeysFromAuthoritativeLedgerChanges(t *testing.T) {
	meta, _ := loadFixtureMeta(t, "pubnet-auto-restore-p25")
	path := materializeLedgers(t, filepath.Join("testdata", "fixtures", meta.Name), t.TempDir())
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	in := extract.NewLedgerInput(lcm, meta.NetworkPassphrase)
	in.LedgerRange = meta.StartLedger
	rows, err := extract.ExtractRestoredKeys(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 22 {
		t.Fatalf("got %d restored keys, want 22", len(rows))
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if seen[row.KeyHash] {
			t.Fatalf("duplicate restored key %s", row.KeyHash)
		}
		seen[row.KeyHash] = true
		if row.LedgerSequence != meta.StartLedger {
			t.Fatalf("ledger = %d", row.LedgerSequence)
		}
		if row.Durability == "unknown" {
			t.Fatalf("unknown durability for %s", row.KeyHash)
		}
		if row.RestoredFromLedger != meta.StartLedger {
			t.Fatalf("restored from ledger %d", row.RestoredFromLedger)
		}
	}
}
