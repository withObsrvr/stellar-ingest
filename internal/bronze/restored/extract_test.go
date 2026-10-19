package restored

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExtractAutomaticRestoreWithoutRestoreFootprint(t *testing.T) {
	path := filepath.Join("..", "..", "ingest", "testdata", "fixtures", "pubnet-auto-restore-p25", "ledgers.xdr.zst")
	compressed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	raw, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatal(err)
	}
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		t.Fatal(err)
	}
	const network = "Public Global Stellar Network ; September 2015"
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network, lcm)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var restoreOperations int
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeRestoreFootprint {
				restoreOperations++
			}
		}
	}
	if restoreOperations != 0 {
		t.Fatalf("fixture contains %d RestoreFootprint operations", restoreOperations)
	}
	rows, err := Extract(lcm, network, 61500126, time.Unix(0, 0), 61500126)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 22 {
		t.Fatalf("restored rows = %d, want 22", len(rows))
	}
	seen := make(map[string]bool)
	for _, row := range rows {
		if seen[row.KeyHash] {
			t.Fatalf("duplicate key %s", row.KeyHash)
		}
		seen[row.KeyHash] = true
		if row.Durability == "unknown" || row.RestoredFromLedger != 61500126 {
			t.Fatalf("unexpected restored row: %+v", row)
		}
	}
	// Re-marshal after decoding so the test cannot pass on a partial stream.
	encoded, err := lcm.MarshalBinary()
	if err != nil || !bytes.Equal(raw, encoded) {
		t.Fatalf("fixture XDR round-trip mismatch: %v", err)
	}
}

func TestDurability(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  xdr.LedgerKey
		want string
	}{
		{"contract code", xdr.LedgerKey{Type: xdr.LedgerEntryTypeContractCode}, "persistent"},
		{"ttl", xdr.LedgerKey{Type: xdr.LedgerEntryTypeTtl}, "persistent"},
		{"missing data", xdr.LedgerKey{Type: xdr.LedgerEntryTypeContractData}, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := durability(tc.key); got != tc.want {
				t.Fatalf("durability = %q, want %q", got, tc.want)
			}
		})
	}
}
