package adapters

import (
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"
	"os"
	"testing"
)

// TestXDRStreamSourceReadsCompleteFile is a regression test for a clean
// end-of-stream being misreported as a decode failure.
//
// NewXDRStreamSource decodes LedgerCloseMeta records in a loop until the file
// is exhausted. It used to detect the end with `err == io.EOF`, but the XDR
// decoder wraps io.EOF rather than returning it bare, so the comparison never
// matched and every complete file — including correct output from
// `nebu fetch` — failed with:
//
//	XDR stream decode error: decoding Int: xdr:DecodeInt: EOF while decoding 4 bytes
//
// That made --storage-type XDR unusable. The fix is errors.Is. Without it this
// test fails at NewXDRStreamSource.
func TestXDRStreamSourceReadsCompleteFile(t *testing.T) {
	meta, path := fixtures.LoadMeta(t, "pubnet-genesis-p1")

	source, err := NewXDRStreamSource(path, 0)
	if err != nil {
		t.Fatalf("NewXDRStreamSource(%s): %v", path, err)
	}
	defer source.Close()

	if got := len(source.ledgers); got != meta.LedgerCount {
		t.Errorf("indexed %d ledgers, want %d", got, meta.LedgerCount)
	}

	// Every sequence in the manifest range must be present and non-empty.
	for seq := meta.StartLedger; seq <= meta.EndLedger; seq++ {
		data, ok := source.ledgers[seq]
		if !ok {
			t.Errorf("ledger %d missing from indexed stream", seq)
			continue
		}
		if len(data) == 0 {
			t.Errorf("ledger %d indexed with empty payload", seq)
		}
	}

	// Nothing outside the manifest range should appear.
	for seq := range source.ledgers {
		if seq < meta.StartLedger || seq > meta.EndLedger {
			t.Errorf("unexpected ledger %d outside range [%d,%d]", seq, meta.StartLedger, meta.EndLedger)
		}
	}
}

// TestFixtureIntegrity pins the fixture bytes so a corrupted or accidentally
// regenerated fixture is caught here rather than surfacing as a confusing
// golden-output mismatch somewhere else.
func TestFixtureIntegrity(t *testing.T) {
	meta, path := fixtures.LoadMeta(t, "pubnet-genesis-p1")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if got := domain.SHA256Hex(data); got != meta.SHA256 {
		t.Errorf("fixture sha256 = %s, manifest says %s", got, meta.SHA256)
	}
}
