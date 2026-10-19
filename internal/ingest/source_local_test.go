package ingest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestXDRStreamSourceRejectsTruncatedFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.xdr")
	valid := streamFixtureLedger(t, 100)
	data := append(append([]byte(nil), valid...), valid[:len(valid)/2]...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	_, err := NewXDRStreamSource(path)
	if err == nil || !strings.Contains(err.Error(), "XDR stream decode error") {
		t.Fatalf("error = %v, want truncated XDR decode error", err)
	}
}
