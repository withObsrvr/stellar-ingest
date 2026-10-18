package main

// Rule 1 stores transaction_hash as the 32 bytes it is instead of 64 hex
// characters. The presentation view hands consumers back the string by
// rendering lower(hex(..)), so the encoding is only safe if it round-trips
// exactly -- and the re-ingest rewrites every file, so an encoding that
// loses information loses it permanently.

import (
	"strings"
	"testing"
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
		raw := decodeHashBytes(tx.TransactionHash)
		if len(raw) == 0 {
			empty++
			continue
		}
		if len(raw) != hashBytesLen {
			t.Errorf("%s decoded to %d bytes, want %d", tx.TransactionHash, len(raw), hashBytesLen)
		}
		// Exactly what bronze.transactions_row_v2 computes.
		if got := renderHashBytes(raw); got != strings.ToLower(tx.TransactionHash) {
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

// The failure path is a decision, so it is pinned: a value that is not a
// 32-byte hex digest is stored as null, which the view surfaces as null.
// Returning a truncated or re-encoded value instead would put a plausible
// wrong hash into the only copy of the data.
func TestDecodeHashBytesRejectsAnythingNotADigest(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"not hex", "zzzz6f36e0e430c44cf60d416ca2a8679155a28f695f98d959772dc103294f52"},
		{"too short", "a2069f36"},
		{"too long", strings.Repeat("ab", 33)},
		{"odd length", "a2069f36e"},
	} {
		if got := decodeHashBytes(tc.input); got != nil {
			t.Errorf("%s: decoded to %d bytes, want nil", tc.name, len(got))
		}
	}

	// And the one shape it must accept, including uppercase input.
	full := strings.Repeat("AB", hashBytesLen)
	raw := decodeHashBytes(full)
	if len(raw) != hashBytesLen {
		t.Fatalf("a %d-byte hex digest decoded to %d bytes", hashBytesLen, len(raw))
	}
	if got := renderHashBytes(raw); got != strings.ToLower(full) {
		t.Errorf("uppercase input rendered as %q, want %q", got, strings.ToLower(full))
	}
}
