package domain

// Rule 1 stores transaction_hash as the 32 bytes it is instead of 64 hex
// characters. The presentation view hands consumers back the string by
// rendering lower(hex(..)), so the encoding is only safe if it round-trips
// exactly -- and the re-ingest rewrites every file, so an encoding that
// loses information loses it permanently.

import (
	"strings"
	"testing"
)

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
		if got := DecodeHashBytes(tc.input); got != nil {
			t.Errorf("%s: decoded to %d bytes, want nil", tc.name, len(got))
		}
	}

	// And the one shape it must accept, including uppercase input.
	full := strings.Repeat("AB", HashBytesLen)
	raw := DecodeHashBytes(full)
	if len(raw) != HashBytesLen {
		t.Fatalf("a %d-byte hex digest decoded to %d bytes", HashBytesLen, len(raw))
	}
	if got := RenderHashBytes(raw); got != strings.ToLower(full) {
		t.Errorf("uppercase input rendered as %q, want %q", got, strings.ToLower(full))
	}
}
