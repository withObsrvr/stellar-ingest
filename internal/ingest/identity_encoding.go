package ingest

// Storage encoding for identity columns, under rule 1 of the Bronze storage
// contract: an identifier is stored as the bytes it is, never as its text
// rendering. A transaction hash is a 32-byte digest; as 64 hex characters it
// costs 66.0 bytes per row and compresses by essentially nothing, because a
// digest is random and a block compressor has nothing to exploit.
//
// The presentation view renders lower(hex(..)) to give consumers back the
// string they query today, so this encoding is only correct if it round-trips
// exactly. identity_encoding_test.go asserts that over every fixture.

import (
	"encoding/hex"
	"log"
)

// hashBytesLen is the width of every digest Bronze stores: transaction and
// ledger hashes are SHA-256 outputs.
const hashBytesLen = 32

// decodeHashBytes converts a hex-rendered digest to the bytes it names.
//
// Returns nil for anything that is not a hex digest of the expected width.
// Nil is deliberate: it writes a null the view will surface as a null, which
// is visibly wrong, where a silently truncated or re-encoded value would not
// be. Every producer builds these with hex.EncodeToString, so a failure here
// is a defect upstream rather than bad input, and it is logged once per row
// precisely because it should never happen.
func decodeHashBytes(hash string) []byte {
	if hash == "" {
		return nil
	}
	raw, err := hex.DecodeString(hash)
	if err != nil || len(raw) != hashBytesLen {
		log.Printf("[identity] transaction_hash %q is not a %d-byte hex digest (len=%d, err=%v); "+
			"storing null", hash, hashBytesLen, len(raw), err)
		return nil
	}
	return raw
}

// renderHashBytes is the Go equivalent of the view's lower(hex(..)). It
// exists so the round-trip can be asserted in a test without a database.
func renderHashBytes(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return hex.EncodeToString(raw)
}
