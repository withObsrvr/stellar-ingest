package main

import (
	"encoding/base64"
	"testing"
)

func strptr(s string) *string { return &s }

func TestSanitizeTransactionMemo(t *testing.T) {
	// Memo bytes that are not valid UTF-8; these would fail a Parquet text write
	// and are exactly why text memos are base64-encoded before extraction.
	invalidUTF8 := string([]byte{0xff, 0xfe, 0x00, 0x41})

	t.Run("text memo is base64 encoded and retyped", func(t *testing.T) {
		tx := &TransactionData{MemoType: strptr("text"), Memo: strptr(invalidUTF8)}
		sanitizeTransactionMemo(tx)

		if tx.MemoType == nil || *tx.MemoType != "text_base64" {
			t.Fatalf("MemoType = %v, want text_base64", tx.MemoType)
		}
		want := base64.StdEncoding.EncodeToString([]byte(invalidUTF8))
		if tx.Memo == nil || *tx.Memo != want {
			t.Fatalf("Memo = %v, want %q", tx.Memo, want)
		}
	})

	t.Run("already base64 memo is left untouched (no double encode)", func(t *testing.T) {
		encoded := base64.StdEncoding.EncodeToString([]byte("hello"))
		tx := &TransactionData{MemoType: strptr("text_base64"), Memo: strptr(encoded)}
		sanitizeTransactionMemo(tx)

		if *tx.MemoType != "text_base64" || *tx.Memo != encoded {
			t.Fatalf("memo mutated: type=%v memo=%v", *tx.MemoType, *tx.Memo)
		}
	})

	t.Run("non-text memo type is left untouched", func(t *testing.T) {
		tx := &TransactionData{MemoType: strptr("id"), Memo: strptr("12345")}
		sanitizeTransactionMemo(tx)

		if *tx.MemoType != "id" || *tx.Memo != "12345" {
			t.Fatalf("memo mutated: type=%v memo=%v", *tx.MemoType, *tx.Memo)
		}
	})

	t.Run("nil inputs do not panic", func(t *testing.T) {
		sanitizeTransactionMemo(nil)
		sanitizeTransactionMemo(&TransactionData{})
		sanitizeTransactionMemo(&TransactionData{MemoType: strptr("text")}) // nil Memo
	})
}
