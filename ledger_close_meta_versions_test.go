package main

import (
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExtractLedgersSupportsAllCurrentLedgerCloseMetaVersions(t *testing.T) {
	header := xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{LedgerSeq: 100}}
	generalized, err := xdr.NewGeneralizedTransactionSet(1, xdr.TransactionSetV1{})
	if err != nil {
		t.Fatalf("create generalized transaction set: %v", err)
	}
	metas := []xdr.LedgerCloseMeta{
		{V: 0, V0: &xdr.LedgerCloseMetaV0{LedgerHeader: header}},
		{V: 1, V1: &xdr.LedgerCloseMetaV1{LedgerHeader: header, TxSet: generalized}},
		{V: 2, V2: &xdr.LedgerCloseMetaV2{LedgerHeader: header, TxSet: generalized}},
	}

	for _, meta := range metas {
		if _, err := extractLedgers(meta, "test", 100, time.Unix(0, 0), 0); err != nil {
			t.Fatalf("LedgerCloseMeta V%d: %v", meta.V, err)
		}
	}
}

func TestExtractEvictedKeysSupportsV1AndV2(t *testing.T) {
	key, err := xdr.NewLedgerKey(xdr.LedgerEntryTypeTtl, xdr.LedgerKeyTtl{KeyHash: xdr.Hash{1}})
	if err != nil {
		t.Fatalf("create TTL ledger key: %v", err)
	}
	metas := []xdr.LedgerCloseMeta{
		{V: 1, V1: &xdr.LedgerCloseMetaV1{EvictedKeys: []xdr.LedgerKey{key}}},
		{V: 2, V2: &xdr.LedgerCloseMetaV2{EvictedKeys: []xdr.LedgerKey{key}}},
	}
	for _, meta := range metas {
		got, err := extractEvictedKeys(meta, 100, time.Unix(0, 0), 0)
		if err != nil {
			t.Fatalf("LedgerCloseMeta V%d: %v", meta.V, err)
		}
		if len(got) != 1 {
			t.Fatalf("LedgerCloseMeta V%d extracted %d evicted keys, want 1", meta.V, len(got))
		}
	}
}
