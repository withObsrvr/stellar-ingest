package ingest

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	extract "github.com/withObsrvr/stellar-extract"
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
		if _, err := extract.ExtractLedgers(extract.NewLedgerInput(meta, "test")); err != nil {
			t.Fatalf("LedgerCloseMeta V%d: %v", meta.V, err)
		}
	}
}
