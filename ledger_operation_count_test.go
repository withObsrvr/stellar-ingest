package main

import (
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestCountEnvelopeOperationsIncludesAllDeclaredFailedOperations(t *testing.T) {
	envelopes := []xdr.TransactionEnvelope{
		transactionEnvelopeWithOperationCount(29),
		transactionEnvelopeWithOperationCount(8),
	}
	if got := countEnvelopeOperations(envelopes); got != 37 {
		t.Fatalf("countEnvelopeOperations() = %d, want 37", got)
	}
}

func TestWorkerLedgerExtractorUsesTransactionSetEnvelopes(t *testing.T) {
	envelopes := []xdr.TransactionEnvelope{
		transactionEnvelopeWithOperationCount(29),
		transactionEnvelopeWithOperationCount(8),
	}
	components := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: envelopes,
		},
	}}
	ledgerSequence := uint32(3_501_542)
	lcm := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(ledgerSequence)},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{{V: 0, V0Components: &components}},
				},
			},
		},
	}

	worker := &Worker{config: OrchestratorConfig{
		NetworkPassphrase: "Test SDF Network ; September 2015",
		OnlyTables:        parseCSVSet("ledgers"),
	}}
	data, err := worker.extractLedger(LedgerMeta{
		LCM:            lcm,
		LedgerSequence: ledgerSequence,
		ClosedAt:       time.Unix(1_750_000_000, 0).UTC(),
		LedgerRange:    3_500_000,
	})
	if err != nil {
		t.Fatalf("extractLedger: %v", err)
	}
	if len(data.Ledgers) != 1 {
		t.Fatalf("ledger rows = %d, want 1", len(data.Ledgers))
	}
	if got := data.Ledgers[0].TxSetOperationCount; got != 37 {
		t.Fatalf("worker tx_set_operation_count = %d, want 37", got)
	}
}

func transactionEnvelopeWithOperationCount(count int) xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{Operations: make([]xdr.Operation, count)},
		},
	}
}
