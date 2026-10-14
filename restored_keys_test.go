package main

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestRestoredFootprintKeysInnerFeeBump(t *testing.T) {
	key := xdr.LedgerKey{Type: xdr.LedgerEntryTypeContractCode, ContractCode: &xdr.LedgerKeyContractCode{}}
	tx := xdr.Transaction{
		Operations: []xdr.Operation{{Body: xdr.OperationBody{Type: xdr.OperationTypeRestoreFootprint}}},
		Ext: xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{
			Resources: xdr.SorobanResources{Footprint: xdr.LedgerFootprint{ReadWrite: []xdr.LedgerKey{key}}},
		}},
	}
	inner := &xdr.TransactionV1Envelope{Tx: tx}
	cases := []struct {
		name     string
		envelope xdr.TransactionEnvelope
		want     int
	}{
		{"ordinary", xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: inner}, 1},
		{"fee bump", xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
			FeeBump: &xdr.FeeBumpTransactionEnvelope{Tx: xdr.FeeBumpTransaction{
				InnerTx: xdr.FeeBumpTransactionInnerTx{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: inner},
			}}}, 1},
		{"missing inner", xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
			FeeBump: &xdr.FeeBumpTransactionEnvelope{}}, 0},
		{"no restore operation", xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{Ext: tx.Ext}}}, 0},
		{"no soroban footprint", xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{Operations: tx.Operations}}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := restoredFootprintKeys(tc.envelope)
			if len(got) != tc.want {
				t.Fatalf("got %d restored keys, want %d", len(got), tc.want)
			}
			if tc.want == 1 && got[0].Type != key.Type {
				t.Fatalf("got key type %v, want %v", got[0].Type, key.Type)
			}
		})
	}
}
