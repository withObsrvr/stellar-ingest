package main

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExtractCreatedContractIDFromOperationMeta(t *testing.T) {
	var rawID xdr.ContractId
	for i := range rawID {
		rawID[i] = byte(i + 1)
	}
	contractData := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &rawID,
		},
		Durability: xdr.ContractDataDurabilityPersistent,
	}
	created := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type:         xdr.LedgerEntryTypeContractData,
			ContractData: &contractData,
		}},
	}
	expected, err := strkey.Encode(strkey.VersionByteContract, rawID[:])
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		meta xdr.TransactionMeta
	}{
		{
			name: "v3",
			meta: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{{}, {Changes: xdr.LedgerEntryChanges{created}}},
			}},
		},
		{
			name: "v4",
			meta: xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
				Operations: []xdr.OperationMetaV2{{}, {Changes: xdr.LedgerEntryChanges{created}}},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := extractCreatedContractIDFromMeta(test.meta, 1)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || *got != expected {
				t.Fatalf("created contract ID = %v, want %s", got, expected)
			}
		})
	}
}

func TestExtractCreatedContractIDFromOperationMetaReturnsNilWhenUnresolved(t *testing.T) {
	got, err := extractCreatedContractIDFromMeta(xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("created contract ID = %q, want nil", *got)
	}
}
