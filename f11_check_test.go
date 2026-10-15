package main

// Guard the premise behind closing F11.
//
// Until this repository extracted transactions and operations through
// stellar-extract, it derived operations.soroban_contract_id for an
// InvokeHostFunction that *creates* a contract; the library leaves it NULL.
// One row in 2,913 on pubnet-contract-creation, and the last value
// difference in operations.
//
// Adopting the library's NULL was defensible only because contract_creations
// already records the created contract, so nothing is lost — the fact moves
// to the table whose job it is. That is a premise, not a guarantee: if the
// library ever stops emitting the creation while still NULLing the
// operation, the contract ID would disappear from Bronze entirely and the
// column would look merely empty rather than wrong.
//
// So this asserts the premise instead of trusting it.

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	extract "github.com/withObsrvr/stellar-extract"
)

func TestCreateContractOperationsAreCoveredByContractCreations(t *testing.T) {
	const fixture = "pubnet-contract-creation"
	meta, _ := loadFixtureMeta(t, fixture)
	path := materializeLedgers(t, filepath.Join("testdata", "fixtures", fixture), t.TempDir())
	src, err := NewXDRStreamSource(path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer src.Close()

	var checked int
	for _, raw := range src.ledgers {
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(raw); err != nil {
			t.Fatalf("unmarshal ledger: %v", err)
		}
		seq := lcm.LedgerSequence()

		in := extract.NewLedgerInput(lcm, meta.NetworkPassphrase)
		creations, err := extract.ExtractContractCreations(in)
		if err != nil {
			t.Fatalf("contract creations: %v", err)
		}
		recorded := map[string]bool{}
		for _, c := range creations {
			recorded[c.ContractID] = true
		}

		// Re-derive what the retired local extractor would have put on the
		// operation: the contract ID out of the create operation's own meta.
		reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(meta.NetworkPassphrase, lcm)
		if err != nil {
			t.Fatalf("transaction reader: %v", err)
		}
		for {
			tx, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("read transaction: %v", err)
			}
			for i, op := range tx.Envelope.Operations() {
				invoke, ok := op.Body.GetInvokeHostFunctionOp()
				if !ok {
					continue
				}
				switch invoke.HostFunction.Type {
				case xdr.HostFunctionTypeHostFunctionTypeCreateContract,
					xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
				default:
					continue
				}
				id, err := extractCreatedContractIDFromMeta(tx.UnsafeMeta, i)
				if err != nil || id == nil {
					continue
				}
				checked++
				if !recorded[*id] {
					t.Errorf("ledger %d op %d: the library leaves soroban_contract_id NULL and "+
						"contract_creations does not record %s either, so the contract ID is "+
						"absent from Bronze entirely. F11 was closed on the assumption that it "+
						"would not be.", seq, i, *id)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no create-contract operation to check; the fixture or the derivation changed")
	}
	t.Logf("checked %d create-contract operations", checked)
}
