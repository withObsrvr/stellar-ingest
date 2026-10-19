package ingest

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
		for _, c := range creations {
			if c.ContractID == "" {
				t.Errorf("ledger %d: contract_creations row has an empty contract_id", seq)
			}
		}

		ops, err := extract.ExtractOperations(in)
		if err != nil {
			t.Fatalf("operations: %v", err)
		}

		// Count the operations that create a contract. Every one of them
		// leaves soroban_contract_id NULL, and contract_creations is where
		// the ID has to appear instead.
		var creating int
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
			for _, op := range tx.Envelope.Operations() {
				invoke, ok := op.Body.GetInvokeHostFunctionOp()
				if !ok {
					continue
				}
				switch invoke.HostFunction.Type {
				case xdr.HostFunctionTypeHostFunctionTypeCreateContract,
					xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
					creating++
				}
			}
		}
		if creating == 0 {
			continue
		}
		checked += creating

		if len(creations) < creating {
			t.Errorf("ledger %d: %d operations create a contract but contract_creations has only %d rows. "+
				"F11 was closed on the premise that the created contract ID moves to that table; "+
				"with fewer rows than creating operations it is absent from Bronze entirely.",
				seq, creating, len(creations))
		}
		for _, o := range ops {
			if o.SorobanOperation != nil &&
				(*o.SorobanOperation == xdr.HostFunctionTypeHostFunctionTypeCreateContract.String() ||
					*o.SorobanOperation == xdr.HostFunctionTypeHostFunctionTypeCreateContractV2.String()) &&
				o.SorobanContractID != nil {
				t.Errorf("ledger %d: a create-contract operation now carries soroban_contract_id=%s; "+
					"F11 was closed as NULL, so this is a behaviour change to review, not a pass",
					seq, *o.SorobanContractID)
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no create-contract operation to check; the fixture or the derivation changed")
	}
	t.Logf("checked %d create-contract operations", checked)
}
