package main

import (
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/withObsrvr/stellar-ingest/internal/bronze/restored"
)

// extractCreatedContractIDFromMeta resolves the contract created by one
// InvokeHostFunction operation. CreateContract inputs do not contain the
// resulting contract ID; it is only present in that operation's ledger changes.
func extractCreatedContractIDFromMeta(meta xdr.TransactionMeta, operationIndex int) (*string, error) {
	var changes xdr.LedgerEntryChanges
	switch meta.V {
	case 3:
		v3 := meta.MustV3()
		if operationIndex < 0 || operationIndex >= len(v3.Operations) {
			return nil, nil
		}
		changes = v3.Operations[operationIndex].Changes
	case 4:
		v4 := meta.MustV4()
		if operationIndex < 0 || operationIndex >= len(v4.Operations) {
			return nil, nil
		}
		changes = v4.Operations[operationIndex].Changes
	default:
		return nil, nil
	}

	for _, change := range changes {
		created, ok := change.GetCreated()
		if !ok {
			continue
		}
		contractData, ok := created.Data.GetContractData()
		if !ok || contractData.Durability != xdr.ContractDataDurabilityPersistent {
			continue
		}
		contractID, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}
		encoded, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
		if err != nil {
			return nil, fmt.Errorf("encode created contract ID: %w", err)
		}
		return &encoded, nil
	}
	return nil, nil
}

// ===========================================================================
// Contract Events
// ===========================================================================

// ===========================================================================
// Contract Data
// ===========================================================================

// ===========================================================================
// Contract Code (WASM)
// ===========================================================================

// ===========================================================================
// Restored Keys
// ===========================================================================

// extractRestoredKeys adapts the private ledger-derived Bronze mapping to the
// existing v3 row type. Keep run timestamps outside the deterministic mapper.
func extractRestoredKeys(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]RestoredKeyData, error) {
	values, err := restored.Extract(lcm, networkPassphrase, ledgerSeq, closedAt, ledgerRange)
	if err != nil {
		return nil, err
	}
	rows := make([]RestoredKeyData, len(values))
	for i, value := range values {
		rows[i] = RestoredKeyData{
			KeyHash: value.KeyHash, LedgerSequence: value.LedgerSequence,
			ContractID: value.ContractID, KeyType: value.KeyType,
			Durability: value.Durability, RestoredFromLedger: value.RestoredFromLedger,
			ClosedAt: value.ClosedAt, LedgerRange: value.LedgerRange,
			CreatedAt: time.Now().UTC(),
		}
	}
	return rows, nil
}

// ===========================================================================
// Contract Creations
// ===========================================================================
