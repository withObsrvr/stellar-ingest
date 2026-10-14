// Package restored maps authoritative Stellar ledger changes to restored-key
// Bronze values. It has no dependency on the CLI, worker, or destination.
// This remains internal until the live ingester's shared row contract is set.
package restored

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Row contains the ledger-derived values for one restored key. Ingestion
// timestamps and operator-supplied provenance are added by the application.
type Row struct {
	KeyHash            string
	LedgerSequence     uint32
	ContractID         string
	KeyType            string
	Durability         string
	RestoredFromLedger uint32
	ClosedAt           time.Time
	LedgerRange        uint32
}

// Extract reads restored-entry changes, including automatic restoration during
// contract invocation when no RestoreFootprint operation is present.
func Extract(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]Row, error) {
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("create restored-key change reader: %w", err)
	}
	defer reader.Close()

	byHash := make(map[string]Row)
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read restored-key change: %w", err)
		}
		if change.ChangeType != xdr.LedgerEntryChangeTypeLedgerEntryRestored || change.Post == nil {
			continue
		}
		key, err := change.Post.LedgerKey()
		if err != nil {
			return nil, fmt.Errorf("derive restored ledger key: %w", err)
		}
		data, err := extractKey(key, ledgerSeq, ledgerRange, durability(key), uint32(change.Post.LastModifiedLedgerSeq), closedAt)
		if err != nil {
			return nil, fmt.Errorf("encode restored ledger key in ledger %d: %w", ledgerSeq, err)
		}
		byHash[data.KeyHash] = data
	}
	rows := make([]Row, 0, len(byHash))
	for _, data := range byHash {
		rows = append(rows, data)
	}
	return rows, nil
}

func durability(key xdr.LedgerKey) string {
	switch key.Type {
	case xdr.LedgerEntryTypeContractData:
		if key.ContractData != nil {
			switch key.ContractData.Durability {
			case xdr.ContractDataDurabilityPersistent:
				return "persistent"
			case xdr.ContractDataDurabilityTemporary:
				return "temporary"
			}
		}
	case xdr.LedgerEntryTypeContractCode, xdr.LedgerEntryTypeTtl:
		return "persistent"
	}
	return "unknown"
}

func extractKey(key xdr.LedgerKey, ledgerSeq, ledgerRange uint32, durability string, restoredFromLedger uint32, closedAt time.Time) (Row, error) {
	keyHashBytes, err := key.MarshalBinary()
	if err != nil {
		return Row{}, err
	}
	hash := sha256.Sum256(keyHashBytes)
	row := Row{
		KeyHash: hex.EncodeToString(hash[:]), LedgerSequence: ledgerSeq,
		Durability: durability, RestoredFromLedger: restoredFromLedger,
		ClosedAt: closedAt, LedgerRange: ledgerRange,
	}
	switch key.Type {
	case xdr.LedgerEntryTypeContractData:
		if key.ContractData == nil {
			row.KeyType = "ContractData"
		} else {
			if key.ContractData.Contract.ContractId != nil {
				contractIDBytes, err := key.ContractData.Contract.ContractId.MarshalBinary()
				if err == nil {
					row.ContractID = hex.EncodeToString(contractIDBytes)
				}
			}
			row.KeyType = key.ContractData.Key.Type.String()
		}
	case xdr.LedgerEntryTypeContractCode:
		if key.ContractCode == nil {
			row.KeyType = "ContractCode"
		} else {
			codeHashBytes, err := key.ContractCode.Hash.MarshalBinary()
			if err == nil {
				row.ContractID = hex.EncodeToString(codeHashBytes)
			}
			row.KeyType = "ContractCode"
		}
	default:
		row.KeyType = key.Type.String()
	}
	return row, nil
}
