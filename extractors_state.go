package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractConfigSettings extracts network configuration settings from a ledger.
// Protocol 20+ Soroban configuration parameters.
// LedgerCloseMeta is already unmarshaled; ledgerSeq, closedAt, ledgerRange are pre-extracted.
func extractConfigSettings(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ConfigSettingData, error) {
	var configSettingsList []ConfigSettingData

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for config settings: %v", err)
		return configSettingsList, nil
	}
	defer txReader.Close()

	// Track unique config settings by ID (deduplicate within ledger)
	configSettingsMap := make(map[int32]*ConfigSettingData)

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for config settings: %v", err)
			continue
		}

		// Get changes from transaction
		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Failed to get transaction changes: %v", err)
			continue
		}

		// Process each change
		for _, change := range changes {
			// Check if this is a config setting change
			if !isConfigSettingChange(change) {
				continue
			}

			// Extract config setting entry
			var configEntry *xdr.ConfigSettingEntry
			var deleted bool
			var lastModifiedLedger uint32

			if change.Post != nil {
				entry, _ := change.Post.Data.GetConfigSetting()
				configEntry = &entry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			} else if change.Pre != nil {
				entry, _ := change.Pre.Data.GetConfigSetting()
				configEntry = &entry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}

			if configEntry == nil {
				continue
			}

			// Extract config setting ID
			configSettingID := int32(configEntry.ConfigSettingId)

			// Create config setting entry
			now := time.Now().UTC()
			resolvedAt := int64(ledgerSeq)

			data := ConfigSettingData{
				// Identity (2 fields)
				ConfigSettingID: configSettingID,
				LedgerSequence:  ledgerSeq,

				// Ledger metadata (3 fields)
				LastModifiedLedger: int32(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           closedAt,

				// Raw XDR for full fidelity
				ConfigSettingXDR: encodeConfigSettingXDR(configEntry),

				// Metadata (2 fields)
				CreatedAt:   now,
				LedgerRange: ledgerRange,

				ObservationSource: "ledger_close_meta",
				ResolvedAtLedger:  &resolvedAt,
			}

			// Extract specific fields based on config setting type (if needed)
			parseConfigSettingFields(configEntry, &data)

			// Deduplicate by config setting ID
			configSettingsMap[configSettingID] = &data
		}
	}

	// Convert map to slice
	for _, data := range configSettingsMap {
		configSettingsList = append(configSettingsList, *data)
	}

	return configSettingsList, nil
}

// isConfigSettingChange checks if a change involves config settings
func isConfigSettingChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	return false
}

// parseConfigSettingFields extracts specific fields from config setting entry.
// For MVP, we store the full XDR which provides complete fidelity.
// Individual field parsing can be added later when needed for specific queries.
func parseConfigSettingFields(entry *xdr.ConfigSettingEntry, data *ConfigSettingData) {
	if entry == nil || data == nil {
		return
	}
	switch entry.ConfigSettingId {
	case xdr.ConfigSettingIdConfigSettingContractComputeV0:
		if entry.ContractCompute == nil {
			return
		}
		ledgerMax := int64(entry.ContractCompute.LedgerMaxInstructions)
		txMax := int64(entry.ContractCompute.TxMaxInstructions)
		feeRate := int64(entry.ContractCompute.FeeRatePerInstructionsIncrement)
		memory := uint32(entry.ContractCompute.TxMemoryLimit)
		data.LedgerMaxInstructions = &ledgerMax
		data.TxMaxInstructions = &txMax
		data.FeeRatePerInstructionsIncrement = &feeRate
		data.TxMemoryLimit = &memory
	case xdr.ConfigSettingIdConfigSettingContractLedgerCostV0:
		if entry.ContractLedgerCost == nil {
			return
		}
		v := entry.ContractLedgerCost
		ledgerReadEntries, ledgerReadBytes := uint32(v.LedgerMaxDiskReadEntries), uint32(v.LedgerMaxDiskReadBytes)
		ledgerWriteEntries, ledgerWriteBytes := uint32(v.LedgerMaxWriteLedgerEntries), uint32(v.LedgerMaxWriteBytes)
		txReadEntries, txReadBytes := uint32(v.TxMaxDiskReadEntries), uint32(v.TxMaxDiskReadBytes)
		txWriteEntries, txWriteBytes := uint32(v.TxMaxWriteLedgerEntries), uint32(v.TxMaxWriteBytes)
		data.LedgerMaxReadLedgerEntries, data.LedgerMaxReadBytes = &ledgerReadEntries, &ledgerReadBytes
		data.LedgerMaxWriteLedgerEntries, data.LedgerMaxWriteBytes = &ledgerWriteEntries, &ledgerWriteBytes
		data.TxMaxReadLedgerEntries, data.TxMaxReadBytes = &txReadEntries, &txReadBytes
		data.TxMaxWriteLedgerEntries, data.TxMaxWriteBytes = &txWriteEntries, &txWriteBytes
	case xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes:
		if entry.ContractMaxSizeBytes != nil {
			value := uint32(*entry.ContractMaxSizeBytes)
			data.ContractMaxSizeBytes = &value
		}
	}
}

// encodeConfigSettingXDR encodes config setting entry to base64 XDR
func encodeConfigSettingXDR(entry *xdr.ConfigSettingEntry) string {
	if entry == nil {
		return ""
	}

	xdrBytes, err := entry.MarshalBinary()
	if err != nil {
		log.Printf("Failed to encode config setting XDR: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(xdrBytes)
}

// extractTTL extracts time-to-live (TTL) entries from a ledger.
// Protocol 20+ Soroban storage expiration tracking.
// LedgerCloseMeta is already unmarshaled; ledgerSeq, closedAt, ledgerRange are pre-extracted.
func extractTTL(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TTLData, error) {
	var ttlList []TTLData

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for TTL: %v", err)
		return ttlList, nil
	}
	defer txReader.Close()

	// Track unique TTL entries by key hash (deduplicate within ledger)
	ttlMap := make(map[string]*TTLData)

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for TTL: %v", err)
			continue
		}

		// Get changes from transaction
		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Failed to get transaction changes: %v", err)
			continue
		}

		// Process each change
		for _, change := range changes {
			// Check if this is a TTL change
			if !isTTLChange(change) {
				continue
			}

			// Extract TTL entry
			var ttlEntry *xdr.TtlEntry
			var deleted bool
			var lastModifiedLedger uint32

			if change.Post != nil {
				entry, _ := change.Post.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			} else if change.Pre != nil {
				entry, _ := change.Pre.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}

			if ttlEntry == nil {
				continue
			}

			// Get key hash from TTL entry (hex encoded)
			keyHashBytes, err := ttlEntry.KeyHash.MarshalBinary()
			if err != nil {
				log.Printf("Failed to marshal key hash: %v", err)
				continue
			}
			keyHash := hex.EncodeToString(keyHashBytes)

			// Calculate TTL remaining and expired status
			liveUntilLedgerSeq := uint32(ttlEntry.LiveUntilLedgerSeq)
			ttlRemaining := int64(liveUntilLedgerSeq) - int64(ledgerSeq)
			expired := ttlRemaining <= 0

			// Create TTL entry
			now := time.Now().UTC()

			data := TTLData{
				// Identity (2 fields)
				KeyHash:        keyHash,
				LedgerSequence: ledgerSeq,

				// TTL tracking (3 fields)
				LiveUntilLedgerSeq: liveUntilLedgerSeq,
				TTLRemaining:       ttlRemaining,
				Expired:            expired,

				// Ledger metadata (3 fields)
				LastModifiedLedger: int32(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           closedAt,

				// Metadata (2 fields)
				CreatedAt:   now,
				LedgerRange: ledgerRange,
			}

			// Deduplicate by key hash
			ttlMap[keyHash] = &data
		}
	}

	// Convert map to slice
	for _, data := range ttlMap {
		ttlList = append(ttlList, *data)
	}

	return ttlList, nil
}

// isTTLChange checks if a change involves TTL entries
func isTTLChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	return false
}

// extractEvictedKeys extracts evicted storage keys from a ledger.
// Protocol 20+ Soroban archival tracking (LedgerCloseMeta V1 and V2).
// LedgerCloseMeta is already unmarshaled; ledgerSeq, closedAt, ledgerRange are pre-extracted.
// NOTE: Does not take networkPassphrase as it reads directly from the meta.
func extractEvictedKeys(lcm xdr.LedgerCloseMeta, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]EvictedKeyData, error) {
	var evictedKeysList []EvictedKeyData

	var evictedKeys []xdr.LedgerKey
	switch lcm.V {
	case 0:
		return evictedKeysList, nil
	case 1:
		evictedKeys = lcm.MustV1().EvictedKeys
	case 2:
		evictedKeys = lcm.MustV2().EvictedKeys
	default:
		return nil, fmt.Errorf("unsupported LedgerCloseMeta version %d for evicted keys", lcm.V)
	}

	// Track unique evicted keys by hash (deduplicate within ledger)
	evictedKeysMap := make(map[string]*EvictedKeyData)

	for _, evictedKey := range evictedKeys {
		// Determine durability based on key type
		// For now, default to "unknown"
		durability := "unknown"

		data := extractEvictedKeyData(evictedKey, ledgerSeq, durability, closedAt, ledgerRange)
		if data != nil {
			evictedKeysMap[data.KeyHash] = data
		}
	}

	// Convert map to slice
	for _, data := range evictedKeysMap {
		evictedKeysList = append(evictedKeysList, *data)
	}

	return evictedKeysList, nil
}

// extractEvictedKeyData extracts data from a single evicted ledger key
func extractEvictedKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, durability string, closedAt time.Time, ledgerRange uint32) *EvictedKeyData {
	// Generate SHA256 hash of the ledger key
	keyHashBytes, err := ledgerKey.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal evicted ledger key: %v", err)
		return nil
	}

	hash := sha256.Sum256(keyHashBytes)
	keyHash := hex.EncodeToString(hash[:])

	// Extract contract ID and key type based on ledger entry type
	var contractID string
	var keyType string

	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeContractData:
		// Explicit nil checks to prevent panics
		if ledgerKey.ContractData == nil {
			log.Printf("Warning: ContractData is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractData"
		} else {
			// Safely extract contract ID
			contractBytes, err := ledgerKey.ContractData.Contract.MarshalBinary()
			if err == nil && len(contractBytes) > 0 {
				// Use hash of the full contract address as ID
				contractHash := sha256.Sum256(contractBytes)
				contractID = hex.EncodeToString(contractHash[:])
			}

			// Safely extract key type from ScVal
			keyType = "ContractData"
			if keyBytes, err := ledgerKey.ContractData.Key.MarshalBinary(); err == nil && len(keyBytes) > 0 {
				keyType = ledgerKey.ContractData.Key.Type.String()
			}
		}

	case xdr.LedgerEntryTypeContractCode:
		// Explicit nil checks to prevent panics
		if ledgerKey.ContractCode == nil {
			log.Printf("Warning: ContractCode is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractCode"
		} else {
			// Safely extract contract code hash
			codeHashBytes, err := ledgerKey.ContractCode.Hash.MarshalBinary()
			if err == nil && len(codeHashBytes) > 0 {
				contractID = hex.EncodeToString(codeHashBytes)
			}
			keyType = "ContractCode"
		}

	default:
		// Other ledger entry types (unlikely to be evicted)
		keyType = ledgerKey.Type.String()
	}

	// Create evicted key entry
	now := time.Now().UTC()

	data := EvictedKeyData{
		// Identity (2 fields)
		KeyHash:        keyHash,
		LedgerSequence: ledgerSeq,

		// Eviction details (3 fields)
		ContractID: contractID,
		KeyType:    keyType,
		Durability: durability,

		// Metadata (3 fields)
		ClosedAt:    closedAt,
		LedgerRange: ledgerRange,
		CreatedAt:   now,
	}

	return &data
}
