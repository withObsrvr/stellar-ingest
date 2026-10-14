package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
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

// extractContractEvents extracts Soroban contract events from a ledger.
// Captures three event types: CONTRACT, SYSTEM, DIAGNOSTIC (includes invocation tree).
func extractContractEvents(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractEventData, error) {
	var events []ContractEventData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract events: %v", err)
		return events, nil
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for contract events: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()

		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			continue
		}

		// Extract diagnostic events (contains invocation tree)
		for diagIdx, diagEvent := range txEvents.DiagnosticEvents {
			eventData := extractDiagnosticEvent(diagEvent, txHash, ledgerSeq, closedAt, ledgerRange, uint32(diagIdx), txSuccessful)
			events = append(events, eventData)
		}

		// Extract operation-level contract events.
		// Operation events only appear for successful operations, so in_successful_contract_call is true.
		for opIdx, opEvents := range txEvents.OperationEvents {
			for eventIdx, contractEvent := range opEvents {
				eventData := extractContractEvent(contractEvent, txHash, ledgerSeq, closedAt, ledgerRange, uint32(opIdx), uint32(eventIdx), txSuccessful, true)
				events = append(events, eventData)
			}
		}

		// Extract transaction-level events (V4 / CAP-67 unified events).
		// These are contract events emitted at the transaction level rather
		// than tied to a specific operation. Classic operations can produce
		// these in TransactionMetaV4+.
		for txEvtIdx, txEvt := range txEvents.TransactionEvents {
			eventData := extractContractEvent(txEvt.Event, txHash, ledgerSeq, closedAt, ledgerRange, 0, uint32(txEvtIdx), txSuccessful, true)
			events = append(events, eventData)
		}
	}

	return events, nil
}

// extractDiagnosticEvent extracts data from a diagnostic event.
func extractDiagnosticEvent(diagEvent xdr.DiagnosticEvent, txHash string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32, diagIdx uint32, txSuccessful bool) ContractEventData {
	eventData := extractContractEvent(
		diagEvent.Event,
		txHash,
		ledgerSeq,
		closedAt,
		ledgerRange,
		diagIdx,
		0,
		txSuccessful,
		diagEvent.InSuccessfulContractCall,
	)
	eventData.EventType = "diagnostic"
	return eventData
}

// extractContractEvent extracts data from a ContractEvent.
func extractContractEvent(
	event xdr.ContractEvent,
	txHash string,
	ledgerSeq uint32,
	closedAt time.Time,
	ledgerRange uint32,
	opIndex uint32,
	eventIndex uint32,
	txSuccessful bool,
	inSuccessfulCall bool,
) ContractEventData {
	eventID := fmt.Sprintf("%s:%d:%d", txHash, opIndex, eventIndex)

	var contractID *string
	if event.ContractId != nil {
		id := hex.EncodeToString((*event.ContractId)[:])
		contractID = &id
	}

	eventType := eventTypeString(event.Type)
	topicsJSON, topicsDecoded, topicCount, dataXDR, dataDecoded, positionalTopics := extractEventBody(event.Body)

	now := time.Now().UTC()

	return ContractEventData{
		EventID:         eventID,
		ContractID:      contractID,
		LedgerSequence:  ledgerSeq,
		TransactionHash: txHash,
		ClosedAt:        closedAt,

		EventType:                eventType,
		Successful:               txSuccessful,
		InSuccessfulContractCall: inSuccessfulCall,

		TopicsJSON:    topicsJSON,
		TopicsDecoded: topicsDecoded,
		DataXDR:       dataXDR,
		DataDecoded:   dataDecoded,
		TopicCount:    topicCount,

		Topic0Decoded: positionalTopics[0],
		Topic1Decoded: positionalTopics[1],
		Topic2Decoded: positionalTopics[2],
		Topic3Decoded: positionalTopics[3],

		OperationIndex: opIndex,
		EventIndex:     eventIndex,

		CreatedAt:   now,
		LedgerRange: ledgerRange,
	}
}

// extractEventBody extracts topics and data from ContractEventBody.
// Returns: (topicsJSON, topicsDecoded, topicCount, dataXDR, dataDecoded, positionalTopics)
func extractEventBody(body xdr.ContractEventBody) (string, string, int32, string, string, [4]*string) {
	var positionalTopics [4]*string

	if body.V != 0 {
		log.Printf("Unknown ContractEventBody version: %d", body.V)
		return "[]", "[]", 0, "", "{}", positionalTopics
	}

	v0 := body.MustV0()

	topicsXDR := []string{}
	topicsDecodedArray := []interface{}{}
	for i, topic := range v0.Topics {
		topicBytes, err := topic.MarshalBinary()
		if err != nil {
			log.Printf("Failed to marshal topic: %v", err)
			continue
		}
		topicsXDR = append(topicsXDR, base64.StdEncoding.EncodeToString(topicBytes))

		decodedTopic, err := ConvertScValToJSON(topic)
		if err != nil {
			log.Printf("Failed to decode topic: %v", err)
			topicsDecodedArray = append(topicsDecodedArray, map[string]interface{}{
				"error": err.Error(),
				"type":  topic.Type.String(),
			})
		} else {
			topicsDecodedArray = append(topicsDecodedArray, decodedTopic)
			if i < 4 {
				positionalTopics[i] = flattenTopicValue(decodedTopic)
			}
		}
	}

	topicsJSON, err := json.Marshal(topicsXDR)
	if err != nil {
		log.Printf("Failed to marshal topics JSON: %v", err)
		return "[]", "[]", 0, "", "{}", positionalTopics
	}

	topicsDecodedJSON, err := json.Marshal(topicsDecodedArray)
	if err != nil {
		log.Printf("Failed to marshal decoded topics: %v", err)
		topicsDecodedJSON = []byte("[]")
	}

	topicCount := int32(len(v0.Topics))

	dataBytes, err := v0.Data.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal data: %v", err)
		return string(topicsJSON), string(topicsDecodedJSON), topicCount, "", "{}", positionalTopics
	}

	dataXDR := base64.StdEncoding.EncodeToString(dataBytes)

	decodedData, err := ConvertScValToJSON(v0.Data)
	if err != nil {
		log.Printf("Failed to decode data: %v", err)
		decodedData = map[string]interface{}{
			"error": err.Error(),
			"type":  v0.Data.Type.String(),
		}
	}

	dataDecodedJSON, err := json.Marshal(decodedData)
	if err != nil {
		log.Printf("Failed to marshal decoded data: %v", err)
		dataDecodedJSON = []byte("{}")
	}

	return string(topicsJSON), string(topicsDecodedJSON), topicCount, dataXDR, string(dataDecodedJSON), positionalTopics
}

// eventTypeString converts ContractEventType to string.
func eventTypeString(t xdr.ContractEventType) string {
	switch t {
	case xdr.ContractEventTypeSystem:
		return "system"
	case xdr.ContractEventTypeContract:
		return "contract"
	case xdr.ContractEventTypeDiagnostic:
		return "diagnostic"
	default:
		return "unknown"
	}
}

// flattenTopicValue extracts a query-friendly string from a decoded topic value.
func flattenTopicValue(decoded interface{}) *string {
	switch v := decoded.(type) {
	case string:
		return &v
	case map[string]interface{}:
		if addr, ok := v["address"].(string); ok {
			return &addr
		}
		if val, ok := v["value"].(string); ok {
			return &val
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		s := string(b)
		return &s
	default:
		s := fmt.Sprintf("%v", v)
		return &s
	}
}

// ===========================================================================
// Contract Data
// ===========================================================================

// contractStorageBalanceFromData inlines the logic of sac.ContractBalanceFromContractData
// but includes native-XLM SAC balances, which the SDK helper intentionally omits.
func contractStorageBalanceFromData(ledgerEntry xdr.LedgerEntry, _ string) ([32]byte, *big.Int, bool) {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return [32]byte{}, nil, false
	}
	return contractBalanceFromData(contractData)
}

// extractContractData extracts contract data state from a ledger.
// Uses LedgerChangeReader directly and includes SAC asset/balance detection plus token metadata.
func extractContractData(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractDataData, error) {
	var contractDataList []ContractDataData

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for contract data: %v", err)
		return contractDataList, nil
	}
	defer changeReader.Close()

	// Track unique contract data by key (deduplicate within ledger)
	contractMap := make(map[string]*ContractDataData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for contract data: %v", err)
			continue
		}

		if !isContractDataChange(change) {
			continue
		}

		// Determine the active entry (post for creates/updates, pre for deletes)
		var entry *xdr.LedgerEntry
		var deleted bool

		if change.Post != nil {
			entry = change.Post
			deleted = false
		} else if change.Pre != nil {
			entry = change.Pre
			deleted = true
		} else {
			continue
		}

		contractData, ok := entry.Data.GetContractData()
		if !ok {
			continue
		}

		// Skip nonce keys
		if contractData.Key.Type == xdr.ScValTypeScvLedgerKeyNonce {
			continue
		}

		// Extract contract ID from ScAddress
		contractIDHash, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}
		contractIDBytes, err := contractIDHash.MarshalBinary()
		if err != nil {
			continue
		}
		contractIDStr, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes)
		if err != nil {
			continue
		}

		// Generate ledger key hash
		var ledgerKeyHash string
		ledgerKey, err := entry.LedgerKey()
		if err == nil {
			keyBytes, err := ledgerKey.MarshalBinary()
			if err == nil {
				hash := sha256.Sum256(keyBytes)
				ledgerKeyHash = hex.EncodeToString(hash[:])
			}
		}

		// Key type and durability
		keyType := contractData.Key.Type.String()
		durability := contractData.Durability.String()

		// Encode contract data as base64 XDR
		var contractDataXDR string
		if xdrBytes, err := contractData.MarshalBinary(); err == nil {
			contractDataXDR = base64.StdEncoding.EncodeToString(xdrBytes)
		}

		now := time.Now().UTC()

		// SAC detection: check if this is a Stellar Asset Contract entry
		var assetCode, assetIssuer, assetType, balanceHolder, balance *string
		if asset, ok := sac.AssetFromContractData(*entry, networkPassphrase); ok {
			canonical := asset.StringCanonical()
			if asset.IsNative() {
				at := "native"
				assetType = &at
			} else {
				code := asset.GetCode()
				issuer := asset.GetIssuer()
				assetCode = &code
				assetIssuer = &issuer
				if len(code) <= 4 {
					at := "credit_alphanum4"
					assetType = &at
				} else {
					at := "credit_alphanum12"
					assetType = &at
				}
			}
			_ = canonical
		}

		// SAC balance detection. Use the local decoder so native-XLM SAC balances
		// are included (the SDK helper intentionally excludes native lumens).
		if holder, amt, ok := contractStorageBalanceFromData(*entry, networkPassphrase); ok {
			holderStr, err := strkey.Encode(strkey.VersionByteContract, holder[:])
			if err == nil {
				balanceHolder = &holderStr
			}
			amtStr := amt.String()
			balance = &amtStr
		}

		// Token metadata extraction from instance storage
		var tokenName, tokenSymbol *string
		var tokenDecimals *int32
		if contractData.Key.Type == xdr.ScValTypeScvLedgerKeyContractInstance {
			tokenName, tokenSymbol, tokenDecimals = extractTokenMetadata(contractData)
		}

		// Deduplicate key
		dedupeKey := fmt.Sprintf("%s_%s_%s", contractIDStr, keyType, ledgerKeyHash)

		data := ContractDataData{
			ContractId:     contractIDStr,
			LedgerSequence: ledgerSeq,
			LedgerKeyHash:  ledgerKeyHash,

			ContractKeyType:    keyType,
			ContractDurability: durability,

			AssetCode:   assetCode,
			AssetIssuer: assetIssuer,
			AssetType:   assetType,

			BalanceHolder: balanceHolder,
			Balance:       balance,

			LastModifiedLedger: int32(entry.LastModifiedLedgerSeq),
			LedgerEntryChange:  int32(change.Type),
			Deleted:            deleted,
			ClosedAt:           closedAt,

			ContractDataXDR: contractDataXDR,

			TokenName:     tokenName,
			TokenSymbol:   tokenSymbol,
			TokenDecimals: tokenDecimals,

			CreatedAt:   now,
			LedgerRange: ledgerRange,
		}

		contractMap[dedupeKey] = &data
	}

	for _, data := range contractMap {
		contractDataList = append(contractDataList, *data)
	}

	return contractDataList, nil
}

// isContractDataChange checks if a change involves contract data.
func isContractDataChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	return false
}

// nullableString converts empty strings to nil pointers.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	str := s
	return &str
}

// extractTokenMetadata extracts token name, symbol, and decimals from
// a contract instance storage entry's metadata map.
func extractTokenMetadata(contractData xdr.ContractDataEntry) (*string, *string, *int32) {
	// Instance storage has the contract's metadata in the Val field
	if contractData.Val.Type != xdr.ScValTypeScvContractInstance {
		return nil, nil, nil
	}

	instance, ok := contractData.Val.GetInstance()
	if !ok || instance.Storage == nil {
		return nil, nil, nil
	}

	var name, symbol *string
	var decimals *int32

	for _, entry := range *instance.Storage {
		// Look for symbol keys like "METADATA"
		if entry.Key.Type == xdr.ScValTypeScvSymbol && entry.Key.Sym != nil {
			sym := string(*entry.Key.Sym)
			switch sym {
			case "METADATA":
				// METADATA is typically a map with name, symbol, decimals
				if entry.Val.Type == xdr.ScValTypeScvMap && entry.Val.Map != nil {
					for _, item := range **entry.Val.Map {
						if item.Key.Type == xdr.ScValTypeScvSymbol && item.Key.Sym != nil {
							key := string(*item.Key.Sym)
							switch key {
							case "name":
								if item.Val.Type == xdr.ScValTypeScvString && item.Val.Str != nil {
									s := string(*item.Val.Str)
									name = &s
								}
							case "symbol":
								if item.Val.Type == xdr.ScValTypeScvString && item.Val.Str != nil {
									s := string(*item.Val.Str)
									symbol = &s
								}
							case "decimal", "decimals":
								if item.Val.Type == xdr.ScValTypeScvU32 && item.Val.U32 != nil {
									d := int32(*item.Val.U32)
									decimals = &d
								}
							}
						}
					}
				}
			}
		}
	}

	return name, symbol, decimals
}

// ===========================================================================
// Contract Code (WASM)
// ===========================================================================

// extractContractCode extracts contract code (WASM) from a ledger.
func extractContractCode(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractCodeData, error) {
	var contractCodeList []ContractCodeData

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for contract code: %v", err)
		return contractCodeList, nil
	}
	defer changeReader.Close()

	contractCodeMap := make(map[string]*ContractCodeData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for contract code: %v", err)
			continue
		}

		if !isContractCodeChange(change) {
			continue
		}

		var contractCode *xdr.ContractCodeEntry
		var deleted bool
		var lastModifiedLedger uint32
		var ledgerKeyHash string

		if change.Post != nil {
			codeEntry, _ := change.Post.Data.GetContractCode()
			contractCode = &codeEntry
			lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
			deleted = false

			if key, err := change.Post.LedgerKey(); err == nil {
				if keyBytes, err := key.MarshalBinary(); err == nil {
					ledgerKeyHash = hex.EncodeToString(keyBytes)
				}
			}
		} else if change.Pre != nil {
			codeEntry, _ := change.Pre.Data.GetContractCode()
			contractCode = &codeEntry
			lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
			deleted = true

			if key, err := change.Pre.LedgerKey(); err == nil {
				if keyBytes, err := key.MarshalBinary(); err == nil {
					ledgerKeyHash = hex.EncodeToString(keyBytes)
				}
			}
		}

		if contractCode == nil {
			continue
		}

		now := time.Now().UTC()
		codeHashHex := hex.EncodeToString(contractCode.Hash[:])

		var extV int32
		if contractCode.Ext.V == 1 && contractCode.Ext.V1 != nil {
			extV = 1
		}

		wasmMetadata := parseWASMMetadata(contractCode.Code)

		data := ContractCodeData{
			ContractCodeHash: codeHashHex,
			LedgerKeyHash:    ledgerKeyHash,

			ContractCodeExtV: extV,

			LastModifiedLedger: int32(lastModifiedLedger),
			LedgerEntryChange:  int32(change.Type),
			Deleted:            deleted,
			ClosedAt:           closedAt,

			LedgerSequence: ledgerSeq,

			NInstructions:     wasmMetadata.NInstructions,
			NFunctions:        wasmMetadata.NFunctions,
			NGlobals:          wasmMetadata.NGlobals,
			NTableEntries:     wasmMetadata.NTableEntries,
			NTypes:            wasmMetadata.NTypes,
			NDataSegments:     wasmMetadata.NDataSegments,
			NElemSegments:     wasmMetadata.NElemSegments,
			NImports:          wasmMetadata.NImports,
			NExports:          wasmMetadata.NExports,
			NDataSegmentBytes: wasmMetadata.NDataSegmentBytes,

			CreatedAt:   now,
			LedgerRange: ledgerRange,
		}

		contractCodeMap[codeHashHex] = &data
	}

	for _, data := range contractCodeMap {
		contractCodeList = append(contractCodeList, *data)
	}

	return contractCodeList, nil
}

// isContractCodeChange checks if a change involves contract code.
func isContractCodeChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	return false
}

// parseWASMMetadata parses WASM bytecode to extract metadata.
func parseWASMMetadata(wasmCode []byte) WASMMetadata {
	metadata := WASMMetadata{}

	if len(wasmCode) < 8 {
		return metadata
	}

	// Check magic number
	if wasmCode[0] != 0x00 || wasmCode[1] != 0x61 || wasmCode[2] != 0x73 || wasmCode[3] != 0x6D {
		return metadata
	}

	offset := 8 // Skip magic + version

	for offset < len(wasmCode) {
		if offset+1 >= len(wasmCode) {
			break
		}

		sectionType := wasmCode[offset]
		offset++

		sectionSize, bytesRead := decodeLEB128(wasmCode[offset:])
		offset += bytesRead

		if sectionSize < 0 || offset+int(sectionSize) > len(wasmCode) {
			break
		}

		sectionData := wasmCode[offset : offset+int(sectionSize)]
		offset += int(sectionSize)

		switch sectionType {
		case 1: // Type section
			count := countWASMElements(sectionData)
			metadata.NTypes = &count
		case 2: // Import section
			count := countWASMElements(sectionData)
			metadata.NImports = &count
		case 3: // Function section
			count := countWASMElements(sectionData)
			metadata.NFunctions = &count
		case 4: // Table section
			count := countWASMElements(sectionData)
			metadata.NTableEntries = &count
		case 6: // Global section
			count := countWASMElements(sectionData)
			metadata.NGlobals = &count
		case 7: // Export section
			count := countWASMElements(sectionData)
			metadata.NExports = &count
		case 9: // Element section
			count := countWASMElements(sectionData)
			metadata.NElemSegments = &count
		case 10: // Code section (contains instructions)
			count := countWASMElements(sectionData)
			if count > 0 {
				instrCount := int64(len(sectionData))
				metadata.NInstructions = &instrCount
			}
		case 11: // Data section
			count := countWASMElements(sectionData)
			metadata.NDataSegments = &count
			dataSize := int64(len(sectionData))
			metadata.NDataSegmentBytes = &dataSize
		}
	}

	return metadata
}

// countWASMElements counts the number of elements in a WASM section.
func countWASMElements(data []byte) int64 {
	if len(data) == 0 {
		return 0
	}
	count, _ := decodeLEB128(data)
	return count
}

// decodeLEB128 decodes a signed LEB128 encoded integer.
func decodeLEB128(data []byte) (int64, int) {
	var result int64
	var shift uint
	var bytesRead int

	for bytesRead = 0; bytesRead < len(data) && bytesRead < 10; bytesRead++ {
		b := data[bytesRead]
		result |= int64(b&0x7F) << shift

		if b&0x80 == 0 {
			if shift < 64 && (b&0x40) != 0 {
				result |= -(1 << (shift + 7))
			}
			return result, bytesRead + 1
		}

		shift += 7
	}

	return 0, bytesRead
}

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

// extractContractCreations extracts contract creation events from a ledger.
// Looks for InvokeHostFunction ops with HOST_FUNCTION_TYPE_CREATE_CONTRACT or CREATE_CONTRACT_V2.
func extractContractCreations(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ContractCreationData, error) {
	var creations []ContractCreationData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return creations, nil
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		txSourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

		for opIdx, op := range tx.Envelope.Operations() {
			invokeHostFn, ok := op.Body.GetInvokeHostFunctionOp()
			if !ok {
				continue
			}

			fnType := invokeHostFn.HostFunction.Type
			if fnType != xdr.HostFunctionTypeHostFunctionTypeCreateContract &&
				fnType != xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
				continue
			}

			creatorAddress := txSourceAccount
			if op.SourceAccount != nil {
				creatorAddress = op.SourceAccount.ToAccountId().Address()
			}

			// Get the created contract ID from the transaction meta
			var changes xdr.LedgerEntryChanges
			switch tx.UnsafeMeta.V {
			case 3:
				v3 := tx.UnsafeMeta.MustV3()
				if v3.SorobanMeta != nil && opIdx < len(v3.Operations) {
					changes = v3.Operations[opIdx].Changes
				}
			case 4:
				v4 := tx.UnsafeMeta.MustV4()
				if v4.SorobanMeta != nil && opIdx < len(v4.Operations) {
					changes = v4.Operations[opIdx].Changes
				}
			}

			if len(changes) > 0 {
				for _, change := range changes {
					created, ok := change.GetCreated()
					if !ok {
						continue
					}
					contractData, ok := created.Data.GetContractData()
					if !ok {
						continue
					}

					if contractData.Durability == xdr.ContractDataDurabilityPersistent {
						if scAddr, ok := contractData.Contract.GetContractId(); ok {
							contractIDStr, err := strkey.Encode(strkey.VersionByteContract, scAddr[:])
							if err != nil {
								continue
							}

							creation := ContractCreationData{
								ContractID:     contractIDStr,
								CreatorAddress: creatorAddress,
								CreatedLedger:  ledgerSeq,
								CreatedAt:      closedAt,
								LedgerRange:    ledgerRange,
							}

							// Try to extract WASM hash
							if fnType == xdr.HostFunctionTypeHostFunctionTypeCreateContract {
								if args, ok := invokeHostFn.HostFunction.GetCreateContract(); ok {
									if wasmHash, ok := args.Executable.GetWasmHash(); ok {
										wasmHashStr := hex.EncodeToString(wasmHash[:])
										creation.WasmHash = &wasmHashStr
									}
								}
							} else if fnType == xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
								if args, ok := invokeHostFn.HostFunction.GetCreateContractV2(); ok {
									if wasmHash, ok := args.Executable.GetWasmHash(); ok {
										wasmHashStr := hex.EncodeToString(wasmHash[:])
										creation.WasmHash = &wasmHashStr
									}
								}
							}

							creations = append(creations, creation)
							break // One contract per op
						}
					}
				}
			}
		}
	}

	return creations, nil
}
