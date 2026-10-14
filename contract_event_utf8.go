package main

import (
	"encoding/base64"
	"encoding/json"
	"unicode/utf8"
)

const invalidUTF8Base64Prefix = "invalid_utf8_base64:"

func sanitizeContractEventUTF8(event *ContractEventData) int {
	if event == nil {
		return 0
	}
	repairs := 0
	if value, ok := sanitizeContractEventJSONText(event.TopicsDecoded); ok {
		event.TopicsDecoded = value
		repairs++
	}
	if value, ok := sanitizeContractEventJSONText(event.DataDecoded); ok {
		event.DataDecoded = value
		repairs++
	}
	repairs += sanitizeContractEventTextPtr(&event.Topic0Decoded)
	repairs += sanitizeContractEventTextPtr(&event.Topic1Decoded)
	repairs += sanitizeContractEventTextPtr(&event.Topic2Decoded)
	repairs += sanitizeContractEventTextPtr(&event.Topic3Decoded)
	return repairs
}

func sanitizeContractEventJSONText(value string) (string, bool) {
	if utf8.ValidString(value) {
		return value, false
	}
	marker := invalidUTF8Marker(value)
	encoded, err := json.Marshal(marker)
	if err != nil {
		return marker, true
	}
	return string(encoded), true
}

func sanitizeContractEventTextPtr(value **string) int {
	if value == nil || *value == nil || utf8.ValidString(**value) {
		return 0
	}
	repaired := invalidUTF8Marker(**value)
	*value = &repaired
	return 1
}

func invalidUTF8Marker(value string) string {
	return invalidUTF8Base64Prefix + base64.StdEncoding.EncodeToString([]byte(value))
}

func parquetContractEventFromData(event ContractEventData, pipelineVersion string) ParquetContractEvent {
	sanitizeContractEventUTF8(&event)
	return ParquetContractEvent{
		EventID:                  event.EventID,
		ContractID:               event.ContractID,
		LedgerSequence:           event.LedgerSequence,
		TransactionHash:          event.TransactionHash,
		ClosedAt:                 event.ClosedAt.UnixMicro(),
		EventType:                event.EventType,
		InSuccessfulContractCall: event.InSuccessfulContractCall,
		Successful:               event.Successful,
		ContractEventXDR:         event.ContractEventXDR,
		TopicsJSON:               event.TopicsJSON,
		TopicsDecoded:            event.TopicsDecoded,
		DataXDR:                  event.DataXDR,
		DataDecoded:              event.DataDecoded,
		TopicCount:               event.TopicCount,
		Topic0Decoded:            event.Topic0Decoded,
		Topic1Decoded:            event.Topic1Decoded,
		Topic2Decoded:            event.Topic2Decoded,
		Topic3Decoded:            event.Topic3Decoded,
		OperationIndex:           event.OperationIndex,
		EventIndex:               event.EventIndex,
		CreatedAt:                event.CreatedAt.UnixMicro(),
		LedgerRange:              event.LedgerRange,
		EraID:                    event.EraID,
		PipelineVersion:          pipelineVersion,
	}
}
