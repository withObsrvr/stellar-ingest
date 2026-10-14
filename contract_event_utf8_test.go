package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSanitizeContractEventUTF8RepairsDecodedFields(t *testing.T) {
	invalid := string([]byte{0xff, 0xfe, 0x00, 'A'})
	topic1 := invalid
	topic2 := "valid-topic"
	event := &ContractEventData{
		TopicsDecoded: invalid,
		DataDecoded:   invalid,
		Topic0Decoded: &invalid,
		Topic1Decoded: &topic1,
		Topic2Decoded: &topic2,
	}

	repairs := sanitizeContractEventUTF8(event)
	if repairs != 4 {
		t.Fatalf("repairs = %d, want 4", repairs)
	}

	wantMarker := invalidUTF8Base64Prefix + base64.StdEncoding.EncodeToString([]byte(invalid))
	var topicsValue string
	if err := json.Unmarshal([]byte(event.TopicsDecoded), &topicsValue); err != nil {
		t.Fatalf("topics decoded is not a JSON string marker: %q err=%v", event.TopicsDecoded, err)
	}
	if topicsValue != wantMarker {
		t.Fatalf("topics marker = %q, want %q", topicsValue, wantMarker)
	}
	var dataValue string
	if err := json.Unmarshal([]byte(event.DataDecoded), &dataValue); err != nil {
		t.Fatalf("data decoded is not a JSON string marker: %q err=%v", event.DataDecoded, err)
	}
	if dataValue != wantMarker {
		t.Fatalf("data marker = %q, want %q", dataValue, wantMarker)
	}
	if event.Topic0Decoded == nil || *event.Topic0Decoded != wantMarker {
		t.Fatalf("topic0 marker = %v, want %q", event.Topic0Decoded, wantMarker)
	}
	if event.Topic1Decoded == nil || *event.Topic1Decoded != wantMarker {
		t.Fatalf("topic1 marker = %v, want %q", event.Topic1Decoded, wantMarker)
	}
	if event.Topic2Decoded == nil || *event.Topic2Decoded != "valid-topic" {
		t.Fatalf("topic2 mutated = %v", event.Topic2Decoded)
	}
	assertContractEventDecodedUTF8(t, *event)
}

func TestSanitizeContractEventUTF8LeavesValidFieldsUntouched(t *testing.T) {
	topic0 := "symbol:transfer"
	event := &ContractEventData{
		TopicsDecoded: `["transfer"]`,
		DataDecoded:   `{"amount":"10"}`,
		DataXDR:       "AAAA-data-xdr",
		Topic0Decoded: &topic0,
	}

	repairs := sanitizeContractEventUTF8(event)
	if repairs != 0 {
		t.Fatalf("repairs = %d, want 0", repairs)
	}
	if event.TopicsDecoded != `["transfer"]` || event.DataDecoded != `{"amount":"10"}` || event.DataXDR != "AAAA-data-xdr" {
		t.Fatalf("valid fields mutated: %+v", event)
	}
	if event.Topic0Decoded == nil || *event.Topic0Decoded != "symbol:transfer" {
		t.Fatalf("topic0 mutated: %v", event.Topic0Decoded)
	}
}

func TestParquetContractEventFromDataSanitizesDecodedFields(t *testing.T) {
	invalid := string([]byte{0xff, 0xfe, 'A'})
	event := ContractEventData{
		EventID:                  "123-0-0",
		LedgerSequence:           123,
		TransactionHash:          "txhash",
		ClosedAt:                 time.Unix(1, 0).UTC(),
		EventType:                "contract",
		InSuccessfulContractCall: true,
		Successful:               true,
		ContractEventXDR:         "AAAA-event-xdr",
		TopicsJSON:               "[]",
		TopicsDecoded:            invalid,
		DataXDR:                  "AAAA-data-xdr",
		DataDecoded:              invalid,
		TopicCount:               1,
		Topic0Decoded:            &invalid,
		OperationIndex:           2,
		EventIndex:               3,
		CreatedAt:                time.Unix(1, 0).UTC(),
		LedgerRange:              0,
	}

	row := parquetContractEventFromData(event, "test-version")
	if row.PipelineVersion != "test-version" {
		t.Fatalf("pipeline version = %q", row.PipelineVersion)
	}
	if row.ContractEventXDR != "AAAA-event-xdr" || row.DataXDR != "AAAA-data-xdr" {
		t.Fatalf("xdr fields mutated: event=%q data=%q", row.ContractEventXDR, row.DataXDR)
	}
	assertParquetContractEventUTF8(t, row)
}

func assertContractEventDecodedUTF8(t *testing.T, event ContractEventData) {
	t.Helper()
	values := map[string]string{
		"topics_decoded": event.TopicsDecoded,
		"data_decoded":   event.DataDecoded,
	}
	for name, value := range values {
		if !utf8.ValidString(value) {
			t.Fatalf("%s is invalid UTF-8", name)
		}
	}
	for name, value := range map[string]*string{
		"topic0_decoded": event.Topic0Decoded,
		"topic1_decoded": event.Topic1Decoded,
		"topic2_decoded": event.Topic2Decoded,
		"topic3_decoded": event.Topic3Decoded,
	} {
		if value != nil && !utf8.ValidString(*value) {
			t.Fatalf("%s is invalid UTF-8", name)
		}
	}
}

func assertParquetContractEventUTF8(t *testing.T, event ParquetContractEvent) {
	t.Helper()
	values := map[string]string{
		"event_id":           event.EventID,
		"transaction_hash":   event.TransactionHash,
		"event_type":         event.EventType,
		"contract_event_xdr": event.ContractEventXDR,
		"topics_json":        event.TopicsJSON,
		"topics_decoded":     event.TopicsDecoded,
		"data_xdr":           event.DataXDR,
		"data_decoded":       event.DataDecoded,
		"version_label":      event.PipelineVersion,
	}
	for name, value := range values {
		if !utf8.ValidString(value) {
			t.Fatalf("%s is invalid UTF-8", name)
		}
	}
	for name, value := range map[string]*string{
		"contract_id":    event.ContractID,
		"topic0_decoded": event.Topic0Decoded,
		"topic1_decoded": event.Topic1Decoded,
		"topic2_decoded": event.Topic2Decoded,
		"topic3_decoded": event.Topic3Decoded,
		"era_id":         event.EraID,
	} {
		if value != nil && !utf8.ValidString(*value) {
			t.Fatalf("%s is invalid UTF-8", name)
		}
	}
}
