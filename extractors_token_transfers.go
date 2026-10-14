package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractTokenTransfers extracts unified token transfer events from the
// already-decoded LedgerCloseMeta using the SDK's token_transfer.EventsProcessor.
// Covers transfer, mint, burn, clawback, and fee events from both classic and
// Soroban operations.
func extractTokenTransfers(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TokenTransferData, error) {
	eventsProcessor := token_transfer.NewEventsProcessorForUnifiedEvents(networkPassphrase)

	events, err := eventsProcessor.EventsFromLedger(lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to extract token transfer events: %w", err)
	}

	if verifyErr := token_transfer.VerifyEvents(lcm, networkPassphrase, true); verifyErr != nil {
		log.Printf("Warning: Token transfer event verification failed for ledger %d: %v", ledgerSeq, verifyErr)
	}

	var transfers []TokenTransferData

	for _, event := range events {
		var from, to *string
		var amount string

		switch evt := event.Event.(type) {
		case *token_transfer.TokenTransferEvent_Transfer:
			from = strPtr(evt.Transfer.From)
			to = strPtr(evt.Transfer.To)
			amount = evt.Transfer.Amount
		case *token_transfer.TokenTransferEvent_Mint:
			to = strPtr(evt.Mint.To)
			amount = evt.Mint.Amount
		case *token_transfer.TokenTransferEvent_Burn:
			from = strPtr(evt.Burn.From)
			amount = evt.Burn.Amount
		case *token_transfer.TokenTransferEvent_Clawback:
			from = strPtr(evt.Clawback.From)
			amount = evt.Clawback.Amount
		case *token_transfer.TokenTransferEvent_Fee:
			from = strPtr(evt.Fee.From)
			amount = evt.Fee.Amount
		default:
			log.Printf("Warning: Unknown token transfer event type in ledger %d", ledgerSeq)
			continue
		}

		amountFloat, _ := strconv.ParseFloat(amount, 64)
		amountFloat = amountFloat * 0.0000001

		eventMeta := event.GetMeta()
		transactionID := toid.New(int32(ledgerSeq), int32(eventMeta.TransactionIndex), 0).ToInt64()

		var operationID *int64
		var operationIndex *int32
		if eventMeta.OperationIndex != nil {
			opIdx := int32(*eventMeta.OperationIndex)
			opID := toid.New(int32(ledgerSeq), int32(eventMeta.TransactionIndex), opIdx).ToInt64()
			operationID = &opID
			operationIndex = &opIdx
		}

		asset, assetType, assetCode, assetIssuer := getAssetFromTokenTransferEvent(event)

		transfers = append(transfers, TokenTransferData{
			LedgerSequence:  ledgerSeq,
			TransactionHash: eventMeta.TxHash,
			TransactionID:   transactionID,
			OperationID:     operationID,
			OperationIndex:  operationIndex,
			EventType:       event.GetEventType(),
			From:            from,
			To:              to,
			Asset:           asset,
			AssetType:       assetType,
			AssetCode:       assetCode,
			AssetIssuer:     assetIssuer,
			Amount:          amountFloat,
			AmountRaw:       amount,
			ContractID:      eventMeta.ContractAddress,
			ClosedAt:        closedAt,
			CreatedAt:       time.Now().UTC(),
			LedgerRange:     ledgerRange,
		})
	}

	return transfers, nil
}

func getAssetFromTokenTransferEvent(event *token_transfer.TokenTransferEvent) (assetConcat, assetType string, assetCode, assetIssuer *string) {
	if event.GetAsset().GetNative() {
		return "native", "native", nil, nil
	}

	issued := event.GetAsset().GetIssuedAsset()
	if issued != nil {
		if len(issued.AssetCode) > 4 {
			assetType = "credit_alphanum12"
		} else {
			assetType = "credit_alphanum4"
		}
		assetCode = &issued.AssetCode
		assetIssuer = &issued.Issuer
		assetConcat = fmt.Sprintf("%s:%s:%s", assetType, issued.AssetCode, issued.Issuer)
		return
	}

	return "unknown", "unknown", nil, nil
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
