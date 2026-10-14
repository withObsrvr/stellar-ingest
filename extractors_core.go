package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ---------------------------------------------------------------------------
// Core Extractors: Transactions, Operations, Effects, Trades
// Adapted from stellar-postgres-ingester/go/extractors.go
// ---------------------------------------------------------------------------

// extractTransactions extracts transaction data from a pre-decoded ledger.
func extractTransactions(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TransactionData, error) {
	var transactions []TransactionData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	var txIndex int32 = 1 // 1-indexed per TOID convention
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txData := TransactionData{
			LedgerSequence:        ledgerSeq,
			TransactionHash:       hex.EncodeToString(tx.Result.TransactionHash[:]),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
			SourceAccountMuxed:    getMuxedAddress(tx.Envelope.SourceAccount()),
			FeeCharged:            int64(tx.Result.Result.FeeCharged),
			MaxFee:                int64(tx.Envelope.Fee()),
			Successful:            tx.Result.Successful(),
			TransactionResultCode: tx.Result.Result.Result.Code.String(),
			OperationCount:        len(tx.Envelope.Operations()),
			CreatedAt:             closedAt,
			AccountSequence:       int64(tx.Envelope.SeqNum()),
			LedgerRange:           ledgerRange,
			SignaturesCount:       len(tx.Envelope.Signatures()),
			NewAccount:            false,
		}
		if err := populateTransactionXDRFields(tx, &txData); err != nil {
			return nil, fmt.Errorf("failed to marshal transaction XDR for ledger %d tx %s: %w", ledgerSeq, txData.TransactionHash, err)
		}

		// Extract timebounds. Stored as BIGINT in v3_bronze_schema.sql, so
		// we keep them as int64 (matching PG/DuckLake) instead of formatting
		// to a string.
		if tb := tx.Envelope.TimeBounds(); tb != nil {
			minTime := int64(tb.MinTime)
			txData.TimeboundsMinTime = &minTime
			maxTime := int64(tb.MaxTime)
			txData.TimeboundsMaxTime = &maxTime
		}

		// Extract Soroban host function type and contract ID from operations
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
					fnType := invokeOp.HostFunction.Type.String()
					txData.SorobanHostFunctionType = &fnType
					// Extract contract ID from soroban meta if available
					if invokeOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract && invokeOp.HostFunction.InvokeContract != nil {
						contractIDStr, err := invokeOp.HostFunction.InvokeContract.ContractAddress.String()
						if err == nil && contractIDStr != "" {
							txData.SorobanContractID = &contractIDStr
						}
					}
				}
				break // Only need the first InvokeHostFunction op
			}
		}

		// Extract memo
		memo := tx.Envelope.Memo()
		switch memo.Type {
		case xdr.MemoTypeMemoNone:
			memoType := "none"
			txData.MemoType = &memoType
		case xdr.MemoTypeMemoText:
			// Stellar MemoText is opaque bytes, not guaranteed UTF-8. DuckDB rejects
			// invalid UTF8-annotated Parquet strings, so encode all text memos as
			// base64 for Bronze history-loader output.
			memoType := "text_base64"
			txData.MemoType = &memoType
			if text, ok := memo.GetText(); ok {
				memoText := base64.StdEncoding.EncodeToString([]byte(text))
				txData.Memo = &memoText
			}
		case xdr.MemoTypeMemoId:
			memoType := "id"
			txData.MemoType = &memoType
			if id, ok := memo.GetId(); ok {
				memoStr := fmt.Sprintf("%d", id)
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoHash:
			memoType := "hash"
			txData.MemoType = &memoType
			if hash, ok := memo.GetHash(); ok {
				memoStr := hex.EncodeToString(hash[:])
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoReturn:
			memoType := "return"
			txData.MemoType = &memoType
			if ret, ok := memo.GetRetHash(); ok {
				memoStr := hex.EncodeToString(ret[:])
				txData.Memo = &memoStr
			}
		}

		// Check for CREATE_ACCOUNT operation
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeCreateAccount {
				txData.NewAccount = true
				break
			}
		}

		// Extract Soroban rent fee charged (C13)
		switch tx.UnsafeMeta.V {
		case 3:
			v3 := tx.UnsafeMeta.MustV3()
			if v3.SorobanMeta != nil && v3.SorobanMeta.Ext.V == 1 && v3.SorobanMeta.Ext.V1 != nil {
				rentFee := int64(v3.SorobanMeta.Ext.V1.RentFeeCharged)
				txData.RentFeeCharged = &rentFee
			}
		case 4:
			v4 := tx.UnsafeMeta.MustV4()
			if v4.SorobanMeta != nil && v4.SorobanMeta.Ext.V == 1 && v4.SorobanMeta.Ext.V1 != nil {
				rentFee := int64(v4.SorobanMeta.Ext.V1.RentFeeCharged)
				txData.RentFeeCharged = &rentFee
			}
		}

		// Extract Soroban resource fields from envelope
		if instructions, ok := tx.SorobanResourcesInstructions(); ok {
			val := int64(instructions)
			txData.SorobanResourcesInstructions = &val
		}
		if readBytes, ok := tx.SorobanResourcesDiskReadBytes(); ok {
			val := int64(readBytes)
			txData.SorobanResourcesReadBytes = &val
		}
		if writeBytes, ok := tx.SorobanResourcesWriteBytes(); ok {
			val := int64(writeBytes)
			txData.SorobanResourcesWriteBytes = &val
		}

		// Compute TOID
		txData.TransactionID = toid.New(int32(ledgerSeq), txIndex, 0).ToInt64()

		transactions = append(transactions, txData)
		txIndex++
	}

	return transactions, nil
}

func populateTransactionXDRFields(tx ingest.LedgerTransaction, txData *TransactionData) error {
	envelopeBase64, err := xdr.MarshalBase64(tx.Envelope)
	if err != nil {
		return err
	}
	envelopeSize, err := canonicalStoredTransactionEnvelopeSize(envelopeBase64)
	if err != nil {
		return err
	}
	txData.EnvelopeSizeBytes = envelopeSize
	resultBase64, err := xdr.MarshalBase64(&tx.Result.Result)
	if err != nil {
		return err
	}
	metaBase64, err := xdr.MarshalBase64(tx.UnsafeMeta)
	if err != nil {
		return err
	}
	feeMetaBase64, err := xdr.MarshalBase64(tx.FeeChanges)
	if err != nil {
		return err
	}
	signersJSON, err := marshalDecoratedSignatures(tx.Envelope.Signatures())
	if err != nil {
		return err
	}

	txData.TxEnvelope = &envelopeBase64
	txData.TxResult = &resultBase64
	txData.TxMeta = &metaBase64
	txData.TxFeeMeta = &feeMetaBase64
	txData.TxSigners = &signersJSON
	return nil
}

func canonicalTransactionEnvelopeSize(envelope xdr.TransactionEnvelope) (int, error) {
	// MarshalBase64 is the same canonical XDR serialization used for the stored
	// tx_envelope. Decode it before measuring so this remains a binary byte
	// count, never the length of the base64 text.
	encoded, err := xdr.MarshalBase64(envelope)
	if err != nil {
		return 0, err
	}
	return canonicalStoredTransactionEnvelopeSize(encoded)
}

func canonicalStoredTransactionEnvelopeSize(encoded string) (int, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, err
	}
	return len(raw), nil
}

func marshalDecoratedSignatures(signatures []xdr.DecoratedSignature) (string, error) {
	out := make([]string, len(signatures))
	for i, sig := range signatures {
		out[i] = base64.StdEncoding.EncodeToString(sig.Signature)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// extractOperations extracts operation data from a pre-decoded ledger.
func extractOperations(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]OperationData, error) {
	var operations []OperationData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	var txIndex int32 = 1 // 1-indexed per TOID convention
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for operations in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()
		txTOID := toid.New(int32(ledgerSeq), txIndex, 0).ToInt64()

		for i, op := range tx.Envelope.Operations() {
			// Get source account (defaults to transaction source if not specified)
			sourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()
			var sourceAccountMuxed *string
			if op.SourceAccount != nil {
				sourceAccount = op.SourceAccount.ToAccountId().Address()
				sourceAccountMuxed = getMuxedAddress(*op.SourceAccount)
			} else {
				sourceAccountMuxed = getMuxedAddress(tx.Envelope.SourceAccount())
			}

			opData := OperationData{
				TransactionHash:       txHash,
				TransactionIndex:      int(txIndex),
				OperationIndex:        i,
				LedgerSequence:        ledgerSeq,
				SourceAccount:         sourceAccount,
				SourceAccountMuxed:    sourceAccountMuxed,
				OpType:                int(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           ledgerRange,
				TransactionID:         txTOID,
				OperationID:           toid.New(int32(ledgerSeq), txIndex, int32(i+1)).ToInt64(),
			}

			// Extract operation-specific fields
			switch op.Body.Type {
			case xdr.OperationTypeCreateAccount:
				if createAcct, ok := op.Body.GetCreateAccountOp(); ok {
					startBal := int64(createAcct.StartingBalance)
					opData.StartingBalance = &startBal
					opData.Amount = &startBal

					dest := createAcct.Destination.Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePayment:
				if payment, ok := op.Body.GetPaymentOp(); ok {
					amount := int64(payment.Amount)
					opData.Amount = &amount
					setAssetFields(&opData, payment.Asset)

					dest := payment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePathPaymentStrictReceive:
				if pp, ok := op.Body.GetPathPaymentStrictReceiveOp(); ok {
					amount := int64(pp.DestAmount)
					opData.Amount = &amount
					setAssetFields(&opData, pp.DestAsset)

					srcAmt := int64(pp.SendMax)
					opData.SourceAmount = &srcAmt
					setSourceAssetFields(&opData, pp.SendAsset)

					dest := pp.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeManageSellOffer:
				if sellOffer, ok := op.Body.GetManageSellOfferOp(); ok {
					amount := int64(sellOffer.Amount)
					opData.Amount = &amount
					setSellingAssetFields(&opData, sellOffer.Selling)
					setBuyingAssetFields(&opData, sellOffer.Buying)
					offerID := int64(sellOffer.OfferId)
					opData.OfferID = &offerID
					price := fmt.Sprintf("%d/%d", sellOffer.Price.N, sellOffer.Price.D)
					opData.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", sellOffer.Price.N, sellOffer.Price.D)
					opData.PriceR = &priceR
				}

			case xdr.OperationTypeCreatePassiveSellOffer:
				if passiveOffer, ok := op.Body.GetCreatePassiveSellOfferOp(); ok {
					amount := int64(passiveOffer.Amount)
					opData.Amount = &amount
					setSellingAssetFields(&opData, passiveOffer.Selling)
					setBuyingAssetFields(&opData, passiveOffer.Buying)
					price := fmt.Sprintf("%d/%d", passiveOffer.Price.N, passiveOffer.Price.D)
					opData.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", passiveOffer.Price.N, passiveOffer.Price.D)
					opData.PriceR = &priceR
				}

			case xdr.OperationTypeSetOptions:
				if setOpts, ok := op.Body.GetSetOptionsOp(); ok {
					if setOpts.HomeDomain != nil {
						hd := string(*setOpts.HomeDomain)
						opData.HomeDomain = &hd
					}
					if setOpts.SetFlags != nil {
						sf := int(*setOpts.SetFlags)
						opData.SetFlags = &sf
					}
					if setOpts.ClearFlags != nil {
						cf := int(*setOpts.ClearFlags)
						opData.ClearFlags = &cf
					}
					if setOpts.MasterWeight != nil {
						mw := int(*setOpts.MasterWeight)
						opData.MasterWeight = &mw
					}
					if setOpts.LowThreshold != nil {
						lt := int(*setOpts.LowThreshold)
						opData.LowThreshold = &lt
					}
					if setOpts.MedThreshold != nil {
						mt := int(*setOpts.MedThreshold)
						opData.MediumThreshold = &mt
					}
					if setOpts.HighThreshold != nil {
						ht := int(*setOpts.HighThreshold)
						opData.HighThreshold = &ht
					}
				}

			case xdr.OperationTypeChangeTrust:
				if changeTrust, ok := op.Body.GetChangeTrustOp(); ok {
					limit := int64(changeTrust.Limit)
					opData.TrustlineLimit = &limit
					// ChangeTrust line can be Asset or LiquidityPoolParameters
					if changeTrust.Line.Type == xdr.AssetTypeAssetTypePoolShare {
						assetStr := "liquidity_pool_shares"
						opData.Asset = &assetStr
						aType := "liquidity_pool_shares"
						opData.AssetType = &aType
					} else {
						asset := changeTrust.Line.ToAsset()
						setAssetFields(&opData, asset)
					}
				}

			case xdr.OperationTypeAllowTrust:
				if allowTrust, ok := op.Body.GetAllowTrustOp(); ok {
					dest := allowTrust.Trustor.Address()
					opData.Destination = &dest
					// Extract asset code from AssetCode union
					var code string
					if ac4, ok := allowTrust.Asset.GetAssetCode4(); ok {
						code = strings.TrimRight(string(ac4[:]), "\x00")
					} else if ac12, ok := allowTrust.Asset.GetAssetCode12(); ok {
						code = strings.TrimRight(string(ac12[:]), "\x00")
					}
					if code != "" {
						opData.AssetCode = &code
					}
					flags := int(allowTrust.Authorize)
					opData.SetFlags = &flags
				}

			case xdr.OperationTypeAccountMerge:
				// AccountMerge destination is in the body itself
				if destAccount, ok := op.Body.GetDestination(); ok {
					dest := destAccount.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeManageData:
				if manageData, ok := op.Body.GetManageDataOp(); ok {
					name := string(manageData.DataName)
					opData.DataName = &name
					if manageData.DataValue != nil {
						val := base64.StdEncoding.EncodeToString(*manageData.DataValue)
						opData.DataValue = &val
					}
				}

			case xdr.OperationTypeBumpSequence:
				if bumpSeq, ok := op.Body.GetBumpSequenceOp(); ok {
					bumpTo := int64(bumpSeq.BumpTo)
					opData.BumpTo = &bumpTo
				}

			case xdr.OperationTypeManageBuyOffer:
				if buyOffer, ok := op.Body.GetManageBuyOfferOp(); ok {
					amount := int64(buyOffer.BuyAmount)
					opData.Amount = &amount
					setSellingAssetFields(&opData, buyOffer.Selling)
					setBuyingAssetFields(&opData, buyOffer.Buying)
					offerID := int64(buyOffer.OfferId)
					opData.OfferID = &offerID
					price := fmt.Sprintf("%d/%d", buyOffer.Price.N, buyOffer.Price.D)
					opData.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", buyOffer.Price.N, buyOffer.Price.D)
					opData.PriceR = &priceR
				}

			case xdr.OperationTypePathPaymentStrictSend:
				if pp, ok := op.Body.GetPathPaymentStrictSendOp(); ok {
					amount := int64(pp.SendAmount)
					opData.Amount = &amount
					setSourceAssetFields(&opData, pp.SendAsset)
					setAssetFields(&opData, pp.DestAsset)

					destMin := int64(pp.DestMin)
					opData.DestinationMin = &destMin

					dest := pp.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeCreateClaimableBalance:
				if createCB, ok := op.Body.GetCreateClaimableBalanceOp(); ok {
					amount := int64(createCB.Amount)
					opData.Amount = &amount
					setAssetFields(&opData, createCB.Asset)
				}

			case xdr.OperationTypeClaimClaimableBalance:
				if claimCB, ok := op.Body.GetClaimClaimableBalanceOp(); ok {
					balanceID, err := xdr.MarshalHex(claimCB.BalanceId)
					if err == nil {
						opData.BalanceID = &balanceID
					}
				}

			case xdr.OperationTypeBeginSponsoringFutureReserves:
				if beginSponsoring, ok := op.Body.GetBeginSponsoringFutureReservesOp(); ok {
					sponsored := beginSponsoring.SponsoredId.Address()
					opData.SponsoredID = &sponsored
				}

			case xdr.OperationTypeEndSponsoringFutureReserves:
				// No fields to extract

			case xdr.OperationTypeRevokeSponsorship:
				// Revoke sponsorship - complex type, just record the op type

			case xdr.OperationTypeClawback:
				if clawback, ok := op.Body.GetClawbackOp(); ok {
					amount := int64(clawback.Amount)
					opData.Amount = &amount
					setAssetFields(&opData, clawback.Asset)
					dest := clawback.From.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeSetTrustLineFlags:
				if setTLFlags, ok := op.Body.GetSetTrustLineFlagsOp(); ok {
					dest := setTLFlags.Trustor.Address()
					opData.Destination = &dest
					setAssetFields(&opData, setTLFlags.Asset)
					sf := int(setTLFlags.SetFlags)
					opData.SetFlags = &sf
					cf := int(setTLFlags.ClearFlags)
					opData.ClearFlags = &cf
				}

			case xdr.OperationTypeLiquidityPoolDeposit:
				if lpDeposit, ok := op.Body.GetLiquidityPoolDepositOp(); ok {
					amount := int64(lpDeposit.MaxAmountA)
					opData.Amount = &amount
				}

			case xdr.OperationTypeLiquidityPoolWithdraw:
				if lpWithdraw, ok := op.Body.GetLiquidityPoolWithdrawOp(); ok {
					amount := int64(lpWithdraw.Amount)
					opData.Amount = &amount
				}

			case xdr.OperationTypeExtendFootprintTtl:
				// ExtendFootprintTtl - no operation-specific extraction needed

			case xdr.OperationTypeRestoreFootprint:
				// RestoreFootprint - no operation-specific extraction needed
			}

			// Get operation result code if available
			if txSuccessful {
				if opResults, ok := tx.Result.Result.OperationResults(); ok {
					if i < len(opResults) {
						resultCode := opResults[i].Code.String()
						opData.OperationResultCode = &resultCode
					}
				}
			}

			// Extract contract invocation details for InvokeHostFunction operations (type 24)
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				var hostFunctionType xdr.HostFunctionType
				if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
					hostFunctionType = invokeOp.HostFunction.Type
					fnType := invokeOp.HostFunction.Type.String()
					opData.SorobanOperation = &fnType
					// Check if auth is required
					authRequired := len(invokeOp.Auth) > 0
					opData.SorobanAuthRequired = &authRequired
				}
				contractID, functionName, argsJSON, err := extractContractInvocationDetails(op)
				if err != nil {
					log.Printf("Warning: Failed to extract contract invocation details for op %s:%d: %v", txHash, i, err)
				}
				opData.SorobanContractID = contractID
				opData.SorobanFunction = functionName
				opData.SorobanArgumentsJSON = argsJSON
				if contractID == nil && (hostFunctionType == xdr.HostFunctionTypeHostFunctionTypeCreateContract ||
					hostFunctionType == xdr.HostFunctionTypeHostFunctionTypeCreateContractV2) {
					createdContractID, err := extractCreatedContractIDFromMeta(tx.UnsafeMeta, i)
					if err != nil {
						log.Printf("Warning: Failed to extract created contract ID for op %s:%d: %v", txHash, i, err)
					} else {
						opData.SorobanContractID = createdContractID
					}
				}

				// Call graph and Soroban auth credential extraction
				if err := integrateCallGraph(tx, i, op, &opData); err != nil {
					log.Printf("Warning: Failed to integrate call graph for op %s:%d: %v", txHash, i, err)
				}
			}

			operations = append(operations, opData)
		}

		txIndex++
	}

	return operations, nil
}

// extractTrades extracts trades from a pre-decoded ledger.
// Extracts trade data from MANAGE_SELL_OFFER, MANAGE_BUY_OFFER, and CREATE_PASSIVE_SELL_OFFER operations.
func extractTrades(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]TradeData, error) {
	var trades []TradeData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for trades in ledger %d: %v", ledgerSeq, err)
			continue
		}

		if !tx.Result.Successful() {
			continue // Only process successful transactions
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		for opIdx, op := range tx.Envelope.Operations() {
			if opResults, ok := tx.Result.Result.OperationResults(); ok {
				if opIdx >= len(opResults) {
					continue
				}
				opResult := opResults[opIdx]

				tradeIndex := 0
				switch op.Body.Type {
				case xdr.OperationTypeManageSellOffer, xdr.OperationTypeManageBuyOffer,
					xdr.OperationTypeCreatePassiveSellOffer:

					var offerResult *xdr.ManageOfferSuccessResult
					switch opResult.Code {
					case xdr.OperationResultCodeOpInner:
						tr := opResult.MustTr()
						switch tr.Type {
						case xdr.OperationTypeManageSellOffer:
							if r, ok := tr.GetManageSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeManageBuyOffer:
							if r, ok := tr.GetManageBuyOfferResult(); ok && r.Code == xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeCreatePassiveSellOffer:
							if r, ok := tr.GetCreatePassiveSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						}
					}

					if offerResult != nil {
						for _, claimAtom := range offerResult.OffersClaimed {
							var sellerAccount, sellingAmount, buyingAmount string
							var sellingCode, sellingIssuer, buyingCode, buyingIssuer *string

							switch claimAtom.Type {
							case xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
								ob := claimAtom.MustOrderBook()
								sellerAccount = ob.SellerId.Address()
								sellingAmount = fmt.Sprintf("%d", ob.AmountSold)
								buyingAmount = fmt.Sprintf("%d", ob.AmountBought)

								// Parse selling asset
								if ob.AssetSold.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetSold.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetSold.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetSold.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									}
								}

								// Parse buying asset
								if ob.AssetBought.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetBought.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetBought.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetBought.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									}
								}

							case xdr.ClaimAtomTypeClaimAtomTypeV0:
								v0 := claimAtom.MustV0()
								sellerAccount = fmt.Sprintf("%x", v0.SellerEd25519)
								sellingAmount = fmt.Sprintf("%d", v0.AmountSold)
								buyingAmount = fmt.Sprintf("%d", v0.AmountBought)
							}

							buyerAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

							trades = append(trades, TradeData{
								LedgerSequence:     ledgerSeq,
								TransactionHash:    txHash,
								OperationIndex:     opIdx,
								TradeIndex:         tradeIndex,
								TradeType:          "orderbook",
								TradeTimestamp:     closedAt,
								SellerAccount:      sellerAccount,
								SellingAssetCode:   sellingCode,
								SellingAssetIssuer: sellingIssuer,
								SellingAmount:      sellingAmount,
								BuyerAccount:       buyerAccount,
								BuyingAssetCode:    buyingCode,
								BuyingAssetIssuer:  buyingIssuer,
								BuyingAmount:       buyingAmount,
								Price:              fmt.Sprintf("%s/%s", buyingAmount, sellingAmount),
								CreatedAt:          closedAt,
								LedgerRange:        ledgerRange,
							})
							tradeIndex++
						}
					}
				}
			}
		}
	}

	return trades, nil
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

// decomposeAsset splits an XDR asset into type, code, and issuer components.
func decomposeAsset(asset xdr.Asset) (assetType, assetCode, assetIssuer string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", "", ""
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if a4, ok := asset.GetAlphaNum4(); ok {
			code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
			return "credit_alphanum4", code, a4.Issuer.Address()
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if a12, ok := asset.GetAlphaNum12(); ok {
			code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
			return "credit_alphanum12", code, a12.Issuer.Address()
		}
	}
	return "", "", ""
}

// setAssetFields populates decomposed asset fields on an OperationData from an XDR asset.
func setAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.Asset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.AssetType = &aType
	if aCode != "" {
		opData.AssetCode = &aCode
	}
	if aIssuer != "" {
		opData.AssetIssuer = &aIssuer
	}
}

// setSourceAssetFields populates source asset fields on an OperationData from an XDR asset.
func setSourceAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.SourceAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.SourceAssetType = &aType
	if aCode != "" {
		opData.SourceAssetCode = &aCode
	}
	if aIssuer != "" {
		opData.SourceAssetIssuer = &aIssuer
	}
}

// setBuyingAssetFields populates buying asset fields on an OperationData from an XDR asset.
func setBuyingAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.BuyingAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.BuyingAssetType = &aType
	if aCode != "" {
		opData.BuyingAssetCode = &aCode
	}
	if aIssuer != "" {
		opData.BuyingAssetIssuer = &aIssuer
	}
}

// setSellingAssetFields populates selling asset fields on an OperationData from an XDR asset.
func setSellingAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.SellingAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.SellingAssetType = &aType
	if aCode != "" {
		opData.SellingAssetCode = &aCode
	}
	if aIssuer != "" {
		opData.SellingAssetIssuer = &aIssuer
	}
}

// getMuxedAddress returns the muxed account string if the account is muxed, nil otherwise.
func getMuxedAddress(account xdr.MuxedAccount) *string {
	if account.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		addr := account.Address()
		return &addr
	}
	return nil
}

// getAssetString returns the canonical string representation of an XDR asset.
func getAssetString(asset xdr.Asset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if a4, ok := asset.GetAlphaNum4(); ok {
			code := string(a4.AssetCode[:])
			issuer := a4.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if a12, ok := asset.GetAlphaNum12(); ok {
			code := string(a12.AssetCode[:])
			issuer := a12.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	}
	return ""
}

// marshalToBase64 encodes an XDR-marshallable value to a base64 string.
func marshalToBase64(v interface{ MarshalBinary() ([]byte, error) }) *string {
	if bytes, err := v.MarshalBinary(); err == nil {
		encoded := base64.StdEncoding.EncodeToString(bytes)
		return &encoded
	}
	return nil
}

// extractContractInvocationDetails extracts contract ID, function name, and arguments
// from an InvokeHostFunction operation.
func extractContractInvocationDetails(op xdr.Operation) (*string, *string, *string, error) {
	invokeOp, ok := op.Body.GetInvokeHostFunctionOp()
	if !ok {
		return nil, nil, nil, nil
	}

	if invokeOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return nil, nil, nil, nil
	}

	if invokeOp.HostFunction.InvokeContract == nil {
		return nil, nil, nil, nil
	}

	invokeContract := invokeOp.HostFunction.InvokeContract

	// Extract contract address
	var contractID *string
	contractIDStr, err := invokeContract.ContractAddress.String()
	if err == nil && contractIDStr != "" {
		contractID = &contractIDStr
	}

	// Extract function name
	var functionName *string
	if invokeContract.FunctionName != "" {
		fnName := string(invokeContract.FunctionName)
		functionName = &fnName
	}

	// Extract arguments - encode each ScVal as base64 XDR for portability
	// (avoids dependency on the full ScVal-to-JSON converter)
	args := invokeContract.Args
	var argsJSON []interface{}
	for _, arg := range args {
		argBytes, marshalErr := arg.MarshalBinary()
		if marshalErr != nil {
			log.Printf("Warning: Failed to marshal ScVal arg: %v", marshalErr)
			argsJSON = append(argsJSON, map[string]interface{}{
				"error": marshalErr.Error(),
				"type":  arg.Type.String(),
			})
		} else {
			argsJSON = append(argsJSON, map[string]interface{}{
				"type":    arg.Type.String(),
				"xdr_b64": base64.StdEncoding.EncodeToString(argBytes),
			})
		}
	}

	var argsStr *string
	if len(argsJSON) > 0 {
		argsJSONBytes, marshalErr := json.Marshal(argsJSON)
		if marshalErr != nil {
			return contractID, functionName, nil, fmt.Errorf("failed to marshal arguments to JSON: %w", marshalErr)
		}
		s := string(argsJSONBytes)
		argsStr = &s
	}

	return contractID, functionName, argsStr, nil
}
