package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractOffers extracts offer snapshots from a ledger.
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeOffer + Map deduplication.
// LedgerCloseMeta is already unmarshaled; ledgerSeq, closedAt, ledgerRange are pre-extracted.
func extractOffers(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]OfferData, error) {
	var offers []OfferData

	// Use ingest package to read ledger changes
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for offers: %v", err)
		return offers, nil // Return empty slice, don't fail the whole batch
	}
	defer reader.Close()

	// Track unique offers (to avoid duplicates within a ledger)
	// Map-based deduplication: same offer can change multiple times per ledger
	offerMap := make(map[int64]*OfferData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for offers: %v", err)
			continue
		}

		// We only care about Offer changes
		if change.Type != xdr.LedgerEntryTypeOffer {
			continue
		}

		// Get the offer entry after the change (post state)
		var offerEntry *xdr.OfferEntry
		if change.Post != nil {
			if oe, ok := change.Post.Data.GetOffer(); ok {
				offerEntry = &oe
			}
		}

		if offerEntry == nil {
			continue
		}

		// Extract offer data
		offerData := extractOfferData(offerEntry, ledgerSeq, closedAt, ledgerRange)

		// Store in map (overwrites if offer appears multiple times)
		// Create a copy to avoid pointer reuse bug (loop variable is reused)
		offerCopy := offerData
		offerMap[offerData.OfferID] = &offerCopy
	}

	// Convert map to slice
	for _, offer := range offerMap {
		offers = append(offers, *offer)
	}

	return offers, nil
}

// extractOfferData extracts all fields from an OfferEntry
func extractOfferData(entry *xdr.OfferEntry, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) OfferData {
	// Identity
	offerID := int64(entry.OfferId)
	sellerAccount := entry.SellerId.Address()

	// Parse selling asset
	sellingType, sellingCode, sellingIssuer := parseAsset(entry.Selling)

	// Parse buying asset
	buyingType, buyingCode, buyingIssuer := parseAsset(entry.Buying)

	// Amount (in stroops)
	amount := strconv.FormatInt(int64(entry.Amount), 10)

	// Price as N/D ratio string (preserves precision)
	priceStr := fmt.Sprintf("%d/%d", entry.Price.N, entry.Price.D)

	// Flags
	flags := uint32(entry.Flags)

	// Metadata
	now := time.Now().UTC()

	return OfferData{
		// Identity
		OfferID:        offerID,
		SellerAccount:  sellerAccount,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,

		// Selling Asset
		SellingAssetType:   sellingType,
		SellingAssetCode:   sellingCode,
		SellingAssetIssuer: sellingIssuer,

		// Buying Asset
		BuyingAssetType:   buyingType,
		BuyingAssetCode:   buyingCode,
		BuyingAssetIssuer: buyingIssuer,

		// Offer Details
		Amount: amount,
		Price:  priceStr,
		Flags:  flags,

		// Metadata
		CreatedAt:   now,
		LedgerRange: ledgerRange,
	}
}

// parseAsset extracts asset type, code, and issuer from xdr.Asset.
// Returns (type, code, issuer) where code and issuer are nullable.
func parseAsset(asset xdr.Asset) (assetType string, code *string, issuer *string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", nil, nil

	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := asset.MustAlphaNum4()
		c := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
		i := a4.Issuer.Address()
		return "credit_alphanum4", &c, &i

	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := asset.MustAlphaNum12()
		c := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
		i := a12.Issuer.Address()
		return "credit_alphanum12", &c, &i

	case xdr.AssetTypeAssetTypePoolShare:
		// Liquidity pool shares (Protocol 18+) - rare in offers
		poolType := "liquidity_pool_shares"
		poolCode := "POOL_SHARE"
		poolIssuer := "pool"
		return poolType, &poolCode, &poolIssuer

	default:
		return "unknown", nil, nil
	}
}

// extractClaimableBalances extracts claimable balance state from a ledger.
// Protocol 14+ feature for deferred payments and multi-party asset distribution.
// LedgerCloseMeta is already unmarshaled; ledgerSeq, closedAt, ledgerRange are pre-extracted.
func extractClaimableBalances(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]ClaimableBalanceData, error) {
	var balances []ClaimableBalanceData

	// Use ingest package to read ledger changes
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for claimable balances: %v", err)
		return balances, nil // Return empty slice, don't fail the whole batch
	}
	defer reader.Close()

	// Track unique balances (to avoid duplicates within a ledger)
	balanceMap := make(map[string]*ClaimableBalanceData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for claimable balances: %v", err)
			continue
		}

		// We only care about ClaimableBalance changes
		if change.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		// Get the claimable balance entry after the change (post state)
		var balanceEntry *xdr.ClaimableBalanceEntry
		var ledgerEntry *xdr.LedgerEntry
		if change.Post != nil {
			ledgerEntry = change.Post
			if cb, ok := change.Post.Data.GetClaimableBalance(); ok {
				balanceEntry = &cb
			}
		}

		// Skip if no post-state (balance was removed)
		if balanceEntry == nil {
			continue
		}

		// Extract balance ID (32-byte hash as hex string)
		var balanceID string
		switch balanceEntry.BalanceId.Type {
		case xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0:
			hashBytes := balanceEntry.BalanceId.MustV0()
			balanceID = hex.EncodeToString(hashBytes[:])
		default:
			log.Printf("Unknown ClaimableBalanceId type: %v", balanceEntry.BalanceId.Type)
			continue
		}

		// Extract sponsor (account funding the reserve) from LedgerEntry extension
		var sponsor string
		sponsorDesc := ledgerEntry.SponsoringID()
		if sponsorDesc != nil {
			sponsor = sponsorDesc.Address()
		} else {
			// No sponsor - use empty string
			sponsor = ""
		}

		// Parse asset using parseAsset
		assetType, assetCode, assetIssuer := parseAsset(balanceEntry.Asset)

		// Extract amount (in stroops)
		amount := int64(balanceEntry.Amount)

		// Count claimants
		claimantsCount := int32(len(balanceEntry.Claimants))

		// Metadata
		now := time.Now().UTC()

		// Create balance data
		balance := ClaimableBalanceData{
			// Identity
			BalanceID:      balanceID,
			Sponsor:        sponsor,
			LedgerSequence: ledgerSeq,
			ClosedAt:       closedAt,

			// Asset & Amount
			AssetType:   assetType,
			AssetCode:   assetCode,
			AssetIssuer: assetIssuer,
			Amount:      amount,

			// Claimants
			ClaimantsCount: claimantsCount,

			// Flags (for future use, currently always 0)
			Flags: 0,

			// Metadata
			CreatedAt:   now,
			LedgerRange: ledgerRange,
		}

		// Deduplicate by balance_id
		balanceMap[balanceID] = &balance
	}

	// Convert map to slice
	for _, balance := range balanceMap {
		balances = append(balances, *balance)
	}

	return balances, nil
}

// extractLiquidityPools extracts liquidity pool state from a ledger.
// Protocol 18+ feature for AMM (Automated Market Maker) pools.
// LedgerCloseMeta is already unmarshaled; ledgerSeq, closedAt, ledgerRange are pre-extracted.
func extractLiquidityPools(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]LiquidityPoolData, error) {
	var pools []LiquidityPoolData

	// Use ingest package to read ledger changes
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for liquidity pools: %v", err)
		return pools, nil // Return empty slice, don't fail the whole batch
	}
	defer reader.Close()

	// Track unique pools (to avoid duplicates within a ledger)
	poolMap := make(map[string]*LiquidityPoolData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for liquidity pools: %v", err)
			continue
		}

		// We only care about LiquidityPool changes
		if change.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}

		// Get the liquidity pool entry after the change (post state)
		var poolEntry *xdr.LiquidityPoolEntry
		if change.Post != nil {
			if lp, ok := change.Post.Data.GetLiquidityPool(); ok {
				poolEntry = &lp
			}
		}

		// Skip if no post-state (pool was removed)
		if poolEntry == nil {
			continue
		}

		// Extract pool ID (32-byte hash as hex string)
		poolID := hex.EncodeToString(poolEntry.LiquidityPoolId[:])

		// Currently only LIQUIDITY_POOL_CONSTANT_PRODUCT is supported
		if poolEntry.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
			log.Printf("Unknown liquidity pool type: %v", poolEntry.Body.Type)
			continue
		}

		// Extract constant product pool data
		cp := poolEntry.Body.MustConstantProduct()

		// Extract assets using parseAsset
		assetAType, assetACode, assetAIssuer := parseAsset(cp.Params.AssetA)
		assetBType, assetBCode, assetBIssuer := parseAsset(cp.Params.AssetB)

		// Extract amounts and shares (in stroops)
		assetAAmount := int64(cp.ReserveA)
		assetBAmount := int64(cp.ReserveB)
		totalPoolShares := int64(cp.TotalPoolShares)
		trustlineCount := int32(cp.PoolSharesTrustLineCount)

		// Fee is in basis points (30 = 0.3%)
		fee := int32(cp.Params.Fee)

		// Metadata
		now := time.Now().UTC()

		// Create pool data
		pool := LiquidityPoolData{
			// Identity (3 fields)
			LiquidityPoolID: poolID,
			LedgerSequence:  ledgerSeq,
			ClosedAt:        closedAt,

			// Pool Type (1 field)
			PoolType: "constant_product",

			// Fee (1 field)
			Fee: fee,

			// Pool Shares (2 fields)
			TrustlineCount:  trustlineCount,
			TotalPoolShares: totalPoolShares,

			// Asset A (4 fields)
			AssetAType:   assetAType,
			AssetACode:   assetACode,
			AssetAIssuer: assetAIssuer,
			AssetAAmount: assetAAmount,

			// Asset B (4 fields)
			AssetBType:   assetBType,
			AssetBCode:   assetBCode,
			AssetBIssuer: assetBIssuer,
			AssetBAmount: assetBAmount,

			// Metadata (2 fields)
			CreatedAt:   now,
			LedgerRange: ledgerRange,
		}

		// Deduplicate by pool_id
		poolMap[poolID] = &pool
	}

	// Convert map to slice
	for _, pool := range poolMap {
		pools = append(pools, *pool)
	}

	return pools, nil
}
