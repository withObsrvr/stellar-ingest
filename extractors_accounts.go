package main

import (
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractAccounts extracts account snapshots from a ledger.
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeAccount + Map deduplication
func extractAccounts(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]AccountData, error) {
	var accounts []AccountData

	// Use ingest package to read ledger changes
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for accounts: %v", err)
		return accounts, nil // Return empty slice, don't fail the whole batch
	}
	defer reader.Close()

	// Track unique accounts (to avoid duplicates within a ledger)
	// Map-based deduplication: same account can change multiple times per ledger
	accountMap := make(map[string]*AccountData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for accounts: %v", err)
			continue
		}

		// We only care about AccountEntry changes
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		// Get the account entry after the change (post state)
		var accountEntry *xdr.AccountEntry
		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
			}
		}

		if accountEntry == nil {
			continue
		}

		// Extract account data
		accountData := extractAccountData(accountEntry, ledgerSeq, closedAt, ledgerRange)

		// Store in map (overwrites if account appears multiple times)
		// Create a copy to avoid pointer reuse bug (loop variable is reused)
		accountCopy := accountData
		accountMap[accountData.AccountID] = &accountCopy
	}

	// Convert map to slice
	for _, account := range accountMap {
		accounts = append(accounts, *account)
	}

	return accounts, nil
}

// extractAccountData extracts all fields from an AccountEntry.
func extractAccountData(entry *xdr.AccountEntry, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) AccountData {
	// Identity
	accountID := entry.AccountId.Address()

	// Balance (convert from stroops to XLM string)
	balance := amount.String(entry.Balance)

	// Account Settings
	sequenceNumber := uint64(entry.SeqNum)
	sequenceLedger := uint32(entry.SeqLedger())
	sequenceTime := uint64(entry.SeqTime())
	numSubentries := uint32(entry.NumSubEntries)
	numSponsoring := uint32(0)
	numSponsored := uint32(0)

	var homeDomain *string
	if entry.HomeDomain != "" {
		hd := string(entry.HomeDomain)
		homeDomain = &hd
	}

	// Thresholds (stored as [4]byte)
	masterWeight := uint32(entry.Thresholds[0])
	lowThreshold := uint32(entry.Thresholds[1])
	medThreshold := uint32(entry.Thresholds[2])
	highThreshold := uint32(entry.Thresholds[3])

	// Flags (bitmask)
	flags := uint32(entry.Flags)
	authRequired := (flags & uint32(xdr.AccountFlagsAuthRequiredFlag)) != 0
	authRevocable := (flags & uint32(xdr.AccountFlagsAuthRevocableFlag)) != 0
	authImmutable := (flags & uint32(xdr.AccountFlagsAuthImmutableFlag)) != 0
	authClawbackEnabled := (flags & uint32(xdr.AccountFlagsAuthClawbackEnabledFlag)) != 0

	// Signers (extract as JSON array)
	var signersJSON *string
	if len(entry.Signers) > 0 {
		type SignerData struct {
			Key    string `json:"key"`
			Weight uint32 `json:"weight"`
		}
		var signers []SignerData
		for _, signer := range entry.Signers {
			signers = append(signers, SignerData{
				Key:    signer.Key.Address(),
				Weight: uint32(signer.Weight),
			})
		}
		if jsonBytes, err := json.Marshal(signers); err == nil {
			jsonStr := string(jsonBytes)
			signersJSON = &jsonStr
		}
	}

	// Sponsorship (from extensions)
	var sponsorAccount *string

	// Extract liabilities and sponsorship counts (Protocol 10+)
	if ext, ok := entry.Ext.GetV1(); ok {
		// Extract sponsorship counts if available (Protocol 14+)
		if ext2, ok := ext.Ext.GetV2(); ok {
			numSponsoring = uint32(ext2.NumSponsoring)
			numSponsored = uint32(ext2.NumSponsored)
		}
	}

	// Metadata
	now := time.Now().UTC()

	return AccountData{
		// Identity
		AccountID:      accountID,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,

		// Balance
		Balance: balance,

		// Account Settings
		SequenceNumber: sequenceNumber,
		SequenceLedger: sequenceLedger,
		SequenceTime:   sequenceTime,
		NumSubentries:  numSubentries,
		NumSponsoring:  numSponsoring,
		NumSponsored:   numSponsored,
		HomeDomain:     homeDomain,

		// Thresholds
		MasterWeight:  masterWeight,
		LowThreshold:  lowThreshold,
		MedThreshold:  medThreshold,
		HighThreshold: highThreshold,

		// Flags
		Flags:               flags,
		AuthRequired:        authRequired,
		AuthRevocable:       authRevocable,
		AuthImmutable:       authImmutable,
		AuthClawbackEnabled: authClawbackEnabled,

		// Signers
		Signers: signersJSON,

		// Sponsorship
		SponsorAccount: sponsorAccount,

		// Metadata
		CreatedAt:   now,
		UpdatedAt:   now,
		LedgerRange: ledgerRange,
	}
}
