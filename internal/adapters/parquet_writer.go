package adapters

import (
	"encoding/json"
	"fmt"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// ParquetTableWriter manages writing rows to a single Parquet file per ledger_range partition.
type ParquetTableWriter[T any] struct {
	mu        sync.Mutex
	outputDir string
	tableName string
	workerID  int
	writers   map[uint32]*parquet.GenericWriter[T] // keyed by ledger_range
	files     map[uint32]*os.File
}

func NewParquetTableWriter[T any](outputDir, tableName string, workerID int) *ParquetTableWriter[T] {
	return &ParquetTableWriter[T]{
		outputDir: outputDir,
		tableName: tableName,
		workerID:  workerID,
		writers:   make(map[uint32]*parquet.GenericWriter[T]),
		files:     make(map[uint32]*os.File),
	}
}

func (w *ParquetTableWriter[T]) getWriter(ledgerRange uint32) (*parquet.GenericWriter[T], error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if writer, ok := w.writers[ledgerRange]; ok {
		return writer, nil
	}

	// Workers process ledgers in strictly ascending order (worker.go's
	// startPrefetcherRange iterates `for seq := start; seq <= end; seq++`),
	// so any writer for a ledger_range LOWER than the one we're opening is
	// guaranteed to receive no more writes. Close and drop those now — the
	// previous behavior was to keep them open until shard end, which let
	// parquet-go's internal row-group buffers accumulate ~10-100 MB per open
	// writer × 21 tables × N ranges. That's the linear memory-growth pattern
	// we saw on mainnet (OOM at ~40 GB on a 5-min run).
	for olderRange, oldWriter := range w.writers {
		if olderRange >= ledgerRange {
			continue
		}
		if err := oldWriter.Close(); err != nil {
			return nil, fmt.Errorf("close completed writer %s range %d: %w", w.tableName, olderRange, err)
		}
		if f, ok := w.files[olderRange]; ok {
			f.Close()
			delete(w.files, olderRange)
		}
		delete(w.writers, olderRange)
	}

	dir := filepath.Join(w.outputDir, "bronze", w.tableName, fmt.Sprintf("range_%07d", ledgerRange))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir %s: %w", dir, err)
	}

	// Use a unique filename per run to avoid truncating existing files on resume.
	// The checkpoint tracks which ledgers have been processed, so duplicate files
	// for the same range are acceptable (DuckLake deduplicates on INSERT).
	path := filepath.Join(dir, fmt.Sprintf("shard_%04d_%d.parquet", w.workerID, time.Now().UnixNano()))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file %s: %w", path, err)
	}

	writer := parquet.NewGenericWriter[T](f,
		parquet.Compression(&parquet.Snappy),
	)

	w.writers[ledgerRange] = writer
	w.files[ledgerRange] = f
	return writer, nil
}

func (w *ParquetTableWriter[T]) Write(rows []T, getLedgerRange func(T) uint32) error {
	if len(rows) == 0 {
		return nil
	}

	// Group rows by ledger_range
	grouped := make(map[uint32][]T)
	for _, row := range rows {
		lr := getLedgerRange(row)
		grouped[lr] = append(grouped[lr], row)
	}

	for lr, batch := range grouped {
		writer, err := w.getWriter(lr)
		if err != nil {
			return err
		}
		if _, err := writer.Write(batch); err != nil {
			return fmt.Errorf("write %s range %d: %w", w.tableName, lr, err)
		}
	}
	return nil
}

func (w *ParquetTableWriter[T]) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for lr, writer := range w.writers {
		if err := writer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close writer %s range %d: %w", w.tableName, lr, err)
		}
		if f, ok := w.files[lr]; ok {
			f.Close()
		}
	}
	return firstErr
}

// --- Parquet row types with struct tags ---
// These mirror the bronze data types but with parquet tags for schema definition.

type ParquetTransaction struct {
	LedgerSequence uint32 `parquet:"ledger_sequence"`
	// Stored as the 32 bytes it is, not as 64 hex characters: a digest is
	// random, so hex costs 2x and compresses by nothing. The
	// bronze.transactions_row_v2 view renders lower(hex(..)) for consumers.
	TransactionHash              []byte  `parquet:"transaction_hash"`
	SourceAccount                string  `parquet:"source_account"`
	SourceAccountMuxed           *string `parquet:"source_account_muxed,optional"`
	FeeCharged                   int64   `parquet:"fee_charged"`
	MaxFee                       int64   `parquet:"max_fee"`
	Successful                   bool    `parquet:"successful"`
	TransactionResultCode        string  `parquet:"transaction_result_code"`
	OperationCount               int32   `parquet:"operation_count"`
	MemoType                     *string `parquet:"memo_type,optional"`
	Memo                         *string `parquet:"memo,optional"`
	MemoRaw                      *string `parquet:"memo_raw,optional"`
	CreatedAt                    int64   `parquet:"closed_at,timestamp(microsecond)"`
	AccountSequence              int64   `parquet:"account_sequence"`
	LedgerRange                  uint32  `parquet:"ledger_range"`
	SignaturesCount              int32   `parquet:"signatures_count"`
	TxEnvelope                   *string `parquet:"tx_envelope,optional"`
	EnvelopeSizeBytes            int32   `parquet:"envelope_size_bytes"`
	TxResult                     *string `parquet:"tx_result,optional"`
	TxMeta                       *string `parquet:"tx_meta,optional"`
	TxFeeMeta                    *string `parquet:"tx_fee_meta,optional"`
	TxSigners                    *string `parquet:"tx_signers,optional"`
	NewAccount                   bool    `parquet:"new_account"`
	TimeboundsMinTime            *int64  `parquet:"timebounds_min_time,optional"`
	TimeboundsMaxTime            *int64  `parquet:"timebounds_max_time,optional"`
	SorobanHostFunctionType      *string `parquet:"soroban_host_function_type,optional"`
	SorobanContractID            *string `parquet:"soroban_contract_id,optional"`
	RentFeeCharged               *int64  `parquet:"rent_fee_charged,optional"`
	SorobanResourcesInstructions *int64  `parquet:"soroban_resources_instructions,optional"`
	SorobanResourcesReadBytes    *int64  `parquet:"soroban_resources_read_bytes,optional"`
	SorobanResourcesWriteBytes   *int64  `parquet:"soroban_resources_write_bytes,optional"`
	SorobanFootprintReadEntries  *int32  `parquet:"soroban_footprint_read_entries,optional"`
	SorobanFootprintWriteEntries *int32  `parquet:"soroban_footprint_write_entries,optional"`
	ContractErrorType            *string `parquet:"contract_error_type,optional"`
	ContractErrorCode            *int32  `parquet:"contract_error_code,optional"`
	FeeBumpFee                   *int64  `parquet:"fee_bump_fee,optional"`
	InnerTransactionHash         *string `parquet:"inner_transaction_hash,optional"`
	InnerSourceAccount           *string `parquet:"inner_source_account,optional"`
	FeeAccountMuxed              *string `parquet:"fee_account_muxed,optional"`
	LedgerboundsMin              *int64  `parquet:"ledgerbounds_min,optional"`
	LedgerboundsMax              *int64  `parquet:"ledgerbounds_max,optional"`
	MinSequenceNumber            *int64  `parquet:"min_sequence_number,optional"`
	MinSequenceAge               *int64  `parquet:"min_sequence_age,optional"`
	ExtraSigners                 *string `parquet:"extra_signers,optional"`
	SorobanDataSizeBytes         *int32  `parquet:"soroban_data_size_bytes,optional"`
	SorobanDataResources         *string `parquet:"soroban_data_resources,optional"`
	SorobanContractEventsCount   *int32  `parquet:"soroban_contract_events_count,optional"`
	TransactionID                int64   `parquet:"transaction_id"`
	EraID                        *string `parquet:"era_id,optional"`
	PipelineVersion              string  `parquet:"version_label"`
}

type ParquetOperation struct {
	// transaction_hash dropped under rule 3: transaction_id below names the
	// same transaction in 8 bytes rather than 31.4, and the
	// bronze.operations_row_v2 view recovers the hash by joining the parent.
	TransactionIndex            int32   `parquet:"transaction_index"`
	OperationIndex              int32   `parquet:"operation_index"`
	LedgerSequence              uint32  `parquet:"ledger_sequence"`
	SourceAccount               string  `parquet:"source_account"`
	SourceAccountMuxed          *string `parquet:"source_account_muxed,optional"`
	OpType                      int32   `parquet:"type"`
	TypeString                  string  `parquet:"type_string"`
	CreatedAt                   int64   `parquet:"closed_at,timestamp(microsecond)"`
	TransactionSuccessful       bool    `parquet:"transaction_successful"`
	OperationResultCode         *string `parquet:"operation_result_code,optional"`
	LedgerRange                 uint32  `parquet:"ledger_range"`
	Amount                      *int64  `parquet:"amount,optional"`
	Asset                       *string `parquet:"asset,optional"`
	AssetType                   *string `parquet:"asset_type,optional"`
	AssetCode                   *string `parquet:"asset_code,optional"`
	AssetIssuer                 *string `parquet:"asset_issuer,optional"`
	Destination                 *string `parquet:"destination,optional"`
	SourceAsset                 *string `parquet:"source_asset,optional"`
	SourceAssetType             *string `parquet:"source_asset_type,optional"`
	SourceAssetCode             *string `parquet:"source_asset_code,optional"`
	SourceAssetIssuer           *string `parquet:"source_asset_issuer,optional"`
	SourceAmount                *int64  `parquet:"source_amount,optional"`
	DestinationMin              *int64  `parquet:"destination_min,optional"`
	StartingBalance             *int64  `parquet:"starting_balance,optional"`
	TrustlineLimit              *int64  `parquet:"trustline_limit,optional"`
	OfferID                     *int64  `parquet:"offer_id,optional"`
	Price                       *string `parquet:"price,optional"`
	PriceR                      *string `parquet:"price_r,optional"`
	BuyingAsset                 *string `parquet:"buying_asset,optional"`
	BuyingAssetType             *string `parquet:"buying_asset_type,optional"`
	BuyingAssetCode             *string `parquet:"buying_asset_code,optional"`
	BuyingAssetIssuer           *string `parquet:"buying_asset_issuer,optional"`
	SellingAsset                *string `parquet:"selling_asset,optional"`
	SellingAssetType            *string `parquet:"selling_asset_type,optional"`
	SellingAssetCode            *string `parquet:"selling_asset_code,optional"`
	SellingAssetIssuer          *string `parquet:"selling_asset_issuer,optional"`
	SetFlags                    *int32  `parquet:"set_flags,optional"`
	ClearFlags                  *int32  `parquet:"clear_flags,optional"`
	HomeDomain                  *string `parquet:"home_domain,optional"`
	MasterWeight                *int32  `parquet:"master_weight,optional"`
	LowThreshold                *int32  `parquet:"low_threshold,optional"`
	MediumThreshold             *int32  `parquet:"medium_threshold,optional"`
	HighThreshold               *int32  `parquet:"high_threshold,optional"`
	DataName                    *string `parquet:"data_name,optional"`
	DataValue                   *string `parquet:"data_value,optional"`
	BalanceID                   *string `parquet:"balance_id,optional"`
	SponsoredID                 *string `parquet:"sponsored_id,optional"`
	BumpTo                      *int64  `parquet:"bump_to,optional"`
	SorobanAuthRequired         *bool   `parquet:"soroban_auth_required,optional"`
	SorobanOperation            *string `parquet:"soroban_operation,optional"`
	SorobanContractID           *string `parquet:"soroban_contract_id,optional"`
	SorobanFunction             *string `parquet:"soroban_function,optional"`
	SorobanArgumentsJSON        *string `parquet:"soroban_arguments_json,optional"`
	ContractCallsJSON           *string `parquet:"contract_calls_json,optional"`
	ContractsInvolved           *string `parquet:"contracts_involved,optional"`
	MaxCallDepth                *int32  `parquet:"max_call_depth,optional"`
	SorobanAuthCredentialsTypes *string `parquet:"soroban_auth_credentials_types,optional"`
	SorobanAuthAddresses        *string `parquet:"soroban_auth_addresses,optional"`
	TransactionID               int64   `parquet:"transaction_id"`
	OperationID                 int64   `parquet:"operation_id"`
	EraID                       *string `parquet:"era_id,optional"`
	PipelineVersion             string  `parquet:"version_label"`
}

type ParquetEffect struct {
	LedgerSequence   uint32  `parquet:"ledger_sequence"`
	TransactionHash  string  `parquet:"transaction_hash"`
	OperationIndex   int32   `parquet:"operation_index"`
	EffectIndex      int32   `parquet:"effect_index"`
	EffectType       int32   `parquet:"effect_type"`
	EffectTypeString string  `parquet:"effect_type_string"`
	AccountID        *string `parquet:"account_id,optional"`
	Amount           *string `parquet:"amount,optional"`
	AssetCode        *string `parquet:"asset_code,optional"`
	AssetIssuer      *string `parquet:"asset_issuer,optional"`
	AssetType        *string `parquet:"asset_type,optional"`
	TrustlineLimit   *string `parquet:"trustline_limit,optional"`
	AuthorizeFlag    *bool   `parquet:"authorize_flag,optional"`
	ClawbackFlag     *bool   `parquet:"clawback_flag,optional"`
	SignerAccount    *string `parquet:"signer_account,optional"`
	SignerWeight     *int32  `parquet:"signer_weight,optional"`
	OfferID          *int64  `parquet:"offer_id,optional"`
	SellerAccount    *string `parquet:"seller_account,optional"`
	OperationID      *int64  `parquet:"operation_id,optional"`
	DetailsJSON      *string `parquet:"details_json,optional"`
	CreatedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	LedgerRange      uint32  `parquet:"ledger_range"`
	EraID            *string `parquet:"era_id,optional"`
	PipelineVersion  string  `parquet:"version_label"`
}

type ParquetTrade struct {
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	TransactionHash    string  `parquet:"transaction_hash"`
	OperationIndex     int32   `parquet:"operation_index"`
	TradeIndex         int32   `parquet:"trade_index"`
	TradeType          string  `parquet:"trade_type"`
	TradeTimestamp     int64   `parquet:"trade_timestamp,timestamp(microsecond)"`
	SellerAccount      string  `parquet:"seller_account"`
	SellingAssetCode   *string `parquet:"selling_asset_code,optional"`
	SellingAssetIssuer *string `parquet:"selling_asset_issuer,optional"`
	SellingAmount      string  `parquet:"selling_amount"`
	BuyerAccount       string  `parquet:"buyer_account"`
	BuyingAssetCode    *string `parquet:"buying_asset_code,optional"`
	BuyingAssetIssuer  *string `parquet:"buying_asset_issuer,optional"`
	BuyingAmount       string  `parquet:"buying_amount"`
	Price              string  `parquet:"price"`
	LiquidityPoolID    *string `parquet:"liquidity_pool_id,optional"`
	CreatedAt          int64   `parquet:"closed_at,timestamp(microsecond)"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	EraID              *string `parquet:"era_id,optional"`
	PipelineVersion    string  `parquet:"version_label"`
}

type ParquetAccount struct {
	AccountID           string  `parquet:"account_id"`
	LedgerSequence      uint32  `parquet:"ledger_sequence"`
	ClosedAt            int64   `parquet:"closed_at,timestamp(microsecond)"`
	Balance             string  `parquet:"balance"`
	SequenceNumber      uint64  `parquet:"sequence_number"`
	SequenceLedger      uint32  `parquet:"sequence_ledger"`
	SequenceTime        uint64  `parquet:"sequence_time"`
	NumSubentries       uint32  `parquet:"num_subentries"`
	NumSponsoring       uint32  `parquet:"num_sponsoring"`
	NumSponsored        uint32  `parquet:"num_sponsored"`
	HomeDomain          *string `parquet:"home_domain,optional"`
	MasterWeight        uint32  `parquet:"master_weight"`
	LowThreshold        uint32  `parquet:"low_threshold"`
	MedThreshold        uint32  `parquet:"med_threshold"`
	HighThreshold       uint32  `parquet:"high_threshold"`
	Flags               uint32  `parquet:"flags"`
	AuthRequired        bool    `parquet:"auth_required"`
	AuthRevocable       bool    `parquet:"auth_revocable"`
	AuthImmutable       bool    `parquet:"auth_immutable"`
	AuthClawbackEnabled bool    `parquet:"auth_clawback_enabled"`
	Signers             *string `parquet:"signers,optional"`
	SponsorAccount      *string `parquet:"sponsor_account,optional"`
	CreatedAt           int64   `parquet:"ingested_at,timestamp(microsecond)"`
	UpdatedAt           int64   `parquet:"updated_at,timestamp(microsecond)"`
	LedgerRange         uint32  `parquet:"ledger_range"`
	EraID               *string `parquet:"era_id,optional"`
	PipelineVersion     string  `parquet:"version_label"`
}

type ParquetContractEvent struct {
	// event_id (rule 2) and transaction_hash (rule 3) both dropped: event_id
	// was transaction_hash || ':' || event_index, so this table stored one
	// transaction identity three times. The bronze.contract_events_stream_v1
	// view reproduces both from transaction_id and event_index. ~1.5 TB.
	ContractID               *string `parquet:"contract_id,optional"`
	LedgerSequence           uint32  `parquet:"ledger_sequence"`
	TransactionID            int64   `parquet:"transaction_id"`
	ClosedAt                 int64   `parquet:"closed_at,timestamp(microsecond)"`
	EventType                string  `parquet:"event_type"`
	InSuccessfulContractCall bool    `parquet:"in_successful_contract_call"`
	Successful               bool    `parquet:"successful"`
	ContractEventXDR         string  `parquet:"contract_event_xdr"`
	TopicsJSON               string  `parquet:"topics_json"`
	TopicsDecoded            string  `parquet:"topics_decoded"`
	DataXDR                  string  `parquet:"data_xdr"`
	DataDecoded              string  `parquet:"data_decoded"`
	TopicCount               int32   `parquet:"topic_count"`
	Topic0Decoded            *string `parquet:"topic0_decoded,optional"`
	Topic1Decoded            *string `parquet:"topic1_decoded,optional"`
	Topic2Decoded            *string `parquet:"topic2_decoded,optional"`
	Topic3Decoded            *string `parquet:"topic3_decoded,optional"`
	OperationIndex           uint32  `parquet:"operation_index"`
	EventStream              string  `parquet:"event_stream"`
	EventIndex               uint32  `parquet:"event_index"`
	CreatedAt                int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange              uint32  `parquet:"ledger_range"`
	EraID                    *string `parquet:"era_id,optional"`
	PipelineVersion          string  `parquet:"version_label"`
}

type ParquetNativeBalance struct {
	AccountID          string  `parquet:"account_id"`
	Balance            int64   `parquet:"balance"`
	BuyingLiabilities  int64   `parquet:"buying_liabilities"`
	SellingLiabilities int64   `parquet:"selling_liabilities"`
	NumSubentries      int32   `parquet:"num_subentries"`
	NumSponsoring      int32   `parquet:"num_sponsoring"`
	NumSponsored       int32   `parquet:"num_sponsored"`
	SequenceNumber     *int64  `parquet:"sequence_number,optional"`
	LastModifiedLedger int64   `parquet:"last_modified_ledger"`
	LedgerSequence     int64   `parquet:"ledger_sequence"`
	LedgerRange        int64   `parquet:"ledger_range"`
	EraID              *string `parquet:"era_id,optional"`
	PipelineVersion    string  `parquet:"version_label"`
}

type ParquetOffer struct {
	OfferID            int64   `parquet:"offer_id"`
	SellerAccount      string  `parquet:"seller_account"`
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	ClosedAt           int64   `parquet:"closed_at,timestamp(microsecond)"`
	SellingAssetType   string  `parquet:"selling_asset_type"`
	SellingAssetCode   *string `parquet:"selling_asset_code,optional"`
	SellingAssetIssuer *string `parquet:"selling_asset_issuer,optional"`
	BuyingAssetType    string  `parquet:"buying_asset_type"`
	BuyingAssetCode    *string `parquet:"buying_asset_code,optional"`
	BuyingAssetIssuer  *string `parquet:"buying_asset_issuer,optional"`
	Amount             string  `parquet:"amount"`
	Price              string  `parquet:"price"`
	Flags              uint32  `parquet:"flags"`
	CreatedAt          int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	EraID              *string `parquet:"era_id,optional"`
	PipelineVersion    string  `parquet:"version_label"`
}

type ParquetTrustline struct {
	AccountID                       string  `parquet:"account_id"`
	AssetCode                       string  `parquet:"asset_code"`
	AssetIssuer                     string  `parquet:"asset_issuer"`
	AssetType                       string  `parquet:"asset_type"`
	Balance                         string  `parquet:"balance"`
	TrustLimit                      string  `parquet:"trust_limit"`
	BuyingLiabilities               string  `parquet:"buying_liabilities"`
	SellingLiabilities              string  `parquet:"selling_liabilities"`
	Authorized                      bool    `parquet:"authorized"`
	AuthorizedToMaintainLiabilities bool    `parquet:"authorized_to_maintain_liabilities"`
	ClawbackEnabled                 bool    `parquet:"clawback_enabled"`
	LedgerSequence                  uint32  `parquet:"ledger_sequence"`
	CreatedAt                       int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange                     uint32  `parquet:"ledger_range"`
	EraID                           *string `parquet:"era_id,optional"`
	PipelineVersion                 string  `parquet:"version_label"`
}

type ParquetAccountSigner struct {
	AccountID       string  `parquet:"account_id"`
	Signer          string  `parquet:"signer"`
	LedgerSequence  uint32  `parquet:"ledger_sequence"`
	Weight          uint32  `parquet:"weight"`
	Sponsor         string  `parquet:"sponsor"`
	Deleted         bool    `parquet:"deleted"`
	ClosedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	CreatedAt       int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	EraID           *string `parquet:"era_id,optional"`
	PipelineVersion string  `parquet:"version_label"`
}

type ParquetClaimableBalance struct {
	BalanceID       string  `parquet:"balance_id"`
	Sponsor         string  `parquet:"sponsor"`
	LedgerSequence  uint32  `parquet:"ledger_sequence"`
	ClosedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	AssetType       string  `parquet:"asset_type"`
	AssetCode       *string `parquet:"asset_code,optional"`
	AssetIssuer     *string `parquet:"asset_issuer,optional"`
	Amount          int64   `parquet:"amount"`
	ClaimantsCount  int32   `parquet:"claimants_count"`
	Flags           uint32  `parquet:"flags"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	EraID           *string `parquet:"era_id,optional"`
	PipelineVersion string  `parquet:"version_label"`
}

type ParquetLiquidityPool struct {
	LiquidityPoolID string  `parquet:"liquidity_pool_id"`
	LedgerSequence  uint32  `parquet:"ledger_sequence"`
	ClosedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	PoolType        string  `parquet:"pool_type"`
	Fee             int32   `parquet:"fee"`
	TrustlineCount  int32   `parquet:"trustline_count"`
	TotalPoolShares int64   `parquet:"total_pool_shares"`
	AssetAType      string  `parquet:"asset_a_type"`
	AssetACode      *string `parquet:"asset_a_code,optional"`
	AssetAIssuer    *string `parquet:"asset_a_issuer,optional"`
	AssetAAmount    int64   `parquet:"asset_a_amount"`
	AssetBType      string  `parquet:"asset_b_type"`
	AssetBCode      *string `parquet:"asset_b_code,optional"`
	AssetBIssuer    *string `parquet:"asset_b_issuer,optional"`
	AssetBAmount    int64   `parquet:"asset_b_amount"`
	CreatedAt       int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	EraID           *string `parquet:"era_id,optional"`
	PipelineVersion string  `parquet:"version_label"`
}

type ParquetTTL struct {
	KeyHash            string  `parquet:"key_hash"`
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	LiveUntilLedgerSeq uint32  `parquet:"live_until_ledger_seq"`
	TTLRemaining       int64   `parquet:"ttl_remaining"`
	Expired            bool    `parquet:"expired"`
	LastModifiedLedger int32   `parquet:"last_modified_ledger"`
	Deleted            bool    `parquet:"deleted"`
	ClosedAt           int64   `parquet:"closed_at,timestamp(microsecond)"`
	CreatedAt          int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	EraID              *string `parquet:"era_id,optional"`
	PipelineVersion    string  `parquet:"version_label"`
}

type ParquetEvictedKey struct {
	KeyHash         string  `parquet:"key_hash"`
	LedgerSequence  uint32  `parquet:"ledger_sequence"`
	ContractID      string  `parquet:"contract_id"`
	KeyType         string  `parquet:"key_type"`
	Durability      string  `parquet:"durability"`
	ClosedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	CreatedAt       int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	EraID           *string `parquet:"era_id,optional"`
	PipelineVersion string  `parquet:"version_label"`
}

type ParquetContractData struct {
	ContractId         string  `parquet:"contract_id"`
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	LedgerKeyHash      string  `parquet:"ledger_key_hash"`
	ContractKeyType    string  `parquet:"contract_key_type"`
	ContractDurability string  `parquet:"contract_durability"`
	AssetCode          *string `parquet:"asset_code,optional"`
	AssetIssuer        *string `parquet:"asset_issuer,optional"`
	AssetType          *string `parquet:"asset_type,optional"`
	BalanceHolder      *string `parquet:"balance_holder,optional"`
	Balance            *string `parquet:"balance,optional"`
	LastModifiedLedger int32   `parquet:"last_modified_ledger"`
	LedgerEntryChange  int32   `parquet:"ledger_entry_change"`
	Deleted            bool    `parquet:"deleted"`
	ClosedAt           int64   `parquet:"closed_at,timestamp(microsecond)"`
	ContractDataXDR    string  `parquet:"contract_data_xdr"`
	TokenName          *string `parquet:"token_name,optional"`
	TokenSymbol        *string `parquet:"token_symbol,optional"`
	TokenDecimals      *int32  `parquet:"token_decimals,optional"`
	CreatedAt          int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	EraID              *string `parquet:"era_id,optional"`
	PipelineVersion    string  `parquet:"version_label"`
}

type ParquetContractCode struct {
	ContractCodeHash   string  `parquet:"contract_code_hash"`
	LedgerKeyHash      string  `parquet:"ledger_key_hash"`
	ContractCodeExtV   int32   `parquet:"contract_code_ext_v"`
	LastModifiedLedger int32   `parquet:"last_modified_ledger"`
	LedgerEntryChange  int32   `parquet:"ledger_entry_change"`
	Deleted            bool    `parquet:"deleted"`
	ClosedAt           int64   `parquet:"closed_at,timestamp(microsecond)"`
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	NInstructions      *int64  `parquet:"n_instructions,optional"`
	NFunctions         *int64  `parquet:"n_functions,optional"`
	NGlobals           *int64  `parquet:"n_globals,optional"`
	NTableEntries      *int64  `parquet:"n_table_entries,optional"`
	NTypes             *int64  `parquet:"n_types,optional"`
	NDataSegments      *int64  `parquet:"n_data_segments,optional"`
	NElemSegments      *int64  `parquet:"n_elem_segments,optional"`
	NImports           *int64  `parquet:"n_imports,optional"`
	NExports           *int64  `parquet:"n_exports,optional"`
	NDataSegmentBytes  *int64  `parquet:"n_data_segment_bytes,optional"`
	CreatedAt          int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	EraID              *string `parquet:"era_id,optional"`
	PipelineVersion    string  `parquet:"version_label"`
}

type ParquetRestoredKey struct {
	KeyHash            string  `parquet:"key_hash"`
	LedgerSequence     uint32  `parquet:"ledger_sequence"`
	ContractID         string  `parquet:"contract_id"`
	KeyType            string  `parquet:"key_type"`
	Durability         string  `parquet:"durability"`
	RestoredFromLedger uint32  `parquet:"restored_from_ledger"`
	ClosedAt           int64   `parquet:"closed_at,timestamp(microsecond)"`
	CreatedAt          int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange        uint32  `parquet:"ledger_range"`
	EraID              *string `parquet:"era_id,optional"`
	PipelineVersion    string  `parquet:"version_label"`
}

type ParquetContractCreation struct {
	ContractID     string  `parquet:"contract_id"`
	CreatorAddress string  `parquet:"creator_address"`
	WasmHash       *string `parquet:"wasm_hash,optional"`
	// ExecutableType records the deployment kind explicitly. A NULL
	// wasm_hash no longer implies a SAC: Protocol 28 added external_ref as
	// a third kind, and the inference broke silently when it arrived.
	ExecutableType   string  `parquet:"executable_type"`
	ExternalRefOwner *string `parquet:"external_ref_owner,optional"`
	ExternalRefTag   *string `parquet:"external_ref_tag,optional"`
	CreatedLedger    uint32  `parquet:"created_ledger"`
	CreatedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	LedgerRange      uint32  `parquet:"ledger_range"`
	EraID            *string `parquet:"era_id,optional"`
	PipelineVersion  string  `parquet:"version_label"`
}

type ParquetConfigSetting struct {
	ConfigSettingID                 int32   `parquet:"config_setting_id"`
	LedgerSequence                  uint32  `parquet:"ledger_sequence"`
	LastModifiedLedger              int32   `parquet:"last_modified_ledger"`
	Deleted                         bool    `parquet:"deleted"`
	ClosedAt                        int64   `parquet:"closed_at,timestamp(microsecond)"`
	LedgerMaxInstructions           *int64  `parquet:"ledger_max_instructions,optional"`
	TxMaxInstructions               *int64  `parquet:"tx_max_instructions,optional"`
	FeeRatePerInstructionsIncrement *int64  `parquet:"fee_rate_per_instructions_increment,optional"`
	TxMemoryLimit                   *int64  `parquet:"tx_memory_limit,optional"`
	LedgerMaxReadLedgerEntries      *int64  `parquet:"ledger_max_read_ledger_entries,optional"`
	LedgerMaxReadBytes              *int64  `parquet:"ledger_max_read_bytes,optional"`
	LedgerMaxWriteLedgerEntries     *int64  `parquet:"ledger_max_write_ledger_entries,optional"`
	LedgerMaxWriteBytes             *int64  `parquet:"ledger_max_write_bytes,optional"`
	TxMaxReadLedgerEntries          *int64  `parquet:"tx_max_read_ledger_entries,optional"`
	TxMaxReadBytes                  *int64  `parquet:"tx_max_read_bytes,optional"`
	TxMaxWriteLedgerEntries         *int64  `parquet:"tx_max_write_ledger_entries,optional"`
	TxMaxWriteBytes                 *int64  `parquet:"tx_max_write_bytes,optional"`
	TxMaxFootprintEntries           *int64  `parquet:"tx_max_footprint_entries,optional"`
	ContractMaxSizeBytes            *int64  `parquet:"contract_max_size_bytes,optional"`
	ConfigSettingXDR                string  `parquet:"config_setting_xdr"`
	CreatedAt                       int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange                     uint32  `parquet:"ledger_range"`
	EraID                           *string `parquet:"era_id,optional"`
	PipelineVersion                 string  `parquet:"version_label"`
	ObservationSource               string  `parquet:"observation_source"`
	ResolvedAtLedger                *int64  `parquet:"resolved_at_ledger,optional"`
}

// ParquetTokenTransfer carries transaction_id rather than transaction_hash.
//
// The hash was 775 GB of cold Bronze — 34.7 compressed bytes per row across
// 23.95 billion rows — because a 64-character hex string is incompressible:
// random hex has no redundancy for a block compressor to exploit, so it costs
// its full width forever. transaction_id is the same identity as an int64 at
// ~4 bytes per row, it is non-null on every one of those rows, and the hash
// is recoverable by joining transactions_row_v2 on it.
//
// Nothing read the hash from this table: not stellar-query-api, not
// serving-cold-backfill, not the silver transformer (which builds
// token_transfers_raw from contract_events_stream_v1, joined to transactions
// on *that* table's hash). It was written and never read.
type ParquetTokenTransfer struct {
	LedgerSequence  uint32  `parquet:"ledger_sequence"`
	TransactionID   int64   `parquet:"transaction_id"`
	OperationID     *int64  `parquet:"operation_id,optional"`
	OperationIndex  *int32  `parquet:"operation_index,optional"`
	EventIndex      uint32  `parquet:"event_index"`
	EventType       string  `parquet:"event_type"`
	From            *string `parquet:"from,optional"`
	To              *string `parquet:"to,optional"`
	Asset           string  `parquet:"asset"`
	AssetType       string  `parquet:"asset_type"`
	AssetCode       *string `parquet:"asset_code,optional"`
	AssetIssuer     *string `parquet:"asset_issuer,optional"`
	Amount          float64 `parquet:"amount"`
	AmountRaw       string  `parquet:"amount_raw"`
	ContractID      string  `parquet:"contract_id"`
	ClosedAt        int64   `parquet:"closed_at,timestamp(microsecond)"`
	CreatedAt       int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange     uint32  `parquet:"ledger_range"`
	EraID           *string `parquet:"era_id,optional"`
	PipelineVersion string  `parquet:"version_label"`
}

// --- Full ParquetWriter implementation ---

// ParquetWriterFull replaces the stub ParquetWriter with real Parquet output.
type ParquetLedger struct {
	Sequence             uint32  `parquet:"sequence"`
	LedgerHash           string  `parquet:"ledger_hash"`
	PreviousLedgerHash   string  `parquet:"previous_ledger_hash"`
	ClosedAt             int64   `parquet:"closed_at,timestamp(microsecond)"`
	ProtocolVersion      uint32  `parquet:"protocol_version"`
	TotalCoins           int64   `parquet:"total_coins"`
	FeePool              int64   `parquet:"fee_pool"`
	BaseFee              uint32  `parquet:"base_fee"`
	BaseReserve          uint32  `parquet:"base_reserve"`
	MaxTxSetSize         uint32  `parquet:"max_tx_set_size"`
	TransactionCount     int32   `parquet:"transaction_count"`
	OperationCount       int32   `parquet:"operation_count"`
	SuccessfulTxCount    int32   `parquet:"successful_tx_count"`
	FailedTxCount        int32   `parquet:"failed_tx_count"`
	TxSetOperationCount  int32   `parquet:"tx_set_operation_count"`
	SorobanFeeWrite1kb   *int64  `parquet:"soroban_fee_write1kb,optional"`
	NodeID               *string `parquet:"node_id,optional"`
	Signature            *string `parquet:"signature,optional"`
	LedgerHeader         *string `parquet:"ledger_header,optional"`
	BucketListSize       *int64  `parquet:"bucket_list_size,optional"`
	LiveSorobanStateSize *int64  `parquet:"live_soroban_state_size,optional"`
	EvictedKeysCount     *int32  `parquet:"evicted_keys_count,optional"`
	SorobanOpCount       *int32  `parquet:"soroban_op_count,optional"`
	TotalFeeCharged      *int64  `parquet:"total_fee_charged,optional"`
	ContractEventsCount  *int32  `parquet:"contract_events_count,optional"`
	IngestionTimestamp   int64   `parquet:"ingested_at,timestamp(microsecond)"`
	LedgerRange          uint32  `parquet:"ledger_range"`
	EraID                *string `parquet:"era_id,optional"`
	PipelineVersion      string  `parquet:"version_label"`
}

type ParquetWriterFull struct {
	mu                sync.Mutex
	closed            bool
	closeErr          error
	workerID          int
	pipelineVersion   string
	transactions      *ParquetTableWriter[ParquetTransaction]
	operations        *ParquetTableWriter[ParquetOperation]
	effects           *ParquetTableWriter[ParquetEffect]
	trades            *ParquetTableWriter[ParquetTrade]
	accounts          *ParquetTableWriter[ParquetAccount]
	offers            *ParquetTableWriter[ParquetOffer]
	trustlines        *ParquetTableWriter[ParquetTrustline]
	accountSigners    *ParquetTableWriter[ParquetAccountSigner]
	claimableBalances *ParquetTableWriter[ParquetClaimableBalance]
	liquidityPools    *ParquetTableWriter[ParquetLiquidityPool]
	configSettings    *ParquetTableWriter[ParquetConfigSetting]
	ttl               *ParquetTableWriter[ParquetTTL]
	evictedKeys       *ParquetTableWriter[ParquetEvictedKey]
	contractEvents    *ParquetTableWriter[ParquetContractEvent]
	contractData      *ParquetTableWriter[ParquetContractData]
	contractCode      *ParquetTableWriter[ParquetContractCode]
	nativeBalances    *ParquetTableWriter[ParquetNativeBalance]
	restoredKeys      *ParquetTableWriter[ParquetRestoredKey]
	contractCreations *ParquetTableWriter[ParquetContractCreation]
	ledgers           *ParquetTableWriter[ParquetLedger]
	tokenTransfers    *ParquetTableWriter[ParquetTokenTransfer]
}

func NewParquetWriterFull(outputDir string, workerID int, pipelineVersion string) *ParquetWriterFull {
	return &ParquetWriterFull{
		workerID:          workerID,
		pipelineVersion:   pipelineVersion,
		transactions:      NewParquetTableWriter[ParquetTransaction](outputDir, "transactions", workerID),
		operations:        NewParquetTableWriter[ParquetOperation](outputDir, "operations", workerID),
		effects:           NewParquetTableWriter[ParquetEffect](outputDir, "effects", workerID),
		trades:            NewParquetTableWriter[ParquetTrade](outputDir, "trades", workerID),
		accounts:          NewParquetTableWriter[ParquetAccount](outputDir, "accounts_snapshot", workerID),
		offers:            NewParquetTableWriter[ParquetOffer](outputDir, "offers_snapshot", workerID),
		trustlines:        NewParquetTableWriter[ParquetTrustline](outputDir, "trustlines_snapshot", workerID),
		accountSigners:    NewParquetTableWriter[ParquetAccountSigner](outputDir, "account_signers_snapshot", workerID),
		claimableBalances: NewParquetTableWriter[ParquetClaimableBalance](outputDir, "claimable_balances_snapshot", workerID),
		liquidityPools:    NewParquetTableWriter[ParquetLiquidityPool](outputDir, "liquidity_pools_snapshot", workerID),
		configSettings:    NewParquetTableWriter[ParquetConfigSetting](outputDir, "config_settings", workerID),
		ttl:               NewParquetTableWriter[ParquetTTL](outputDir, "ttl_snapshot", workerID),
		evictedKeys:       NewParquetTableWriter[ParquetEvictedKey](outputDir, "evicted_keys", workerID),
		contractEvents:    NewParquetTableWriter[ParquetContractEvent](outputDir, "contract_events", workerID),
		contractData:      NewParquetTableWriter[ParquetContractData](outputDir, "contract_data_snapshot", workerID),
		contractCode:      NewParquetTableWriter[ParquetContractCode](outputDir, "contract_code_snapshot", workerID),
		nativeBalances:    NewParquetTableWriter[ParquetNativeBalance](outputDir, "native_balances", workerID),
		restoredKeys:      NewParquetTableWriter[ParquetRestoredKey](outputDir, "restored_keys", workerID),
		contractCreations: NewParquetTableWriter[ParquetContractCreation](outputDir, "contract_creations", workerID),
		ledgers:           NewParquetTableWriter[ParquetLedger](outputDir, "ledgers", workerID),
		tokenTransfers:    NewParquetTableWriter[ParquetTokenTransfer](outputDir, "token_transfers", workerID),
	}
}

func (pw *ParquetWriterFull) WriteBatch(batch *domain.BatchData) error {
	pw.mu.Lock()
	closed := pw.closed
	pw.mu.Unlock()
	if closed {
		return fmt.Errorf("write batch after parquet writer was closed")
	}
	// Transactions
	if len(batch.Transactions) > 0 {
		rows := make([]ParquetTransaction, len(batch.Transactions))
		for i, t := range batch.Transactions {
			rows[i] = ParquetTransaction{
				LedgerSequence:               t.LedgerSequence,
				TransactionHash:              domain.DecodeHashBytes(t.TransactionHash),
				SourceAccount:                t.SourceAccount,
				SourceAccountMuxed:           t.SourceAccountMuxed,
				FeeCharged:                   t.FeeCharged,
				MaxFee:                       t.MaxFee,
				Successful:                   t.Successful,
				TransactionResultCode:        t.TransactionResultCode,
				OperationCount:               int32(t.OperationCount),
				MemoType:                     t.MemoType,
				Memo:                         t.Memo,
				MemoRaw:                      t.MemoRaw,
				CreatedAt:                    t.CreatedAt.UnixMicro(),
				AccountSequence:              t.AccountSequence,
				LedgerRange:                  t.LedgerRange,
				SignaturesCount:              int32(t.SignaturesCount),
				TxEnvelope:                   t.TxEnvelope,
				EnvelopeSizeBytes:            int32(t.EnvelopeSizeBytes),
				TxResult:                     t.TxResult,
				TxMeta:                       t.TxMeta,
				TxFeeMeta:                    t.TxFeeMeta,
				TxSigners:                    t.TxSigners,
				NewAccount:                   t.NewAccount,
				TimeboundsMinTime:            t.TimeboundsMinTime,
				TimeboundsMaxTime:            t.TimeboundsMaxTime,
				SorobanHostFunctionType:      t.SorobanHostFunctionType,
				SorobanContractID:            t.SorobanContractID,
				RentFeeCharged:               t.RentFeeCharged,
				SorobanResourcesInstructions: t.SorobanResourcesInstructions,
				SorobanResourcesReadBytes:    t.SorobanResourcesReadBytes,
				SorobanResourcesWriteBytes:   t.SorobanResourcesWriteBytes,
				SorobanFootprintReadEntries:  t.SorobanFootprintReadEntries,
				SorobanFootprintWriteEntries: t.SorobanFootprintWriteEntries,
				ContractErrorType:            t.ContractErrorType,
				ContractErrorCode:            t.ContractErrorCode,
				FeeBumpFee:                   t.FeeBumpFee,
				InnerTransactionHash:         t.InnerTransactionHash,
				InnerSourceAccount:           t.InnerSourceAccount,
				FeeAccountMuxed:              t.FeeAccountMuxed,
				LedgerboundsMin:              t.LedgerboundsMin,
				LedgerboundsMax:              t.LedgerboundsMax,
				MinSequenceNumber:            t.MinSequenceNumber,
				MinSequenceAge:               t.MinSequenceAge,
				ExtraSigners:                 t.ExtraSigners,
				SorobanDataSizeBytes:         t.SorobanDataSizeBytes,
				SorobanDataResources:         t.SorobanDataResources,
				SorobanContractEventsCount:   t.SorobanContractEventsCount,
				TransactionID:                t.TransactionID,
				EraID:                        t.EraID,
				PipelineVersion:              pw.pipelineVersion,
			}
		}
		if err := pw.transactions.Write(rows, func(r ParquetTransaction) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write transactions: %w", err)
		}
	}

	// Operations
	if len(batch.Operations) > 0 {
		rows := make([]ParquetOperation, len(batch.Operations))
		for i, o := range batch.Operations {
			var maxDepth *int32
			if o.MaxCallDepth != nil {
				d := int32(*o.MaxCallDepth)
				maxDepth = &d
			}
			// Convert *int to *int32 for parquet fields
			var setFlags, clearFlags, masterWeight, lowThreshold, mediumThreshold, highThreshold *int32
			if o.SetFlags != nil {
				v := int32(*o.SetFlags)
				setFlags = &v
			}
			if o.ClearFlags != nil {
				v := int32(*o.ClearFlags)
				clearFlags = &v
			}
			if o.MasterWeight != nil {
				v := int32(*o.MasterWeight)
				masterWeight = &v
			}
			if o.LowThreshold != nil {
				v := int32(*o.LowThreshold)
				lowThreshold = &v
			}
			if o.MediumThreshold != nil {
				v := int32(*o.MediumThreshold)
				mediumThreshold = &v
			}
			if o.HighThreshold != nil {
				v := int32(*o.HighThreshold)
				highThreshold = &v
			}
			rows[i] = ParquetOperation{
				TransactionIndex:            int32(o.TransactionIndex),
				OperationIndex:              int32(o.OperationIndex),
				LedgerSequence:              o.LedgerSequence,
				SourceAccount:               o.SourceAccount,
				SourceAccountMuxed:          o.SourceAccountMuxed,
				OpType:                      int32(o.OpType),
				TypeString:                  o.TypeString,
				CreatedAt:                   o.CreatedAt.UnixMicro(),
				TransactionSuccessful:       o.TransactionSuccessful,
				OperationResultCode:         o.OperationResultCode,
				LedgerRange:                 o.LedgerRange,
				Amount:                      o.Amount,
				Asset:                       o.Asset,
				AssetType:                   o.AssetType,
				AssetCode:                   o.AssetCode,
				AssetIssuer:                 o.AssetIssuer,
				Destination:                 o.Destination,
				SourceAsset:                 o.SourceAsset,
				SourceAssetType:             o.SourceAssetType,
				SourceAssetCode:             o.SourceAssetCode,
				SourceAssetIssuer:           o.SourceAssetIssuer,
				SourceAmount:                o.SourceAmount,
				DestinationMin:              o.DestinationMin,
				StartingBalance:             o.StartingBalance,
				TrustlineLimit:              o.TrustlineLimit,
				OfferID:                     o.OfferID,
				Price:                       o.Price,
				PriceR:                      o.PriceR,
				BuyingAsset:                 o.BuyingAsset,
				BuyingAssetType:             o.BuyingAssetType,
				BuyingAssetCode:             o.BuyingAssetCode,
				BuyingAssetIssuer:           o.BuyingAssetIssuer,
				SellingAsset:                o.SellingAsset,
				SellingAssetType:            o.SellingAssetType,
				SellingAssetCode:            o.SellingAssetCode,
				SellingAssetIssuer:          o.SellingAssetIssuer,
				SetFlags:                    setFlags,
				ClearFlags:                  clearFlags,
				HomeDomain:                  o.HomeDomain,
				MasterWeight:                masterWeight,
				LowThreshold:                lowThreshold,
				MediumThreshold:             mediumThreshold,
				HighThreshold:               highThreshold,
				DataName:                    o.DataName,
				DataValue:                   o.DataValue,
				BalanceID:                   o.BalanceID,
				SponsoredID:                 o.SponsoredID,
				BumpTo:                      o.BumpTo,
				SorobanAuthRequired:         o.SorobanAuthRequired,
				SorobanOperation:            o.SorobanOperation,
				SorobanContractID:           o.SorobanContractID,
				SorobanFunction:             o.SorobanFunction,
				SorobanArgumentsJSON:        o.SorobanArgumentsJSON,
				ContractCallsJSON:           o.ContractCallsJSON,
				ContractsInvolved:           stringSliceJSONPtr(o.ContractsInvolved),
				MaxCallDepth:                maxDepth,
				SorobanAuthCredentialsTypes: stringSliceJSONPtr(o.SorobanAuthCredentialsTypes),
				SorobanAuthAddresses:        stringSliceJSONPtr(o.SorobanAuthAddresses),
				TransactionID:               o.TransactionID,
				OperationID:                 o.OperationID,
				EraID:                       o.EraID,
				PipelineVersion:             pw.pipelineVersion,
			}
		}
		if err := pw.operations.Write(rows, func(r ParquetOperation) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write operations: %w", err)
		}
	}

	// Effects
	if len(batch.Effects) > 0 {
		rows := make([]ParquetEffect, len(batch.Effects))
		for i, e := range batch.Effects {
			var signerWeight *int32
			if e.SignerWeight != nil {
				w := int32(*e.SignerWeight)
				signerWeight = &w
			}
			rows[i] = ParquetEffect{
				LedgerSequence:   e.LedgerSequence,
				TransactionHash:  e.TransactionHash,
				OperationIndex:   int32(e.OperationIndex),
				EffectIndex:      int32(e.EffectIndex),
				EffectType:       int32(e.EffectType),
				EffectTypeString: e.EffectTypeString,
				AccountID:        e.AccountID,
				Amount:           e.Amount,
				AssetCode:        e.AssetCode,
				AssetIssuer:      e.AssetIssuer,
				AssetType:        e.AssetType,
				TrustlineLimit:   e.TrustlineLimit,
				AuthorizeFlag:    e.AuthorizeFlag,
				ClawbackFlag:     e.ClawbackFlag,
				SignerAccount:    e.SignerAccount,
				SignerWeight:     signerWeight,
				OfferID:          e.OfferID,
				SellerAccount:    e.SellerAccount,
				OperationID:      e.OperationID,
				DetailsJSON:      e.DetailsJSON,
				CreatedAt:        e.CreatedAt.UnixMicro(),
				LedgerRange:      e.LedgerRange,
				EraID:            e.EraID,
				PipelineVersion:  pw.pipelineVersion,
			}
		}
		if err := pw.effects.Write(rows, func(r ParquetEffect) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write effects: %w", err)
		}
	}

	// Trades
	if len(batch.Trades) > 0 {
		rows := make([]ParquetTrade, len(batch.Trades))
		for i, t := range batch.Trades {
			rows[i] = ParquetTrade{
				LedgerSequence:     t.LedgerSequence,
				TransactionHash:    t.TransactionHash,
				OperationIndex:     int32(t.OperationIndex),
				TradeIndex:         int32(t.TradeIndex),
				TradeType:          t.TradeType,
				TradeTimestamp:     t.TradeTimestamp.UnixMicro(),
				SellerAccount:      t.SellerAccount,
				SellingAssetCode:   t.SellingAssetCode,
				SellingAssetIssuer: t.SellingAssetIssuer,
				SellingAmount:      t.SellingAmount,
				BuyerAccount:       t.BuyerAccount,
				BuyingAssetCode:    t.BuyingAssetCode,
				BuyingAssetIssuer:  t.BuyingAssetIssuer,
				BuyingAmount:       t.BuyingAmount,
				Price:              t.Price,
				LiquidityPoolID:    t.LiquidityPoolID,
				CreatedAt:          t.CreatedAt.UnixMicro(),
				LedgerRange:        t.LedgerRange,
				EraID:              t.EraID,
				PipelineVersion:    pw.pipelineVersion,
			}
		}
		if err := pw.trades.Write(rows, func(r ParquetTrade) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write trades: %w", err)
		}
	}

	// Accounts
	if len(batch.Accounts) > 0 {
		rows := make([]ParquetAccount, len(batch.Accounts))
		for i, a := range batch.Accounts {
			rows[i] = ParquetAccount{
				AccountID:           a.AccountID,
				LedgerSequence:      a.LedgerSequence,
				ClosedAt:            a.ClosedAt.UnixMicro(),
				Balance:             a.Balance,
				SequenceNumber:      a.SequenceNumber,
				SequenceLedger:      a.SequenceLedger,
				SequenceTime:        a.SequenceTime,
				NumSubentries:       a.NumSubentries,
				NumSponsoring:       a.NumSponsoring,
				NumSponsored:        a.NumSponsored,
				HomeDomain:          a.HomeDomain,
				MasterWeight:        a.MasterWeight,
				LowThreshold:        a.LowThreshold,
				MedThreshold:        a.MedThreshold,
				HighThreshold:       a.HighThreshold,
				Flags:               a.Flags,
				AuthRequired:        a.AuthRequired,
				AuthRevocable:       a.AuthRevocable,
				AuthImmutable:       a.AuthImmutable,
				AuthClawbackEnabled: a.AuthClawbackEnabled,
				Signers:             a.Signers,
				SponsorAccount:      a.SponsorAccount,
				CreatedAt:           a.CreatedAt.UnixMicro(),
				UpdatedAt:           a.UpdatedAt.UnixMicro(),
				LedgerRange:         a.LedgerRange,
				EraID:               a.EraID,
				PipelineVersion:     pw.pipelineVersion,
			}
		}
		if err := pw.accounts.Write(rows, func(r ParquetAccount) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write accounts: %w", err)
		}
	}

	// Contract Events
	if len(batch.ContractEvents) > 0 {
		rows := make([]ParquetContractEvent, len(batch.ContractEvents))
		for i, e := range batch.ContractEvents {
			rows[i] = ParquetContractEventFromData(e, pw.pipelineVersion)
		}
		if err := pw.contractEvents.Write(rows, func(r ParquetContractEvent) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_events: %w", err)
		}
	}

	// Offers
	if len(batch.Offers) > 0 {
		rows := make([]ParquetOffer, len(batch.Offers))
		for i, o := range batch.Offers {
			rows[i] = ParquetOffer{
				OfferID: o.OfferID, SellerAccount: o.SellerAccount, LedgerSequence: o.LedgerSequence,
				ClosedAt: o.ClosedAt.UnixMicro(), SellingAssetType: o.SellingAssetType, SellingAssetCode: o.SellingAssetCode,
				SellingAssetIssuer: o.SellingAssetIssuer, BuyingAssetType: o.BuyingAssetType, BuyingAssetCode: o.BuyingAssetCode,
				BuyingAssetIssuer: o.BuyingAssetIssuer, Amount: o.Amount, Price: o.Price, Flags: o.Flags,
				CreatedAt: o.CreatedAt.UnixMicro(), LedgerRange: o.LedgerRange,
				EraID: o.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.offers.Write(rows, func(r ParquetOffer) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write offers: %w", err)
		}
	}

	// Trustlines
	if len(batch.Trustlines) > 0 {
		rows := make([]ParquetTrustline, len(batch.Trustlines))
		for i, t := range batch.Trustlines {
			rows[i] = ParquetTrustline{
				AccountID: t.AccountID, AssetCode: t.AssetCode, AssetIssuer: t.AssetIssuer, AssetType: t.AssetType,
				Balance: t.Balance, TrustLimit: t.TrustLimit, BuyingLiabilities: t.BuyingLiabilities,
				SellingLiabilities: t.SellingLiabilities, Authorized: t.Authorized,
				AuthorizedToMaintainLiabilities: t.AuthorizedToMaintainLiabilities, ClawbackEnabled: t.ClawbackEnabled,
				LedgerSequence: t.LedgerSequence, CreatedAt: t.CreatedAt.UnixMicro(), LedgerRange: t.LedgerRange,
				EraID: t.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.trustlines.Write(rows, func(r ParquetTrustline) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write trustlines: %w", err)
		}
	}

	// Account Signers
	if len(batch.AccountSigners) > 0 {
		rows := make([]ParquetAccountSigner, len(batch.AccountSigners))
		for i, s := range batch.AccountSigners {
			rows[i] = ParquetAccountSigner{
				AccountID: s.AccountID, Signer: s.Signer, LedgerSequence: s.LedgerSequence,
				Weight: s.Weight, Sponsor: s.Sponsor, Deleted: s.Deleted,
				ClosedAt: s.ClosedAt.UnixMicro(), CreatedAt: s.CreatedAt.UnixMicro(), LedgerRange: s.LedgerRange,
				EraID: s.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.accountSigners.Write(rows, func(r ParquetAccountSigner) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write account_signers: %w", err)
		}
	}

	// Claimable Balances
	if len(batch.ClaimableBalances) > 0 {
		rows := make([]ParquetClaimableBalance, len(batch.ClaimableBalances))
		for i, b := range batch.ClaimableBalances {
			rows[i] = ParquetClaimableBalance{
				BalanceID: b.BalanceID, Sponsor: b.Sponsor, LedgerSequence: b.LedgerSequence,
				ClosedAt: b.ClosedAt.UnixMicro(), AssetType: b.AssetType, AssetCode: b.AssetCode,
				AssetIssuer: b.AssetIssuer, Amount: b.Amount, ClaimantsCount: b.ClaimantsCount,
				Flags: b.Flags, LedgerRange: b.LedgerRange,
				EraID: b.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.claimableBalances.Write(rows, func(r ParquetClaimableBalance) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write claimable_balances: %w", err)
		}
	}

	// Liquidity Pools
	if len(batch.LiquidityPools) > 0 {
		rows := make([]ParquetLiquidityPool, len(batch.LiquidityPools))
		for i, p := range batch.LiquidityPools {
			rows[i] = ParquetLiquidityPool{
				LiquidityPoolID: p.LiquidityPoolID, LedgerSequence: p.LedgerSequence, ClosedAt: p.ClosedAt.UnixMicro(),
				PoolType: p.PoolType, Fee: p.Fee, TrustlineCount: p.TrustlineCount, TotalPoolShares: p.TotalPoolShares,
				AssetAType: p.AssetAType, AssetACode: p.AssetACode, AssetAIssuer: p.AssetAIssuer, AssetAAmount: p.AssetAAmount,
				AssetBType: p.AssetBType, AssetBCode: p.AssetBCode, AssetBIssuer: p.AssetBIssuer, AssetBAmount: p.AssetBAmount,
				CreatedAt: p.CreatedAt.UnixMicro(), LedgerRange: p.LedgerRange,
				EraID: p.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.liquidityPools.Write(rows, func(r ParquetLiquidityPool) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write liquidity_pools: %w", err)
		}
	}

	// Config Settings
	if len(batch.ConfigSettings) > 0 {
		rows := make([]ParquetConfigSetting, len(batch.ConfigSettings))
		for i, c := range batch.ConfigSettings {
			rows[i] = ParquetConfigSetting{
				ConfigSettingID:                 c.ConfigSettingID,
				LedgerSequence:                  c.LedgerSequence,
				LastModifiedLedger:              c.LastModifiedLedger,
				Deleted:                         c.Deleted,
				ClosedAt:                        c.ClosedAt.UnixMicro(),
				LedgerMaxInstructions:           toInt64Ptr(c.LedgerMaxInstructions),
				TxMaxInstructions:               toInt64Ptr(c.TxMaxInstructions),
				FeeRatePerInstructionsIncrement: toInt64Ptr(c.FeeRatePerInstructionsIncrement),
				TxMemoryLimit:                   uint32ToInt64Ptr(c.TxMemoryLimit),
				LedgerMaxReadLedgerEntries:      uint32ToInt64Ptr(c.LedgerMaxReadLedgerEntries),
				LedgerMaxReadBytes:              uint32ToInt64Ptr(c.LedgerMaxReadBytes),
				LedgerMaxWriteLedgerEntries:     uint32ToInt64Ptr(c.LedgerMaxWriteLedgerEntries),
				LedgerMaxWriteBytes:             uint32ToInt64Ptr(c.LedgerMaxWriteBytes),
				TxMaxReadLedgerEntries:          uint32ToInt64Ptr(c.TxMaxReadLedgerEntries),
				TxMaxReadBytes:                  uint32ToInt64Ptr(c.TxMaxReadBytes),
				TxMaxWriteLedgerEntries:         uint32ToInt64Ptr(c.TxMaxWriteLedgerEntries),
				TxMaxWriteBytes:                 uint32ToInt64Ptr(c.TxMaxWriteBytes),
				TxMaxFootprintEntries:           uint32ToInt64Ptr(c.TxMaxFootprintEntries),
				ContractMaxSizeBytes:            uint32ToInt64Ptr(c.ContractMaxSizeBytes),
				ConfigSettingXDR:                c.ConfigSettingXDR,
				CreatedAt:                       c.CreatedAt.UnixMicro(),
				LedgerRange:                     c.LedgerRange,
				EraID:                           c.EraID,
				PipelineVersion:                 pw.pipelineVersion,
				ObservationSource:               c.ObservationSource,
				ResolvedAtLedger:                c.ResolvedAtLedger,
			}
		}
		if err := pw.configSettings.Write(rows, func(r ParquetConfigSetting) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write config_settings: %w", err)
		}
	}

	// TTL
	if len(batch.TTLEntries) > 0 {
		rows := make([]ParquetTTL, len(batch.TTLEntries))
		for i, t := range batch.TTLEntries {
			rows[i] = ParquetTTL{
				KeyHash: t.KeyHash, LedgerSequence: t.LedgerSequence, LiveUntilLedgerSeq: t.LiveUntilLedgerSeq,
				TTLRemaining: t.TTLRemaining, Expired: t.Expired, LastModifiedLedger: t.LastModifiedLedger,
				Deleted: t.Deleted, ClosedAt: t.ClosedAt.UnixMicro(), CreatedAt: t.CreatedAt.UnixMicro(), LedgerRange: t.LedgerRange,
				EraID: t.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.ttl.Write(rows, func(r ParquetTTL) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write ttl: %w", err)
		}
	}

	// Evicted Keys
	if len(batch.EvictedKeys) > 0 {
		rows := make([]ParquetEvictedKey, len(batch.EvictedKeys))
		for i, e := range batch.EvictedKeys {
			rows[i] = ParquetEvictedKey{
				KeyHash: e.KeyHash, LedgerSequence: e.LedgerSequence, ContractID: e.ContractID,
				KeyType: e.KeyType, Durability: e.Durability, ClosedAt: e.ClosedAt.UnixMicro(), CreatedAt: e.CreatedAt.UnixMicro(), LedgerRange: e.LedgerRange,
				EraID: e.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.evictedKeys.Write(rows, func(r ParquetEvictedKey) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write evicted_keys: %w", err)
		}
	}

	// Contract Data
	if len(batch.ContractData) > 0 {
		rows := make([]ParquetContractData, len(batch.ContractData))
		for i, d := range batch.ContractData {
			rows[i] = ParquetContractData{
				ContractId: d.ContractId, LedgerSequence: d.LedgerSequence, LedgerKeyHash: d.LedgerKeyHash,
				ContractKeyType: d.ContractKeyType, ContractDurability: d.ContractDurability,
				AssetCode: d.AssetCode, AssetIssuer: d.AssetIssuer, AssetType: d.AssetType,
				BalanceHolder: d.BalanceHolder, Balance: d.Balance,
				LastModifiedLedger: d.LastModifiedLedger, LedgerEntryChange: d.LedgerEntryChange,
				Deleted: d.Deleted, ClosedAt: d.ClosedAt.UnixMicro(), ContractDataXDR: d.ContractDataXDR,
				TokenName: d.TokenName, TokenSymbol: d.TokenSymbol, TokenDecimals: d.TokenDecimals,
				CreatedAt: d.CreatedAt.UnixMicro(), LedgerRange: d.LedgerRange,
				EraID: d.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.contractData.Write(rows, func(r ParquetContractData) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_data: %w", err)
		}
	}

	// Contract Code
	if len(batch.ContractCode) > 0 {
		rows := make([]ParquetContractCode, len(batch.ContractCode))
		for i, c := range batch.ContractCode {
			rows[i] = ParquetContractCode{
				ContractCodeHash: c.ContractCodeHash, LedgerKeyHash: c.LedgerKeyHash, ContractCodeExtV: c.ContractCodeExtV,
				LastModifiedLedger: c.LastModifiedLedger, LedgerEntryChange: c.LedgerEntryChange,
				Deleted: c.Deleted, ClosedAt: c.ClosedAt.UnixMicro(), LedgerSequence: c.LedgerSequence,
				NInstructions: c.NInstructions, NFunctions: c.NFunctions, NGlobals: c.NGlobals,
				NTableEntries: c.NTableEntries, NTypes: c.NTypes, NDataSegments: c.NDataSegments,
				NElemSegments: c.NElemSegments, NImports: c.NImports, NExports: c.NExports,
				NDataSegmentBytes: c.NDataSegmentBytes, CreatedAt: c.CreatedAt.UnixMicro(), LedgerRange: c.LedgerRange,
				EraID: c.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.contractCode.Write(rows, func(r ParquetContractCode) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_code: %w", err)
		}
	}

	// Restored Keys
	if len(batch.RestoredKeys) > 0 {
		rows := make([]ParquetRestoredKey, len(batch.RestoredKeys))
		for i, r := range batch.RestoredKeys {
			rows[i] = ParquetRestoredKey{
				KeyHash: r.KeyHash, LedgerSequence: r.LedgerSequence, ContractID: r.ContractID,
				KeyType: r.KeyType, Durability: r.Durability, RestoredFromLedger: r.RestoredFromLedger,
				ClosedAt: r.ClosedAt.UnixMicro(), CreatedAt: r.CreatedAt.UnixMicro(), LedgerRange: r.LedgerRange,
				EraID: r.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.restoredKeys.Write(rows, func(r ParquetRestoredKey) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write restored_keys: %w", err)
		}
	}

	// Contract Creations
	if len(batch.ContractCreations) > 0 {
		rows := make([]ParquetContractCreation, len(batch.ContractCreations))
		for i, c := range batch.ContractCreations {
			rows[i] = ParquetContractCreation{
				ContractID: c.ContractID, CreatorAddress: c.CreatorAddress, WasmHash: c.WasmHash,
				ExecutableType: c.ExecutableType, ExternalRefOwner: c.ExternalRefOwner,
				ExternalRefTag: c.ExternalRefTag,
				CreatedLedger:  c.CreatedLedger, CreatedAt: c.CreatedAt.UnixMicro(), LedgerRange: c.LedgerRange,
				EraID: c.EraID, PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.contractCreations.Write(rows, func(r ParquetContractCreation) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write contract_creations: %w", err)
		}
	}

	// Native Balances
	if len(batch.NativeBalances) > 0 {
		rows := make([]ParquetNativeBalance, len(batch.NativeBalances))
		for i, n := range batch.NativeBalances {
			rows[i] = ParquetNativeBalance{
				AccountID:          n.AccountID,
				Balance:            n.Balance,
				BuyingLiabilities:  n.BuyingLiabilities,
				SellingLiabilities: n.SellingLiabilities,
				NumSubentries:      n.NumSubentries,
				NumSponsoring:      n.NumSponsoring,
				NumSponsored:       n.NumSponsored,
				SequenceNumber:     n.SequenceNumber,
				LastModifiedLedger: n.LastModifiedLedger,
				LedgerSequence:     n.LedgerSequence,
				LedgerRange:        n.LedgerRange,
				EraID:              n.EraID,
				PipelineVersion:    pw.pipelineVersion,
			}
		}
		if err := pw.nativeBalances.Write(rows, func(r ParquetNativeBalance) uint32 { return uint32(r.LedgerRange) }); err != nil {
			return fmt.Errorf("write native_balances: %w", err)
		}
	}

	// Ledgers
	if len(batch.Ledgers) > 0 {
		rows := make([]ParquetLedger, len(batch.Ledgers))
		for i, l := range batch.Ledgers {
			rows[i] = ParquetLedger{
				Sequence:             l.Sequence,
				LedgerHash:           l.LedgerHash,
				PreviousLedgerHash:   l.PreviousLedgerHash,
				ClosedAt:             l.ClosedAt.UnixMicro(),
				ProtocolVersion:      l.ProtocolVersion,
				TotalCoins:           l.TotalCoins,
				FeePool:              l.FeePool,
				BaseFee:              l.BaseFee,
				BaseReserve:          l.BaseReserve,
				MaxTxSetSize:         l.MaxTxSetSize,
				TransactionCount:     int32(l.TransactionCount),
				OperationCount:       int32(l.OperationCount),
				SuccessfulTxCount:    int32(l.SuccessfulTxCount),
				FailedTxCount:        int32(l.FailedTxCount),
				TxSetOperationCount:  int32(l.TxSetOperationCount),
				SorobanFeeWrite1kb:   l.SorobanFeeWrite1kb,
				NodeID:               l.NodeID,
				Signature:            l.Signature,
				LedgerHeader:         l.LedgerHeader,
				BucketListSize:       l.BucketListSize,
				LiveSorobanStateSize: l.LiveSorobanStateSize,
				EvictedKeysCount:     l.EvictedKeysCount,
				SorobanOpCount:       l.SorobanOpCount,
				TotalFeeCharged:      l.TotalFeeCharged,
				ContractEventsCount:  l.ContractEventsCount,
				IngestionTimestamp:   l.IngestionTimestamp.UnixMicro(),
				LedgerRange:          l.LedgerRange,
				EraID:                l.EraID,
				PipelineVersion:      pw.pipelineVersion,
			}
		}
		if err := pw.ledgers.Write(rows, func(r ParquetLedger) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write ledgers: %w", err)
		}
	}

	// Token Transfers
	if len(batch.TokenTransfers) > 0 {
		rows := make([]ParquetTokenTransfer, len(batch.TokenTransfers))
		for i, t := range batch.TokenTransfers {
			rows[i] = ParquetTokenTransfer{
				LedgerSequence:  t.LedgerSequence,
				TransactionID:   t.TransactionID,
				OperationID:     t.OperationID,
				OperationIndex:  t.OperationIndex,
				EventIndex:      t.EventIndex,
				EventType:       t.EventType,
				From:            t.From,
				To:              t.To,
				Asset:           t.Asset,
				AssetType:       t.AssetType,
				AssetCode:       t.AssetCode,
				AssetIssuer:     t.AssetIssuer,
				Amount:          t.Amount,
				AmountRaw:       t.AmountRaw,
				ContractID:      t.ContractID,
				ClosedAt:        t.ClosedAt.UnixMicro(),
				CreatedAt:       t.CreatedAt.UnixMicro(),
				LedgerRange:     t.LedgerRange,
				EraID:           t.EraID,
				PipelineVersion: pw.pipelineVersion,
			}
		}
		if err := pw.tokenTransfers.Write(rows, func(r ParquetTokenTransfer) uint32 { return r.LedgerRange }); err != nil {
			return fmt.Errorf("write token_transfers: %w", err)
		}
	}

	return nil
}

// toInt64Ptr converts *int64 to *int64 (passthrough, for consistency with uint32ToInt64Ptr).
func toInt64Ptr(v *int64) *int64 {
	return v
}

// uint32ToInt64Ptr widens a *uint32 to *int64 for Parquet BIGINT columns.
func uint32ToInt64Ptr(v *uint32) *int64 {
	if v == nil {
		return nil
	}
	val := int64(*v)
	return &val
}

func stringSliceJSONPtr(v []string) *string {
	if len(v) == 0 {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
}

func (pw *ParquetWriterFull) Close() error {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	if pw.closed {
		return pw.closeErr
	}
	pw.closed = true

	var firstErr error
	for _, closer := range []interface{ Close() error }{
		pw.transactions, pw.operations, pw.effects, pw.trades,
		pw.accounts, pw.offers, pw.trustlines, pw.accountSigners,
		pw.claimableBalances, pw.liquidityPools, pw.configSettings,
		pw.ttl, pw.evictedKeys, pw.contractEvents, pw.contractData,
		pw.contractCode, pw.nativeBalances, pw.restoredKeys, pw.contractCreations,
		pw.ledgers, pw.tokenTransfers,
	} {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	pw.closeErr = firstErr
	return pw.closeErr
}

// The alias and constructor the worker uses, kept here with the writer they
// name. They lived in worker.go, which made the orchestration layer the
// declaring site for a sink type -- the one remaining cycle in the layering,
// because the writer and the table-name map both had to reach back for it.
// ParquetWriter is an alias for ParquetWriterFull (see parquet_writer.go)
type ParquetWriter = ParquetWriterFull

// NewParquetWriter creates a new Parquet writer for a worker.
func NewParquetWriter(outputDir string, workerID int, pipelineVersion string) (*ParquetWriter, error) {
	return NewParquetWriterFull(outputDir, workerID, pipelineVersion), nil
}
