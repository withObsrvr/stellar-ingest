package ingest

import (
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// LedgerMeta contains pre-extracted ledger metadata to avoid redundant XDR parsing
type LedgerMeta struct {
	LedgerSequence uint32
	ClosedAt       time.Time
	LedgerRange    uint32
	EraID          *string
	LCM            xdr.LedgerCloseMeta
}

// LedgerData holds all extracted data for a single ledger
type LedgerData struct {
	Meta              LedgerMeta
	Transactions      []TransactionData
	Operations        []OperationData
	Effects           []EffectData
	Trades            []TradeData
	Accounts          []AccountData
	Offers            []OfferData
	Trustlines        []TrustlineData
	AccountSigners    []AccountSignerData
	ClaimableBalances []ClaimableBalanceData
	LiquidityPools    []LiquidityPoolData
	ConfigSettings    []ConfigSettingData
	TTLEntries        []TTLData
	EvictedKeys       []EvictedKeyData
	ContractEvents    []ContractEventData
	ContractData      []ContractDataData
	ContractCode      []ContractCodeData
	NativeBalances    []NativeBalanceData
	RestoredKeys      []RestoredKeyData
	ContractCreations []ContractCreationData
	Ledgers           []LedgerRowData
	TokenTransfers    []TokenTransferData
}

// TransactionData represents a single transaction
type TransactionData struct {
	LedgerSequence        uint32
	TransactionHash       string
	SourceAccount         string
	SourceAccountMuxed    *string
	FeeCharged            int64
	MaxFee                int64
	Successful            bool
	TransactionResultCode string
	OperationCount        int
	MemoType              *string
	Memo                  *string
	// MemoRaw is base64 of the raw memo bytes, set for text memos only and
	// authoritative when set. A MEMO_TEXT payload is opaque bytes that need
	// not be valid UTF-8, so Memo carries the text only when it genuinely is
	// text and is NULL otherwise — a value the producer cannot represent is
	// absent, not mangled. id, hash and return memos are already lossless as
	// decimal and hex, so MemoRaw stays nil for them.
	MemoRaw           *string
	CreatedAt         time.Time
	AccountSequence   int64
	LedgerRange       uint32
	SignaturesCount   int
	NewAccount        bool
	TimeboundsMinTime *int64
	TimeboundsMaxTime *int64
	// Soroban fields
	SorobanHostFunctionType *string
	SorobanContractID       *string
	RentFeeCharged          *int64
	// Soroban resource fields
	SorobanResourcesInstructions *int64
	SorobanResourcesReadBytes    *int64
	SorobanResourcesWriteBytes   *int64
	// Declared footprint entry counts. The protocol charges ledger capacity
	// against what a transaction declares, not against the changes it turns
	// out to make, so these are the numerators for a capacity meter. Counting
	// observed changes instead undercounts badly.
	SorobanFootprintReadEntries  *int32
	SorobanFootprintWriteEntries *int32
	// Contract failure cause, set only when the transaction emitted an
	// ScError. Successful transactions and classic failures leave it nil.
	ContractErrorType *string
	ContractErrorCode *int32
	// Fee-bump wrapper. Non-nil only on a FEE_BUMP envelope, where an outer
	// transaction pays for an inner one. MaxFee above stays the inner bid,
	// which is what TransactionEnvelope.Fee() reports for a fee bump.
	FeeBumpFee           *int64
	InnerTransactionHash *string
	InnerSourceAccount   *string
	// FeeAccountMuxed is the M-address of the fee source, non-nil only when
	// that source is muxed.
	FeeAccountMuxed *string

	// Preconditions (CAP-0021). Each is non-nil only when the transaction
	// declares it: "no minimum sequence number" and "minimum sequence
	// number zero" are different, so an undeclared precondition is nil
	// rather than zero.
	LedgerboundsMin   *int64
	LedgerboundsMax   *int64
	MinSequenceNumber *int64
	MinSequenceAge    *int64
	// ExtraSigners is a JSON array of strkey signer keys in declared order,
	// nil when none are declared.
	ExtraSigners *string

	// Declared Soroban resources, from the envelope. These are what the
	// transaction asked for, not what it was charged -- the protocol meters
	// ledger capacity against the declaration.
	SorobanDataSizeBytes *int32
	SorobanDataResources *string
	// SorobanContractEventsCount counts the contract events in the
	// transaction's Soroban meta. Distinct from the contract_events table's
	// row count for the transaction, which also carries diagnostic events.
	SorobanContractEventsCount *int32

	// Raw XDR, base64-encoded, and the signature list. These are what makes
	// a Bronze transaction row reprocessable without going back to the
	// archive: every derived column above can be recomputed from TxEnvelope,
	// TxResult and TxMeta. Horizon exposes the same five.
	TxEnvelope *string
	TxResult   *string
	TxMeta     *string
	TxFeeMeta  *string
	// TxSigners is a JSON array of base64 signatures, in envelope order.
	TxSigners *string
	// EnvelopeSizeBytes is the length of the decoded envelope XDR, not of its
	// base64 encoding — the size the protocol charges against.
	EnvelopeSizeBytes int
	// TOID
	TransactionID int64
	EraID         *string
}

// OperationData represents a single operation
type OperationData struct {
	TransactionHash       string
	TransactionIndex      int
	OperationIndex        int
	LedgerSequence        uint32
	SourceAccount         string
	SourceAccountMuxed    *string
	OpType                int
	TypeString            string
	CreatedAt             time.Time
	TransactionSuccessful bool
	OperationResultCode   *string
	LedgerRange           uint32
	// Core operation fields
	Amount      *int64
	Asset       *string
	Destination *string
	// Decomposed asset fields
	AssetType   *string
	AssetCode   *string
	AssetIssuer *string
	// Source asset (path payments)
	SourceAsset       *string
	SourceAssetType   *string
	SourceAssetCode   *string
	SourceAssetIssuer *string
	SourceAmount      *int64
	DestinationMin    *int64
	StartingBalance   *int64
	// Trustline
	TrustlineLimit *int64
	// Offer fields
	OfferID *int64
	Price   *string
	PriceR  *string
	// Buying/Selling assets (offers)
	BuyingAsset        *string
	BuyingAssetType    *string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	SellingAsset       *string
	SellingAssetType   *string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	// Set options
	SetFlags        *int
	ClearFlags      *int
	HomeDomain      *string
	MasterWeight    *int
	LowThreshold    *int
	MediumThreshold *int
	HighThreshold   *int
	// Manage data
	DataName  *string
	DataValue *string
	// Claimable balance
	BalanceID *string
	// Sponsorship
	SponsoredID *string
	// Bump sequence
	BumpTo *int64
	// Soroban
	SorobanAuthRequired  *bool
	SorobanOperation     *string
	SorobanContractID    *string
	SorobanFunction      *string
	SorobanArgumentsJSON *string
	// Call graph fields
	ContractCallsJSON *string
	ContractsInvolved []string
	MaxCallDepth      *int

	// Soroban authorization credentials, one entry per
	// SorobanAuthorizationEntry on the operation. Parallel arrays of equal
	// length: Types holds the credentials discriminant and Addresses the
	// strkey authorizer, empty for SOURCE_ACCOUNT where the authorizer is
	// the transaction source. Both nil on non-Soroban operations and on
	// InvokeHostFunction operations with no auth entries, which keeps
	// "no auth" distinct from "an empty authorizer".
	SorobanAuthCredentialsTypes []string
	SorobanAuthAddresses        []string
	// TOID
	TransactionID int64
	OperationID   int64
	EraID         *string
}

// EffectData represents a single effect (state changes from operations)
type EffectData struct {
	// Identity
	LedgerSequence  uint32
	TransactionHash string
	OperationIndex  int
	EffectIndex     int

	// Effect type
	EffectType       int
	EffectTypeString string

	// Account affected
	AccountID *string

	// Amount changes
	Amount      *string
	AssetCode   *string
	AssetIssuer *string
	AssetType   *string

	// Trustline effects
	TrustlineLimit *string
	AuthorizeFlag  *bool
	ClawbackFlag   *bool

	// Signer effects
	SignerAccount *string
	SignerWeight  *int

	// Offer effects
	OfferID       *int64
	SellerAccount *string

	// TOID
	OperationID *int64
	DetailsJSON *string

	// Metadata
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// TradeData represents a single trade execution (DEX trades)
type TradeData struct {
	// Identity
	LedgerSequence  uint32
	TransactionHash string
	OperationIndex  int
	TradeIndex      int

	// Trade details
	TradeType      string
	TradeTimestamp time.Time

	// Seller side
	SellerAccount      string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	SellingAmount      string

	// Buyer side
	BuyerAccount      string
	BuyingAssetCode   *string
	BuyingAssetIssuer *string
	BuyingAmount      string

	// Price
	Price string

	// Pool
	//
	// LiquidityPoolID is the hex-encoded pool hash, non-nil exactly when
	// TradeType is "liquidity_pool". An AMM match has no resting offer and
	// no counterparty account, so SellerAccount is empty on those rows and
	// this identifies the pool instead.
	LiquidityPoolID *string

	// Metadata
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// AccountData represents account snapshot state (accounts_snapshot_v1)
// 23 fields total - captures complete account state including flags, thresholds, signers
type AccountData struct {
	// Identity (3 fields)
	AccountID      string
	LedgerSequence uint32
	ClosedAt       time.Time

	// Balance (1 field)
	Balance string

	// Account settings (5 fields)
	SequenceNumber uint64
	SequenceLedger uint32
	SequenceTime   uint64
	NumSubentries  uint32
	NumSponsoring  uint32
	NumSponsored   uint32
	HomeDomain     *string

	// Thresholds (4 fields)
	MasterWeight  uint32
	LowThreshold  uint32
	MedThreshold  uint32
	HighThreshold uint32

	// Flags (5 fields)
	Flags               uint32
	AuthRequired        bool
	AuthRevocable       bool
	AuthImmutable       bool
	AuthClawbackEnabled bool

	// Signers (1 field) - JSON array
	Signers *string

	// Sponsorship (1 field)
	SponsorAccount *string

	// Metadata (3 fields)
	CreatedAt   time.Time
	UpdatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// OfferData represents DEX offer snapshot state (offers_snapshot_v1)
// 15 fields total - captures orderbook state with selling/buying assets, amount, and price
type OfferData struct {
	// Identity (4 fields)
	OfferID        int64
	SellerAccount  string
	LedgerSequence uint32
	ClosedAt       time.Time

	// Selling asset (3 fields)
	SellingAssetType   string
	SellingAssetCode   *string
	SellingAssetIssuer *string

	// Buying asset (3 fields)
	BuyingAssetType   string
	BuyingAssetCode   *string
	BuyingAssetIssuer *string

	// Offer details (2 fields)
	Amount string
	Price  string

	// Flags (1 field)
	Flags uint32

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// TrustlineData represents trustline snapshot state (trustlines_snapshot_v1)
// 14 fields total - captures non-native asset holdings and trust relationships
type TrustlineData struct {
	// Identity (4 fields)
	AccountID   string
	AssetCode   string
	AssetIssuer string
	AssetType   string

	// Trust & Balance (4 fields)
	Balance            string
	TrustLimit         string
	BuyingLiabilities  string
	SellingLiabilities string

	// Authorization (3 fields)
	Authorized                      bool
	AuthorizedToMaintainLiabilities bool
	ClawbackEnabled                 bool

	// Metadata (3 fields)
	LedgerSequence uint32
	CreatedAt      time.Time
	LedgerRange    uint32
	EraID          *string
}

// AccountSignerData represents account signer snapshot state (account_signers_snapshot_v1)
// 9 fields total - tracks multi-sig configurations and signer changes
type AccountSignerData struct {
	// Identity (3 fields)
	AccountID      string
	Signer         string
	LedgerSequence uint32

	// Signer details (2 fields)
	Weight  uint32
	Sponsor string // Nullable - sponsor account ID

	// Status (1 field)
	Deleted bool

	// Metadata (3 fields)
	ClosedAt    time.Time
	LedgerRange uint32
	CreatedAt   time.Time
	EraID       *string
}

// ClaimableBalanceData represents claimable balance snapshot state (claimable_balances_snapshot_v1)
// 12 fields total - Protocol 14+ feature for deferred payments
type ClaimableBalanceData struct {
	// Identity (4 fields)
	BalanceID      string
	Sponsor        string // Nullable
	LedgerSequence uint32
	ClosedAt       time.Time

	// Asset & Amount (4 fields)
	AssetType   string
	AssetCode   *string // Nullable
	AssetIssuer *string // Nullable
	Amount      int64

	// Claimants (1 field)
	ClaimantsCount int32

	// Flags (1 field)
	Flags uint32

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// LiquidityPoolData represents liquidity pool snapshot state (liquidity_pools_snapshot_v1)
// 17 fields total - Protocol 18+ AMM pools
type LiquidityPoolData struct {
	// Identity (3 fields)
	LiquidityPoolID string
	LedgerSequence  uint32
	ClosedAt        time.Time

	// Pool Type (1 field)
	PoolType string // "constant_product"

	// Fee (1 field)
	Fee int32 // Basis points (30 = 0.3%)

	// Pool Shares (2 fields)
	TrustlineCount  int32
	TotalPoolShares int64

	// Asset A (4 fields)
	AssetAType   string
	AssetACode   *string // Nullable
	AssetAIssuer *string // Nullable
	AssetAAmount int64

	// Asset B (4 fields)
	AssetBType   string
	AssetBCode   *string // Nullable
	AssetBIssuer *string // Nullable
	AssetBAmount int64

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// ConfigSettingData represents network configuration settings snapshot (config_settings_snapshot_v1)
// 19 fields total - Protocol 20+ Soroban configuration parameters
type ConfigSettingData struct {
	// Identity (2 fields)
	ConfigSettingID int32
	LedgerSequence  uint32

	// Ledger metadata (3 fields)
	LastModifiedLedger int32
	Deleted            bool
	ClosedAt           time.Time

	// Soroban compute settings (4 fields, nullable)
	LedgerMaxInstructions           *int64
	TxMaxInstructions               *int64
	FeeRatePerInstructionsIncrement *int64
	TxMemoryLimit                   *uint32

	// Soroban ledger cost settings (8 fields, nullable)
	LedgerMaxReadLedgerEntries  *uint32
	LedgerMaxReadBytes          *uint32
	LedgerMaxWriteLedgerEntries *uint32
	LedgerMaxWriteBytes         *uint32
	TxMaxReadLedgerEntries      *uint32
	TxMaxReadBytes              *uint32
	TxMaxWriteLedgerEntries     *uint32
	TxMaxWriteBytes             *uint32

	// TxMaxFootprintEntries is CAP-0062's per-transaction cap on declared
	// footprint entries. It arrives in its own config setting entry
	// (ContractLedgerCostExtV0), not alongside the ledger-wide limits above.
	TxMaxFootprintEntries *uint32

	// Contract size limit (1 field, nullable)
	ContractMaxSizeBytes *uint32

	// Raw XDR (1 field)
	ConfigSettingXDR string

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string

	// Observation provenance. History extraction observes these rows from
	// LedgerCloseMeta; current-state reconciliation is performed only by the
	// streaming ingester.
	ObservationSource string
	ResolvedAtLedger  *int64
}

// TTLData represents time-to-live entries snapshot (ttl_snapshot_v1)
// 10 fields total - Protocol 20+ Soroban storage expiration tracking
type TTLData struct {
	// Identity (2 fields)
	KeyHash        string
	LedgerSequence uint32

	// TTL tracking (3 fields)
	LiveUntilLedgerSeq uint32
	TTLRemaining       int64
	Expired            bool

	// Ledger metadata (3 fields)
	LastModifiedLedger int32
	Deleted            bool
	ClosedAt           time.Time

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// EvictedKeyData represents evicted storage keys state (evicted_keys_state_v1)
// 8 fields total - Protocol 20+ Soroban archival tracking (LCM V1/V2)
type EvictedKeyData struct {
	// Identity (2 fields)
	KeyHash        string
	LedgerSequence uint32

	// Eviction details (3 fields)
	ContractID string
	KeyType    string
	Durability string

	// Metadata (3 fields)
	ClosedAt    time.Time
	LedgerRange uint32
	CreatedAt   time.Time
	EraID       *string
}

// ContractEventData represents Soroban contract events stream (contract_events_stream_v1)
// 16 fields total - Protocol 20+ Soroban smart contract events
type ContractEventData struct {
	// Identity (5 fields)
	EventID         string
	ContractID      *string
	LedgerSequence  uint32
	TransactionHash string
	// TransactionID is the TOID of the transaction this event belongs to,
	// the same value TransactionData.TransactionID carries for the parent
	// row. Added in stellar-extract v0.3.6.
	//
	// Position matters: this type is converted directly from
	// extract.ContractEventData, so the fields have to line up. The compiler
	// caught the mismatch when the pin moved, which is the only check that
	// layer gets.
	TransactionID int64
	ClosedAt      time.Time

	// Event Type (4 fields)
	EventType                string
	InSuccessfulContractCall bool
	Successful               bool
	ContractEventXDR         string

	// Event Data (5 fields - Hubble compatible with decoded versions)
	TopicsJSON    string
	TopicsDecoded string
	DataXDR       string
	DataDecoded   string
	TopicCount    int32

	// Positional topic values (flattened for indexed querying)
	Topic0Decoded *string
	Topic1Decoded *string
	Topic2Decoded *string
	Topic3Decoded *string

	// Context (2 fields)
	OperationIndex uint32
	// EventStream names which of a transaction's three event lists this row
	// came from: "diagnostic", "operation" or "transaction". Only operation
	// events have a real OperationIndex.
	EventStream string
	// EventIndex is the row's position within its transaction, counted
	// across all three streams, which is what makes it a key component.
	EventIndex uint32

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// ContractCall represents a single cross-contract call in the call graph
// Used for tracking nested invocations (e.g., Soroswap -> Aquarius -> Token)
type ContractCall struct {
	FromContract   string      `json:"from_contract"`       // Caller contract ID
	ToContract     string      `json:"to_contract"`         // Callee contract ID
	FunctionName   string      `json:"function"`            // Function being called
	Arguments      interface{} `json:"arguments,omitempty"` // Function arguments (decoded from ScVal)
	CallDepth      int         `json:"call_depth"`          // Depth in call hierarchy (1 = direct, 2 = nested)
	ExecutionOrder int         `json:"execution_order"`     // Order of execution within transaction
	Successful     bool        `json:"successful"`          // Whether this call succeeded
}

// CallGraphResult contains the extracted call graph for an operation
type CallGraphResult struct {
	Calls             []ContractCall // Flattened list of calls
	ContractsInvolved []string       // All unique contract IDs in the call chain
	MaxDepth          int            // Maximum call depth
}

// ContractDataData represents Soroban contract data snapshot (contract_data_snapshot_v1)
// 17 fields total - Protocol 20+ Soroban smart contract storage with SAC detection
type ContractDataData struct {
	// Identity (3 fields)
	ContractId     string
	LedgerSequence uint32
	LedgerKeyHash  string

	// Contract metadata (2 fields)
	ContractKeyType    string
	ContractDurability string

	// Asset information (3 fields, nullable - SAC detection)
	AssetCode   *string
	AssetIssuer *string
	AssetType   *string

	// Balance information (2 fields, nullable - SAC detection)
	BalanceHolder *string
	Balance       *string

	// Ledger metadata (4 fields)
	LastModifiedLedger int32
	LedgerEntryChange  int32
	Deleted            bool
	ClosedAt           time.Time

	// XDR data (1 field)
	ContractDataXDR string

	// Token metadata (3 fields, nullable - from METADATA key in instance storage)
	TokenName     *string
	TokenSymbol   *string
	TokenDecimals *int32

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// ContractCodeData represents Soroban contract code snapshot (contract_code_snapshot_v1)
// 18 fields total - Protocol 20+ Soroban WASM contract deployments
type ContractCodeData struct {
	// Identity (2 fields)
	ContractCodeHash string
	LedgerKeyHash    string

	// Extension (1 field)
	ContractCodeExtV int32

	// Ledger metadata (4 fields)
	LastModifiedLedger int32
	LedgerEntryChange  int32
	Deleted            bool
	ClosedAt           time.Time

	// Ledger tracking (1 field)
	LedgerSequence uint32

	// WASM metadata (10 fields, all nullable)
	NInstructions     *int64
	NFunctions        *int64
	NGlobals          *int64
	NTableEntries     *int64
	NTypes            *int64
	NDataSegments     *int64
	NElemSegments     *int64
	NImports          *int64
	NExports          *int64
	NDataSegmentBytes *int64

	// Metadata (2 fields)
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// NativeBalanceData represents XLM-only balances snapshot (native_balances_snapshot_v1)
// 11 fields total - Native (XLM) balance tracking
type NativeBalanceData struct {
	// Identity (1 field)
	AccountID string

	// Balance details (7 fields)
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubentries      int32
	NumSponsoring      int32
	NumSponsored       int32
	SequenceNumber     *int64 // Nullable

	// Ledger tracking (2 fields)
	LastModifiedLedger int64
	LedgerSequence     int64

	// Partition key (1 field)
	LedgerRange int64
	EraID       *string
}

// RestoredKeyData represents restored storage keys state (restored_keys_state_v1)
// 10 fields total - Protocol 20+ Soroban archival restoration tracking
type RestoredKeyData struct {
	// Identity (2 fields)
	KeyHash        string
	LedgerSequence uint32

	// Restoration details (4 fields)
	ContractID         string
	KeyType            string
	Durability         string
	RestoredFromLedger uint32

	// Metadata (3 fields)
	ClosedAt    time.Time
	LedgerRange uint32
	CreatedAt   time.Time
	EraID       *string
}

// LedgerRowData represents a single ledger row (ledgers_row_v2)
type LedgerRowData struct {
	Sequence             uint32
	LedgerHash           string
	PreviousLedgerHash   string
	ClosedAt             time.Time
	ProtocolVersion      uint32
	TotalCoins           int64
	FeePool              int64
	BaseFee              uint32
	BaseReserve          uint32
	MaxTxSetSize         uint32
	TransactionCount     int
	OperationCount       int
	SuccessfulTxCount    int
	FailedTxCount        int
	TxSetOperationCount  int
	SorobanFeeWrite1kb   *int64
	NodeID               *string
	Signature            *string
	LedgerHeader         *string
	BucketListSize       *int64
	LiveSorobanStateSize *int64
	EvictedKeysCount     *int32
	SorobanOpCount       *int32
	TotalFeeCharged      *int64
	ContractEventsCount  *int32
	IngestionTimestamp   time.Time
	LedgerRange          uint32
	PipelineVersion      string
	EraID                *string
}

// ContractCreationData represents a contract creation event
type ContractCreationData struct {
	ContractID     string
	CreatorAddress string
	WasmHash       *string
	// ExecutableType is "wasm", "stellar_asset", or "external_ref"
	// (Protocol 28, CAP-0085). It exists because a nil WasmHash alone no
	// longer implies a SAC deployment: the inference this replaced broke
	// when the third kind arrived, and it broke silently.
	ExecutableType   string
	ExternalRefOwner *string
	ExternalRefTag   *string
	CreatedLedger    uint32
	CreatedAt        time.Time
	LedgerRange      uint32
	EraID            *string
}

// TokenTransferData represents a unified token transfer event (token_transfers_stream_v1)
// Covers: transfer, mint, burn, clawback, fee events from both classic and Soroban operations
type TokenTransferData struct {
	LedgerSequence  uint32
	TransactionHash string
	TransactionID   int64  // TOID of parent transaction
	OperationID     *int64 // TOID of parent operation (nullable - fee events have no operation)
	OperationIndex  *int32 // 1-indexed (nullable)
	// EventIndex is the row's position within its transaction. Nothing else
	// identifies a transfer: one operation may transfer the same amount
	// between the same parties twice.
	EventIndex  uint32
	EventType   string  // transfer, mint, burn, clawback, fee
	From        *string // nullable (mint has no from)
	To          *string // nullable (burn/clawback/fee have no to)
	Asset       string  // canonical: "native" or "credit_alphanum4:CODE:ISSUER"
	AssetType   string  // native, credit_alphanum4, credit_alphanum12
	AssetCode   *string // nullable (native has none)
	AssetIssuer *string // nullable
	Amount      float64 // human-readable (stroops * 0.0000001)
	AmountRaw   string  // raw stroops string from SDK
	ContractID  string
	ClosedAt    time.Time
	CreatedAt   time.Time
	LedgerRange uint32
	EraID       *string
}

// WASMMetadata holds parsed metadata from a WASM binary
type WASMMetadata struct {
	NInstructions     *int64
	NFunctions        *int64
	NGlobals          *int64
	NTableEntries     *int64
	NTypes            *int64
	NDataSegments     *int64
	NElemSegments     *int64
	NImports          *int64
	NExports          *int64
	NDataSegmentBytes *int64
}
