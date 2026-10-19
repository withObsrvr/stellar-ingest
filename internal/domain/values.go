package domain

import (
	"github.com/withObsrvr/stellar-ingest/internal/logging"
	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// Value objects and configuration, gathered here ahead of the package split.
//
// Each of these sat in a file that will become jobs or adapters while being
// referenced from the other side, which is what made the layering cyclic:
//
//	PrefetchedLedger, BatchData   worker.go, read by the source and the writer
//	OrchestratorConfig            orchestrator.go, read by the manifest
//	LineageRecord                 lineage.go, read by the manifest
//	MapToDuckLakeTable            ducklake.go, read by table selection
//
// None of them is behaviour. They are the shapes the layers agree on, so they
// belong with the other shapes rather than with whichever file happened to
// declare them first.

type PrefetchedLedger struct {
	// Exported because this type crosses a package boundary now: the source
	// constructs it in adapters and the worker reads it in jobs.
	Sequence uint32
	Data     []byte
	Err      error
}

// BatchData holds all extracted data from multiple ledgers, ready for flushing
// to the ParquetWriter.
type BatchData struct {
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

// OrchestratorConfig holds configuration for the orchestrator.
type OrchestratorConfig struct {
	// StatusEmitter publishes OSC 7501 program status so a terminal or UI can
	// follow the run. Nil disables it, and every call is then a no-op.
	StatusEmitter *status.Emitter

	// Log receives the orchestrator's narration: shard layout, worker
	// starts, errors. Nil is safe -- every method on logging.Logger
	// tolerates a nil receiver -- so a caller that does not narrate passes
	// nothing rather than a discarding instance.
	Log *logging.Logger

	StartLedger       uint32
	EndLedger         uint32
	NumWorkers        int
	OutputDir         string
	BatchSize         int
	NetworkPassphrase string
	StorageType       string
	Bucket            string
	LedgersPerFile    uint32
	FilesPerPartition uint32
	EraID             string
	OnlyTables        map[string]bool
}

// LineageRecord tracks one pipeline run's output for a specific table.
type LineageRecord struct {
	LineageID         string `parquet:"lineage_id"`
	Dataset           string `parquet:"dataset"`
	SourceLedgerStart uint32 `parquet:"source_ledger_start"`
	SourceLedgerEnd   uint32 `parquet:"source_ledger_end"`
	PipelineName      string `parquet:"pipeline_name"`
	PipelineVersion   string `parquet:"pipeline_version"`
	RecordsProcessed  int64  `parquet:"records_processed"`
	StartedAt         int64  `parquet:"started_at,timestamp(microsecond)"`
	CompletedAt       int64  `parquet:"completed_at,timestamp(microsecond)"`
	DurationMs        int64  `parquet:"duration_ms"`
	WorkerID          int32  `parquet:"worker_id"`
}

// MapToDuckLakeTable maps Parquet directory names to DuckLake table names.
func MapToDuckLakeTable(dirName string) string {
	return DuckLakeTableBySource[dirName]
}

// DefaultPrefetchBuffer is how many decoded ledgers a worker buffers.
//
// Shared rather than owned: the source fills this channel and the worker
// drains it, so neither layer can hold the number without the other reaching
// across. It is also the whole of the loader's memory model -- peak is
// roughly chunks x workers x this x ledger size -- which is why it is worth
// stating in one place.
const DefaultPrefetchBuffer = 200

// A method on BatchData, so it travels with the type.
//
// It lived in worker.go, which is what made BatchData look like a pure value
// object when it is not: it has behaviour, and a type cannot have methods
// defined outside its own package. Exported because the worker calls it from
// the other side of the boundary now.
// MergeLedger appends all data from a single LedgerData into the batch.
func (b *BatchData) MergeLedger(ld *LedgerData) {
	b.Transactions = append(b.Transactions, ld.Transactions...)
	b.Operations = append(b.Operations, ld.Operations...)
	b.Effects = append(b.Effects, ld.Effects...)
	b.Trades = append(b.Trades, ld.Trades...)
	b.Accounts = append(b.Accounts, ld.Accounts...)
	b.Offers = append(b.Offers, ld.Offers...)
	b.Trustlines = append(b.Trustlines, ld.Trustlines...)
	b.AccountSigners = append(b.AccountSigners, ld.AccountSigners...)
	b.ClaimableBalances = append(b.ClaimableBalances, ld.ClaimableBalances...)
	b.LiquidityPools = append(b.LiquidityPools, ld.LiquidityPools...)
	b.ConfigSettings = append(b.ConfigSettings, ld.ConfigSettings...)
	b.TTLEntries = append(b.TTLEntries, ld.TTLEntries...)
	b.EvictedKeys = append(b.EvictedKeys, ld.EvictedKeys...)
	b.ContractEvents = append(b.ContractEvents, ld.ContractEvents...)
	b.ContractData = append(b.ContractData, ld.ContractData...)
	b.ContractCode = append(b.ContractCode, ld.ContractCode...)
	b.NativeBalances = append(b.NativeBalances, ld.NativeBalances...)
	b.RestoredKeys = append(b.RestoredKeys, ld.RestoredKeys...)
	b.ContractCreations = append(b.ContractCreations, ld.ContractCreations...)
	b.Ledgers = append(b.Ledgers, ld.Ledgers...)
	b.TokenTransfers = append(b.TokenTransfers, ld.TokenTransfers...)
}
