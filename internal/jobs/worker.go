package jobs

import (
	"context"
	"fmt"
	"github.com/withObsrvr/stellar-ingest/internal/adapters"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	extract "github.com/withObsrvr/stellar-extract"
)

// ---------------------------------------------------------------------------
// Ledger Source Interface & Filesystem Stub
// ---------------------------------------------------------------------------

// LedgerSource provides an ordered, bounded stream of raw ledger data.
type LedgerSource interface {
	StreamRange(ctx context.Context, start, end uint32) <-chan domain.PrefetchedLedger
	Close() error
}

// FilesystemSource reads ledger XDR files from a local directory.
// File layout expected: <basePath>/<sequence>.xdr
type FilesystemSource struct {
	basePath string
}

// NewFilesystemSource creates a new FilesystemSource.
func NewFilesystemSource(basePath string) (*FilesystemSource, error) {
	info, err := os.Stat(basePath)
	if err != nil {
		return nil, fmt.Errorf("filesystem source: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("filesystem source: %q is not a directory", basePath)
	}
	return &FilesystemSource{basePath: basePath}, nil
}

// GetLedger reads the XDR bytes for a given ledger sequence from disk.
func (fs *FilesystemSource) GetLedger(sequence uint32) ([]byte, error) {
	path := filepath.Join(fs.basePath, fmt.Sprintf("%d.xdr", sequence))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ledger %d: %w", sequence, err)
	}
	return data, nil
}

func (fs *FilesystemSource) StreamRange(ctx context.Context, start, end uint32) <-chan domain.PrefetchedLedger {
	return adapters.StreamSequentialRange(ctx, start, end, fs.GetLedger)
}

// Close is a no-op for the filesystem source.
func (fs *FilesystemSource) Close() error {
	return nil
}

// NewLedgerSourceFromConfig creates the appropriate LedgerSource for the given config.
func NewLedgerSourceFromConfig(config domain.OrchestratorConfig) (LedgerSource, error) {
	switch config.StorageType {
	case "FS":
		return NewFilesystemSource(config.Bucket)
	case "XDR":
		return adapters.NewXDRStreamSource(config.Bucket)
	case "RPC", "CAPTIVE_CORE":
		return adapters.NewSDKStreamSource(config)
	case "GCS":
		return adapters.NewSDKStreamSource(config)
	case "S3":
		return adapters.NewSDKStreamSource(config)
	default:
		return nil, fmt.Errorf("unknown storage type: %q", config.StorageType)
	}
}

// ---------------------------------------------------------------------------
// Batch: aggregated domain.LedgerData across multiple ledgers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Prefetched ledger item sent through the read-ahead channel
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

// shardWriter is the worker's Parquet boundary. Close finalizes file footers;
// a shard must not be checkpointed as complete until it succeeds. This stays
// private: it does not represent a DuckLake or PostgreSQL commit contract.
type shardWriter interface {
	WriteBatch(*domain.BatchData) error
	Close() error
}

// Worker processes a shard of ledgers through the extraction pipeline.
type Worker struct {
	id         int
	shard      Shard
	config     domain.OrchestratorConfig
	progress   *ProgressTracker
	writer     shardWriter
	source     LedgerSource       // shared across workers for XDR mode, per-worker for FS/GCS
	checkpoint *CheckpointManager // optional, for resume support
	lineage    *LineageWriter     // optional, for lineage tracking
}

// NewWorker creates a new Worker for the given shard.
func NewWorker(id int, shard Shard, config domain.OrchestratorConfig, progress *ProgressTracker, writer shardWriter, source LedgerSource, checkpoint *CheckpointManager, lineage *LineageWriter) *Worker {
	return &Worker{
		id:         id,
		shard:      shard,
		config:     config,
		progress:   progress,
		writer:     writer,
		source:     source,
		checkpoint: checkpoint,
		lineage:    lineage,
	}
}

// Run executes the full worker pipeline:
//
//  1. Create ledger source
//  2. Start prefetcher goroutine (reads ahead into a buffered channel)
//  3. Loop: receive from prefetch channel, decode XDR once, extract all
//     tables in parallel, accumulate batch
//  4. Every batchSize ledgers, flush to ParquetWriter
//  5. Final flush for remaining data
//  6. Report final progress
func (w *Worker) Run(ctx context.Context) error {
	// 1. Use the shared source. SDK LedgerStream owns an independent backend
	// lifecycle for every StreamRange invocation, so concurrent shards do not
	// share PrepareRange/GetLedger state.
	source := w.source

	// 2. Check for resume point
	effectiveShard := w.shard
	if w.checkpoint != nil {
		resumeFrom, skip := w.checkpoint.GetShardResumePoint(w.id, w.shard)
		if skip {
			log.Printf("[Worker %d] Shard already completed, skipping", w.id)
			return nil
		}
		if resumeFrom > effectiveShard.StartLedger {
			log.Printf("[Worker %d] Resuming from ledger %d (was %d)", w.id, resumeFrom, effectiveShard.StartLedger)
			effectiveShard.StartLedger = resumeFrom
		}
	}

	// 3. Start prefetcher goroutine
	prefetchCh := source.StreamRange(ctx, effectiveShard.StartLedger, effectiveShard.EndLedger)

	// 4-5. Process ledgers in batches
	batch := &domain.BatchData{}
	batchCount := 0
	var lastSeq uint32
	var batchStartSeq uint32
	batchStartTime := time.Now()

	for item := range prefetchCh {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle prefetch errors
		if item.Err != nil {
			return fmt.Errorf("[Worker %d] prefetch ledger %d: %w", w.id, item.Sequence, item.Err)
		}

		// Decode XDR ONCE per ledger
		meta, err := w.decodeLedger(item.Sequence, item.Data)
		if err != nil {
			return fmt.Errorf("[Worker %d] decode ledger %d: %w", w.id, item.Sequence, err)
		}

		// Extract all 21 data types in parallel
		ledgerData, err := w.extractLedger(meta)
		if err != nil {
			return fmt.Errorf("[Worker %d] extract ledger %d: %w", w.id, item.Sequence, err)
		}

		// Accumulate into batch
		if batchCount == 0 {
			batchStartSeq = item.Sequence
			batchStartTime = time.Now()
		}
		batch.MergeLedger(ledgerData)
		batchCount++
		lastSeq = item.Sequence

		// Flush when batch is full
		if batchCount >= w.config.BatchSize {
			if err := w.writer.WriteBatch(batch); err != nil {
				return fmt.Errorf("[Worker %d] write batch: %w", w.id, err)
			}
			w.progress.ReportProgress(w.id, uint64(batchCount))
			w.recordLineage(batch, batchStartSeq, lastSeq, time.Since(batchStartTime))
			batch = &domain.BatchData{}
			batchCount = 0
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("[Worker %d] source stream canceled: %w", w.id, err)
	}

	// 5. Final flush for remaining data
	if batchCount > 0 {
		if err := w.writer.WriteBatch(batch); err != nil {
			return fmt.Errorf("[Worker %d] write final batch: %w", w.id, err)
		}
		w.progress.ReportProgress(w.id, uint64(batchCount))
		w.recordLineage(batch, batchStartSeq, lastSeq, time.Since(batchStartTime))
	}

	// 6. Close all table writers before publishing completion. Parquet footers
	// are written during Close, so a shard is not durable before this succeeds.
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("[Worker %d] finalize parquet: %w", w.id, err)
	}

	// 7. Mark the fully finalized shard complete in the checkpoint.
	if w.checkpoint != nil {
		w.checkpoint.CompleteShard(w.id, w.shard)
		if err := w.checkpoint.Save(); err != nil {
			return fmt.Errorf("[Worker %d] save completed checkpoint: %w", w.id, err)
		}
	}

	shardSize := w.shard.EndLedger - w.shard.StartLedger + 1
	log.Printf("[Worker %d] Completed: %d ledgers processed", w.id, shardSize)

	return nil
}

// recordLineage records per-table row counts for a batch in the lineage writer.
func (w *Worker) recordLineage(batch *domain.BatchData, startSeq, endSeq uint32, duration time.Duration) {
	if w.lineage == nil {
		return
	}
	tables := map[string]int{
		"transactions":                len(batch.Transactions),
		"operations":                  len(batch.Operations),
		"effects":                     len(batch.Effects),
		"trades":                      len(batch.Trades),
		"accounts_snapshot":           len(batch.Accounts),
		"offers_snapshot":             len(batch.Offers),
		"trustlines_snapshot":         len(batch.Trustlines),
		"account_signers_snapshot":    len(batch.AccountSigners),
		"claimable_balances_snapshot": len(batch.ClaimableBalances),
		"liquidity_pools_snapshot":    len(batch.LiquidityPools),
		"config_settings":             len(batch.ConfigSettings),
		"ttl_snapshot":                len(batch.TTLEntries),
		"evicted_keys":                len(batch.EvictedKeys),
		"contract_events":             len(batch.ContractEvents),
		"contract_data_snapshot":      len(batch.ContractData),
		"contract_code_snapshot":      len(batch.ContractCode),
		"native_balances":             len(batch.NativeBalances),
		"restored_keys":               len(batch.RestoredKeys),
		"contract_creations":          len(batch.ContractCreations),
		"ledgers":                     len(batch.Ledgers),
		"token_transfers":             len(batch.TokenTransfers),
	}
	for table, count := range tables {
		if count > 0 {
			w.lineage.RecordBatch(w.id, table, startSeq, endSeq, count, duration)
		}
	}
}

// decodeLedger unmarshals raw XDR bytes into a LedgerCloseMeta and extracts
// header metadata. This is done ONCE per ledger (not per extractor).
func (w *Worker) decodeLedger(sequence uint32, rawXDR []byte) (domain.LedgerMeta, error) {
	var eraID *string
	if w.config.EraID != "" {
		eraID = &w.config.EraID
	}
	return decodeLedgerMeta(sequence, rawXDR, eraID)
}

// decodeLedgerMeta is the worker-independent half of decodeLedger.
//
// It is separate so that the Ticket 04 parity harness enters the Bronze
// mapping through exactly the decode the production worker uses. A harness
// that reimplemented this — the close-time conversion and the 10,000-ledger
// range bucket in particular — would be comparing its own derivation of
// closed_at against the live ingester's, and a drift between the two would
// surface as a data discrepancy in every table that carries a timestamp.
func decodeLedgerMeta(sequence uint32, rawXDR []byte, eraID *string) (domain.LedgerMeta, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawXDR); err != nil {
		return domain.LedgerMeta{}, fmt.Errorf("unmarshal XDR: %w", err)
	}

	// Extract header metadata
	ledgerSeq := uint32(lcm.LedgerSequence())
	closedAt := time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	// Sanity check
	if ledgerSeq != sequence {
		return domain.LedgerMeta{}, fmt.Errorf("sequence mismatch: expected %d, got %d", sequence, ledgerSeq)
	}

	return domain.LedgerMeta{
		LCM:            lcm,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,
		LedgerRange:    ledgerRange,
		EraID:          eraID,
	}, nil
}

// extractorResult holds the output of a single extractor goroutine.
type extractorResult struct {
	name string
	data *domain.LedgerData
	err  error
}

// extractLedger preserves the worker call site while keeping the mapping
// independent of scheduling, sources, sinks, and checkpoint state.
func (w *Worker) extractLedger(meta domain.LedgerMeta) (*domain.LedgerData, error) {
	return ExtractBronzeLedger(meta, w.config.NetworkPassphrase, w.config.OnlyTables, w.id)
}

// ExtractBronzeLedger fans out to all 21 extractors over an already-decoded
// ledger. It is not exported yet: the live consumer's contract is unresolved.
func ExtractBronzeLedger(meta domain.LedgerMeta, np string, onlyTables map[string]bool, workerID int) (*domain.LedgerData, error) {
	lcm := meta.LCM
	lr := meta.LedgerRange

	resultCh := make(chan extractorResult, len(domain.ExtractorOutputDirByName))
	var wg sync.WaitGroup
	launched := 0

	// Helper to launch an extractor goroutine
	launch := func(name string, fn func() (*domain.LedgerData, error)) {
		if !shouldExtractTable(onlyTables, name) {
			return
		}
		launched++
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := fn()
			resultCh <- extractorResult{name: name, data: data, err: err}
		}()
	}

	// Create shared library input ONCE (read-only, safe for concurrent access)
	libInput := extract.NewLedgerInput(lcm, np)
	libInput.LedgerRange = lr
	libInput.EraID = meta.EraID

	// Fan out to all extractors.
	launch("transactions", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractTransactions(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.TransactionData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.TransactionData(r)
		}
		return &domain.LedgerData{Transactions: rows}, nil
	})
	launch("operations", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractOperations(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.OperationData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.OperationData(r)
		}
		return &domain.LedgerData{Operations: rows}, nil
	})
	launch("effects", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractEffects(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.EffectData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.EffectData(r)
		}
		return &domain.LedgerData{Effects: rows}, nil
	})
	launch("trades", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractTrades(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.TradeData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.TradeData(r)
		}
		return &domain.LedgerData{Trades: rows}, nil
	})
	launch("accounts", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractAccounts(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.AccountData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.AccountData(r)
		}
		return &domain.LedgerData{Accounts: rows}, nil
	})
	launch("offers", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractOffers(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.OfferData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.OfferData(r)
		}
		return &domain.LedgerData{Offers: rows}, nil
	})
	launch("trustlines", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractTrustlines(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.TrustlineData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.TrustlineData(r)
		}
		return &domain.LedgerData{Trustlines: rows}, nil
	})
	launch("account_signers", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractAccountSigners(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.AccountSignerData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.AccountSignerData(r)
		}
		return &domain.LedgerData{AccountSigners: rows}, nil
	})
	launch("claimable_balances", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractClaimableBalances(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.ClaimableBalanceData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.ClaimableBalanceData(r)
		}
		return &domain.LedgerData{ClaimableBalances: rows}, nil
	})
	launch("liquidity_pools", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractLiquidityPools(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.LiquidityPoolData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.LiquidityPoolData(r)
		}
		return &domain.LedgerData{LiquidityPools: rows}, nil
	})
	launch("config_settings", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractConfigSettings(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.ConfigSettingData, len(libRows))
		for i, r := range libRows {
			rows[i] = convertExtractedConfigSetting(r)
		}
		return &domain.LedgerData{ConfigSettings: rows}, nil
	})
	launch("ttl", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractTTL(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.TTLData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.TTLData(r)
		}
		return &domain.LedgerData{TTLEntries: rows}, nil
	})
	launch("evicted_keys", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractEvictedKeys(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.EvictedKeyData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.EvictedKeyData(r)
		}
		return &domain.LedgerData{EvictedKeys: rows}, nil
	})
	launch("contract_events", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractContractEvents(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.ContractEventData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.ContractEventData(r)
			if repairs := adapters.SanitizeContractEventUTF8(&rows[i]); repairs > 0 {
				log.Printf("[Worker %d] contract_events utf8_repaired ledger=%d transaction_id=%d event_index=%d repairs=%d", workerID, meta.LedgerSequence, rows[i].TransactionID, rows[i].EventIndex, repairs)
			}
		}
		return &domain.LedgerData{ContractEvents: rows}, nil
	})
	launch("contract_data", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractContractData(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.ContractDataData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.ContractDataData(r)
		}
		return &domain.LedgerData{ContractData: rows}, nil
	})
	launch("contract_code", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractContractCode(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.ContractCodeData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.ContractCodeData(r)
		}
		return &domain.LedgerData{ContractCode: rows}, nil
	})
	launch("native_balances", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractNativeBalances(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.NativeBalanceData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.NativeBalanceData(r)
		}
		return &domain.LedgerData{NativeBalances: rows}, nil
	})
	launch("restored_keys", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractRestoredKeys(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.RestoredKeyData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.RestoredKeyData(r)
		}
		return &domain.LedgerData{RestoredKeys: rows}, nil
	})
	launch("contract_creations", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractContractCreations(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.ContractCreationData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.ContractCreationData(r)
		}
		return &domain.LedgerData{ContractCreations: rows}, nil
	})
	launch("ledgers", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractLedgers(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.LedgerRowData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.LedgerRowData(r)
		}
		return &domain.LedgerData{Ledgers: rows}, nil
	})
	launch("token_transfers", func() (*domain.LedgerData, error) {
		libRows, err := extract.ExtractTokenTransfers(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]domain.TokenTransferData, len(libRows))
		for i, r := range libRows {
			rows[i] = domain.TokenTransferData(r)
		}
		return &domain.LedgerData{TokenTransfers: rows}, nil
	})

	if launched == 0 {
		return nil, fmt.Errorf("no extractors selected by --only-tables=%q", domain.TableSetKey(onlyTables))
	}

	// Wait for all extractors to complete, then close the results channel
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect and merge results
	merged := &domain.LedgerData{Meta: meta}
	var extractErrors []error

	for result := range resultCh {
		if result.err != nil {
			extractErrors = append(extractErrors, fmt.Errorf("%s: %w", result.name, result.err))
			continue
		}
		if result.data != nil {
			merged.Transactions = append(merged.Transactions, result.data.Transactions...)
			merged.Operations = append(merged.Operations, result.data.Operations...)
			merged.Effects = append(merged.Effects, result.data.Effects...)
			merged.Trades = append(merged.Trades, result.data.Trades...)
			merged.Accounts = append(merged.Accounts, result.data.Accounts...)
			merged.Offers = append(merged.Offers, result.data.Offers...)
			merged.Trustlines = append(merged.Trustlines, result.data.Trustlines...)
			merged.AccountSigners = append(merged.AccountSigners, result.data.AccountSigners...)
			merged.ClaimableBalances = append(merged.ClaimableBalances, result.data.ClaimableBalances...)
			merged.LiquidityPools = append(merged.LiquidityPools, result.data.LiquidityPools...)
			merged.ConfigSettings = append(merged.ConfigSettings, result.data.ConfigSettings...)
			merged.TTLEntries = append(merged.TTLEntries, result.data.TTLEntries...)
			merged.EvictedKeys = append(merged.EvictedKeys, result.data.EvictedKeys...)
			merged.ContractEvents = append(merged.ContractEvents, result.data.ContractEvents...)
			merged.ContractData = append(merged.ContractData, result.data.ContractData...)
			merged.ContractCode = append(merged.ContractCode, result.data.ContractCode...)
			merged.NativeBalances = append(merged.NativeBalances, result.data.NativeBalances...)
			merged.RestoredKeys = append(merged.RestoredKeys, result.data.RestoredKeys...)
			merged.ContractCreations = append(merged.ContractCreations, result.data.ContractCreations...)
			merged.Ledgers = append(merged.Ledgers, result.data.Ledgers...)
			merged.TokenTransfers = append(merged.TokenTransfers, result.data.TokenTransfers...)
		}
	}

	if len(extractErrors) > 0 {
		for _, err := range extractErrors {
			log.Printf("[Worker %d] extractor error on ledger %d: %v", workerID, meta.LedgerSequence, err)
		}
		return nil, fmt.Errorf("%d extractor(s) failed for ledger %d", len(extractErrors), meta.LedgerSequence)
	}

	// Stamp era_id on all rows from config (avoids passing through every extractor)
	stampEraID(merged, meta.EraID)

	return merged, nil
}

func convertExtractedConfigSetting(r extract.ConfigSettingData) domain.ConfigSettingData {
	resolvedAt := int64(r.LedgerSequence)
	return domain.ConfigSettingData{
		ConfigSettingID:                 r.ConfigSettingID,
		LedgerSequence:                  r.LedgerSequence,
		LastModifiedLedger:              r.LastModifiedLedger,
		Deleted:                         r.Deleted,
		ClosedAt:                        r.ClosedAt,
		LedgerMaxInstructions:           r.LedgerMaxInstructions,
		TxMaxInstructions:               r.TxMaxInstructions,
		FeeRatePerInstructionsIncrement: r.FeeRatePerInstructionsIncrement,
		TxMemoryLimit:                   r.TxMemoryLimit,
		LedgerMaxReadLedgerEntries:      r.LedgerMaxReadLedgerEntries,
		LedgerMaxReadBytes:              r.LedgerMaxReadBytes,
		LedgerMaxWriteLedgerEntries:     r.LedgerMaxWriteLedgerEntries,
		LedgerMaxWriteBytes:             r.LedgerMaxWriteBytes,
		TxMaxReadLedgerEntries:          r.TxMaxReadLedgerEntries,
		TxMaxReadBytes:                  r.TxMaxReadBytes,
		TxMaxWriteLedgerEntries:         r.TxMaxWriteLedgerEntries,
		TxMaxWriteBytes:                 r.TxMaxWriteBytes,
		TxMaxFootprintEntries:           r.TxMaxFootprintEntries,
		ContractMaxSizeBytes:            r.ContractMaxSizeBytes,
		ConfigSettingXDR:                r.ConfigSettingXDR,
		CreatedAt:                       r.CreatedAt,
		LedgerRange:                     r.LedgerRange,
		EraID:                           r.EraID,
		ObservationSource:               "ledger_close_meta",
		ResolvedAtLedger:                &resolvedAt,
	}
}

func (w *Worker) shouldExtractTable(name string) bool {
	return shouldExtractTable(w.config.OnlyTables, name)
}

func shouldExtractTable(onlyTables map[string]bool, name string) bool {
	if len(onlyTables) == 0 {
		return true
	}
	sourceName := domain.ExtractorOutputDir(name)
	duckTableName := domain.MapToDuckLakeTable(sourceName)
	return domain.TableSetIncludes(onlyTables, name, sourceName, duckTableName)
}

// stampEraID sets the EraID field on every row in every table slice.
func stampEraID(data *domain.LedgerData, eraID *string) {
	if eraID == nil {
		return
	}
	for i := range data.Transactions {
		data.Transactions[i].EraID = eraID
	}
	for i := range data.Operations {
		data.Operations[i].EraID = eraID
	}
	for i := range data.Effects {
		data.Effects[i].EraID = eraID
	}
	for i := range data.Trades {
		data.Trades[i].EraID = eraID
	}
	for i := range data.Accounts {
		data.Accounts[i].EraID = eraID
	}
	for i := range data.Offers {
		data.Offers[i].EraID = eraID
	}
	for i := range data.Trustlines {
		data.Trustlines[i].EraID = eraID
	}
	for i := range data.AccountSigners {
		data.AccountSigners[i].EraID = eraID
	}
	for i := range data.ClaimableBalances {
		data.ClaimableBalances[i].EraID = eraID
	}
	for i := range data.LiquidityPools {
		data.LiquidityPools[i].EraID = eraID
	}
	for i := range data.ConfigSettings {
		data.ConfigSettings[i].EraID = eraID
	}
	for i := range data.TTLEntries {
		data.TTLEntries[i].EraID = eraID
	}
	for i := range data.EvictedKeys {
		data.EvictedKeys[i].EraID = eraID
	}
	for i := range data.ContractEvents {
		data.ContractEvents[i].EraID = eraID
	}
	for i := range data.ContractData {
		data.ContractData[i].EraID = eraID
	}
	for i := range data.ContractCode {
		data.ContractCode[i].EraID = eraID
	}
	for i := range data.NativeBalances {
		data.NativeBalances[i].EraID = eraID
	}
	for i := range data.RestoredKeys {
		data.RestoredKeys[i].EraID = eraID
	}
	for i := range data.ContractCreations {
		data.ContractCreations[i].EraID = eraID
	}
	for i := range data.Ledgers {
		data.Ledgers[i].EraID = eraID
	}
	for i := range data.TokenTransfers {
		data.TokenTransfers[i].EraID = eraID
	}
}

// ===========================================================================
// LOCAL EXTRACTORS
//
// Each extractor takes the pre-decoded LedgerCloseMeta and returns typed data.
// Most hot-path extraction now uses the shared stellar-extract library; local
// extractors remain for history-loader-specific tables or fields that are not
// yet surfaced by the shared library.
// ===========================================================================

// extractTransactions — see extractors_core.go
// extractOperations  — see extractors_core.go
// extractEffects     — see extractors_effects.go
// extractTrades      — see extractors_core.go

// extractAccounts         — see extractors_accounts.go
// extractTrustlines       — see extractors_accounts.go
// extractAccountSigners   — see extractors_accounts.go
// extractNativeBalances   — see extractors_accounts.go

// extractOffers            — see extractors_market.go
// extractClaimableBalances — see extractors_market.go
// extractLiquidityPools    — see extractors_market.go

// extractConfigSettings — see extractors_state.go
// extractTTL            — see extractors_state.go
// extractEvictedKeys    — see extractors_state.go

// extractContractEvents — see extractors_soroban.go
// extractContractData  — see extractors_soroban.go
// extractContractCode  — see extractors_soroban.go

// extractRestoredKeys     — see extractors_soroban.go
// extractContractCreations — see extractors_soroban.go
