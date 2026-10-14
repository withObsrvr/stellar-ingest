package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
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
	StreamRange(ctx context.Context, start, end uint32) <-chan prefetchedLedger
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

// getLedger reads the XDR bytes for a given ledger sequence from disk.
func (fs *FilesystemSource) getLedger(sequence uint32) ([]byte, error) {
	path := filepath.Join(fs.basePath, fmt.Sprintf("%d.xdr", sequence))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ledger %d: %w", sequence, err)
	}
	return data, nil
}

func (fs *FilesystemSource) StreamRange(ctx context.Context, start, end uint32) <-chan prefetchedLedger {
	return streamSequentialRange(ctx, start, end, fs.getLedger)
}

// Close is a no-op for the filesystem source.
func (fs *FilesystemSource) Close() error {
	return nil
}

// XDRStreamSource reads a framed XDR stream file (output of `nebu fetch`)
// and indexes all ledgers into memory for random access by sequence.
type XDRStreamSource struct {
	ledgers map[uint32][]byte
}

// NewXDRStreamSource reads a nebu-style framed XDR file and indexes all ledgers.
func NewXDRStreamSource(filePath string) (*XDRStreamSource, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open xdr stream: %w", err)
	}
	defer f.Close()

	source := &XDRStreamSource{ledgers: make(map[uint32][]byte)}

	for {
		var lcm xdr.LedgerCloseMeta
		_, err := xdr.Unmarshal(f, &lcm)
		if err != nil {
			// The XDR decoder wraps io.EOF rather than returning it bare, so a
			// clean end of stream must be detected with errors.Is. Comparing
			// with == made every complete file look like a decode failure.
			if errors.Is(err, io.EOF) {
				break // clean end of stream
			}
			return nil, fmt.Errorf("XDR stream decode error: %w", err)
		}

		seq := uint32(lcm.LedgerSequence())
		raw, err := lcm.MarshalBinary()
		if err != nil {
			log.Printf("WARN: failed to re-marshal ledger %d: %v", seq, err)
			continue
		}
		source.ledgers[seq] = raw
	}

	if len(source.ledgers) == 0 {
		return nil, fmt.Errorf("no ledgers found in %s", filePath)
	}

	log.Printf("XDRStreamSource: loaded %d ledgers from %s", len(source.ledgers), filePath)
	return source, nil
}

func (s *XDRStreamSource) getLedger(sequence uint32) ([]byte, error) {
	data, ok := s.ledgers[sequence]
	if !ok {
		return nil, fmt.Errorf("ledger %d not found in stream", sequence)
	}
	return data, nil
}

func (s *XDRStreamSource) StreamRange(ctx context.Context, start, end uint32) <-chan prefetchedLedger {
	return streamSequentialRange(ctx, start, end, s.getLedger)
}

func (s *XDRStreamSource) Close() error { return nil }

// NewLedgerSourceFromConfig creates the appropriate LedgerSource for the given config.
func NewLedgerSourceFromConfig(config OrchestratorConfig) (LedgerSource, error) {
	switch config.StorageType {
	case "FS":
		return NewFilesystemSource(config.Bucket)
	case "XDR":
		return NewXDRStreamSource(config.Bucket)
	case "RPC", "CAPTIVE_CORE":
		return NewSDKStreamSource(config)
	case "GCS":
		return NewSDKStreamSource(config)
	case "S3":
		return NewSDKStreamSource(config)
	default:
		return nil, fmt.Errorf("unknown storage type: %q", config.StorageType)
	}
}

// ---------------------------------------------------------------------------
// Batch: aggregated LedgerData across multiple ledgers
// ---------------------------------------------------------------------------

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

// mergeLedger appends all data from a single LedgerData into the batch.
func (b *BatchData) mergeLedger(ld *LedgerData) {
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

// ---------------------------------------------------------------------------
// Prefetched ledger item sent through the read-ahead channel
// ---------------------------------------------------------------------------

type prefetchedLedger struct {
	sequence uint32
	data     []byte
	err      error
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

const defaultPrefetchBuffer = 200

// shardWriter is the worker's Parquet boundary. Close finalizes file footers;
// a shard must not be checkpointed as complete until it succeeds. This stays
// private: it does not represent a DuckLake or PostgreSQL commit contract.
type shardWriter interface {
	WriteBatch(*BatchData) error
	Close() error
}

// Worker processes a shard of ledgers through the extraction pipeline.
type Worker struct {
	id         int
	shard      Shard
	config     OrchestratorConfig
	progress   *ProgressTracker
	writer     shardWriter
	source     LedgerSource       // shared across workers for XDR mode, per-worker for FS/GCS
	checkpoint *CheckpointManager // optional, for resume support
	lineage    *LineageWriter     // optional, for lineage tracking
}

// NewWorker creates a new Worker for the given shard.
func NewWorker(id int, shard Shard, config OrchestratorConfig, progress *ProgressTracker, writer shardWriter, source LedgerSource, checkpoint *CheckpointManager, lineage *LineageWriter) *Worker {
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
	batch := &BatchData{}
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
		if item.err != nil {
			return fmt.Errorf("[Worker %d] prefetch ledger %d: %w", w.id, item.sequence, item.err)
		}

		// Decode XDR ONCE per ledger
		meta, err := w.decodeLedger(item.sequence, item.data)
		if err != nil {
			return fmt.Errorf("[Worker %d] decode ledger %d: %w", w.id, item.sequence, err)
		}

		// Extract all 21 data types in parallel
		ledgerData, err := w.extractLedger(meta)
		if err != nil {
			return fmt.Errorf("[Worker %d] extract ledger %d: %w", w.id, item.sequence, err)
		}

		// Accumulate into batch
		if batchCount == 0 {
			batchStartSeq = item.sequence
			batchStartTime = time.Now()
		}
		batch.mergeLedger(ledgerData)
		batchCount++
		lastSeq = item.sequence

		// Flush when batch is full
		if batchCount >= w.config.BatchSize {
			if err := w.writer.WriteBatch(batch); err != nil {
				return fmt.Errorf("[Worker %d] write batch: %w", w.id, err)
			}
			w.progress.ReportProgress(w.id, uint64(batchCount))
			w.recordLineage(batch, batchStartSeq, lastSeq, time.Since(batchStartTime))
			batch = &BatchData{}
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
func (w *Worker) recordLineage(batch *BatchData, startSeq, endSeq uint32, duration time.Duration) {
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

// streamSequentialRange adapts random-access local inputs to the same bounded
// stream contract used by the SDK acquisition sources.
func streamSequentialRange(ctx context.Context, start, end uint32, read func(uint32) ([]byte, error)) <-chan prefetchedLedger {
	bufferSize := defaultPrefetchBuffer
	ch := make(chan prefetchedLedger, bufferSize)

	go func() {
		defer close(ch)

		for seq := start; seq <= end; seq++ {
			// Check for cancellation before each fetch
			select {
			case <-ctx.Done():
				ch <- prefetchedLedger{sequence: seq, err: ctx.Err()}
				return
			default:
			}

			data, err := read(seq)

			select {
			case ch <- prefetchedLedger{sequence: seq, data: data, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

// decodeLedger unmarshals raw XDR bytes into a LedgerCloseMeta and extracts
// header metadata. This is done ONCE per ledger (not per extractor).
func (w *Worker) decodeLedger(sequence uint32, rawXDR []byte) (LedgerMeta, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawXDR); err != nil {
		return LedgerMeta{}, fmt.Errorf("unmarshal XDR: %w", err)
	}

	// Extract header metadata
	ledgerSeq := uint32(lcm.LedgerSequence())
	closedAt := time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	// Sanity check
	if ledgerSeq != sequence {
		return LedgerMeta{}, fmt.Errorf("sequence mismatch: expected %d, got %d", sequence, ledgerSeq)
	}

	var eraID *string
	if w.config.EraID != "" {
		eraID = &w.config.EraID
	}

	return LedgerMeta{
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
	data *LedgerData
	err  error
}

// extractLedger preserves the worker call site while keeping the mapping
// independent of scheduling, sources, sinks, and checkpoint state.
func (w *Worker) extractLedger(meta LedgerMeta) (*LedgerData, error) {
	return extractBronzeLedger(meta, w.config.NetworkPassphrase, w.config.OnlyTables, w.id)
}

// extractBronzeLedger fans out to all 21 extractors over an already-decoded
// ledger. It is not exported yet: the live consumer's contract is unresolved.
func extractBronzeLedger(meta LedgerMeta, np string, onlyTables map[string]bool, workerID int) (*LedgerData, error) {
	lcm := meta.LCM
	lr := meta.LedgerRange

	resultCh := make(chan extractorResult, len(extractorOutputDirByName))
	var wg sync.WaitGroup
	launched := 0

	// Helper to launch an extractor goroutine
	launch := func(name string, fn func() (*LedgerData, error)) {
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

	// Fan out to all extractors. Transactions use the local extractor because it
	// carries Horizon XDR/signature fields not yet exposed by stellar-extract.
	launch("transactions", func() (*LedgerData, error) {
		rows, err := extractTransactions(meta.LCM, np, meta.LedgerSequence, meta.ClosedAt, meta.LedgerRange)
		if err != nil {
			return nil, err
		}
		for i := range rows {
			rows[i].EraID = meta.EraID
			sanitizeTransactionMemo(&rows[i])
		}
		return &LedgerData{Transactions: rows}, nil
	})
	launch("operations", func() (*LedgerData, error) {
		rows, err := extractOperations(meta.LCM, np, meta.LedgerSequence, meta.ClosedAt, meta.LedgerRange)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Operations: rows}, nil
	})
	launch("effects", func() (*LedgerData, error) {
		libRows, err := extract.ExtractEffects(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]EffectData, len(libRows))
		for i, r := range libRows {
			rows[i] = EffectData(r)
		}
		return &LedgerData{Effects: rows}, nil
	})
	launch("trades", func() (*LedgerData, error) {
		libRows, err := extract.ExtractTrades(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]TradeData, len(libRows))
		for i, r := range libRows {
			rows[i] = TradeData(r)
		}
		return &LedgerData{Trades: rows}, nil
	})
	launch("accounts", func() (*LedgerData, error) {
		rows, err := extractAccounts(meta.LCM, np, meta.LedgerSequence, meta.ClosedAt, meta.LedgerRange)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Accounts: rows}, nil
	})
	launch("offers", func() (*LedgerData, error) {
		libRows, err := extract.ExtractOffers(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]OfferData, len(libRows))
		for i, r := range libRows {
			rows[i] = OfferData(r)
		}
		return &LedgerData{Offers: rows}, nil
	})
	launch("trustlines", func() (*LedgerData, error) {
		libRows, err := extract.ExtractTrustlines(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]TrustlineData, len(libRows))
		for i, r := range libRows {
			rows[i] = TrustlineData(r)
		}
		return &LedgerData{Trustlines: rows}, nil
	})
	launch("account_signers", func() (*LedgerData, error) {
		libRows, err := extract.ExtractAccountSigners(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]AccountSignerData, len(libRows))
		for i, r := range libRows {
			rows[i] = AccountSignerData(r)
		}
		return &LedgerData{AccountSigners: rows}, nil
	})
	launch("claimable_balances", func() (*LedgerData, error) {
		libRows, err := extract.ExtractClaimableBalances(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]ClaimableBalanceData, len(libRows))
		for i, r := range libRows {
			rows[i] = ClaimableBalanceData(r)
		}
		return &LedgerData{ClaimableBalances: rows}, nil
	})
	launch("liquidity_pools", func() (*LedgerData, error) {
		libRows, err := extract.ExtractLiquidityPools(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]LiquidityPoolData, len(libRows))
		for i, r := range libRows {
			rows[i] = LiquidityPoolData(r)
		}
		return &LedgerData{LiquidityPools: rows}, nil
	})
	launch("config_settings", func() (*LedgerData, error) {
		libRows, err := extract.ExtractConfigSettings(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]ConfigSettingData, len(libRows))
		for i, r := range libRows {
			rows[i] = convertExtractedConfigSetting(r)
		}
		return &LedgerData{ConfigSettings: rows}, nil
	})
	launch("ttl", func() (*LedgerData, error) {
		libRows, err := extract.ExtractTTL(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]TTLData, len(libRows))
		for i, r := range libRows {
			rows[i] = TTLData(r)
		}
		return &LedgerData{TTLEntries: rows}, nil
	})
	launch("evicted_keys", func() (*LedgerData, error) {
		libRows, err := extract.ExtractEvictedKeys(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]EvictedKeyData, len(libRows))
		for i, r := range libRows {
			rows[i] = EvictedKeyData(r)
		}
		return &LedgerData{EvictedKeys: rows}, nil
	})
	launch("contract_events", func() (*LedgerData, error) {
		libRows, err := extract.ExtractContractEvents(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]ContractEventData, len(libRows))
		for i, r := range libRows {
			rows[i] = ContractEventData(r)
			if repairs := sanitizeContractEventUTF8(&rows[i]); repairs > 0 {
				log.Printf("[Worker %d] contract_events utf8_repaired ledger=%d event_id=%s repairs=%d", workerID, meta.LedgerSequence, rows[i].EventID, repairs)
			}
		}
		return &LedgerData{ContractEvents: rows}, nil
	})
	launch("contract_data", func() (*LedgerData, error) {
		libRows, err := extract.ExtractContractData(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]ContractDataData, len(libRows))
		for i, r := range libRows {
			rows[i] = ContractDataData(r)
		}
		return &LedgerData{ContractData: rows}, nil
	})
	launch("contract_code", func() (*LedgerData, error) {
		libRows, err := extract.ExtractContractCode(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]ContractCodeData, len(libRows))
		for i, r := range libRows {
			rows[i] = ContractCodeData(r)
		}
		return &LedgerData{ContractCode: rows}, nil
	})
	launch("native_balances", func() (*LedgerData, error) {
		libRows, err := extract.ExtractNativeBalances(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]NativeBalanceData, len(libRows))
		for i, r := range libRows {
			rows[i] = NativeBalanceData(r)
		}
		return &LedgerData{NativeBalances: rows}, nil
	})
	launch("restored_keys", func() (*LedgerData, error) {
		// The pinned library v0.1.2 scans operations and misses automatic
		// restorations. Keep this mapping local until a full library upgrade
		// can be reconciled against the other 20 tables' goldens.
		rows, err := extractRestoredKeys(meta.LCM, np, meta.LedgerSequence, meta.ClosedAt, lr)
		if err != nil {
			return nil, err
		}
		return &LedgerData{RestoredKeys: rows}, nil
	})
	launch("contract_creations", func() (*LedgerData, error) {
		libRows, err := extract.ExtractContractCreations(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]ContractCreationData, len(libRows))
		for i, r := range libRows {
			rows[i] = ContractCreationData(r)
		}
		return &LedgerData{ContractCreations: rows}, nil
	})
	launch("ledgers", func() (*LedgerData, error) {
		rows, err := extractLedgers(meta.LCM, np, meta.LedgerSequence, meta.ClosedAt, meta.LedgerRange)
		if err != nil {
			return nil, err
		}
		return &LedgerData{Ledgers: rows}, nil
	})
	launch("token_transfers", func() (*LedgerData, error) {
		libRows, err := extract.ExtractTokenTransfers(libInput)
		if err != nil {
			return nil, err
		}
		rows := make([]TokenTransferData, len(libRows))
		for i, r := range libRows {
			rows[i] = TokenTransferData(r)
		}
		return &LedgerData{TokenTransfers: rows}, nil
	})

	if launched == 0 {
		return nil, fmt.Errorf("no extractors selected by --only-tables=%q", tableSetKey(onlyTables))
	}

	// Wait for all extractors to complete, then close the results channel
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect and merge results
	merged := &LedgerData{Meta: meta}
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

func convertExtractedConfigSetting(r extract.ConfigSettingData) ConfigSettingData {
	resolvedAt := int64(r.LedgerSequence)
	return ConfigSettingData{
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
	sourceName := extractorOutputDir(name)
	duckTableName := mapToDuckLakeTable(sourceName)
	return tableSetIncludes(onlyTables, name, sourceName, duckTableName)
}

// stampEraID sets the EraID field on every row in every table slice.
func stampEraID(data *LedgerData, eraID *string) {
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

// ParquetWriter is an alias for ParquetWriterFull (see parquet_writer.go)
type ParquetWriter = ParquetWriterFull

// NewParquetWriter creates a new Parquet writer for a worker.
func NewParquetWriter(outputDir string, workerID int, pipelineVersion string) (*ParquetWriter, error) {
	return NewParquetWriterFull(outputDir, workerID, pipelineVersion), nil
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

func sanitizeTransactionMemo(tx *TransactionData) {
	if tx == nil || tx.MemoType == nil || tx.Memo == nil || *tx.MemoType != "text" {
		return
	}
	memoType := "text_base64"
	memo := base64.StdEncoding.EncodeToString([]byte(*tx.Memo))
	tx.MemoType = &memoType
	tx.Memo = &memo
}
