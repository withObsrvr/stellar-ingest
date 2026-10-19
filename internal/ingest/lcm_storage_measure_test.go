package ingest

// Measure storage contract rule 5 before implementing it.
//
// The rule says Bronze should store the LedgerCloseMeta once per ledger and
// derive tx_envelope / tx_result / tx_meta / tx_fee_meta / tx_signers from it.
// The expectation is that one blob per ledger beats five columns per
// transaction, because it removes per-row overhead and gives the compressor
// more context.
//
// That expectation is not obviously true. Unlike event_id, these columns are
// not derived data being stored twice -- they are the raw protocol bytes, and
// an LCM is the same bytes rearranged, plus parts Bronze does not store today
// (upgrades, the full tx set) and a ledger_header it already stores
// separately. The rearrangement could cost more than it saves.
//
// So this writes both shapes from identical ledgers, as real Parquet with the
// same compression the lake uses, and compares. Run:
//
//	go test -run TestLCMVersusPerTransactionColumns -v .
//
// It is skipped by default because it is a measurement, not an assertion:
// there is no correct answer to regress against, and the number it produces
// is an input to a decision rather than a property of the code.

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// lcmRow is rule 5's shape: the whole ledger, once.
type lcmRow struct {
	LedgerSequence uint32 `parquet:"ledger_sequence"`
	LedgerRange    uint32 `parquet:"ledger_range"`
	LCM            string `parquet:"lcm_xdr"`
}

// txXDRRow is today's shape: the five columns, per transaction. Only the
// columns under question -- the rest of transactions_row_v2 is identical
// either way and would just add noise to both sides.
type txXDRRow struct {
	LedgerSequence uint32  `parquet:"ledger_sequence"`
	TransactionID  int64   `parquet:"transaction_id"`
	TxEnvelope     *string `parquet:"tx_envelope,optional"`
	TxResult       *string `parquet:"tx_result,optional"`
	TxMeta         *string `parquet:"tx_meta,optional"`
	TxFeeMeta      *string `parquet:"tx_fee_meta,optional"`
	TxSigners      *string `parquet:"tx_signers,optional"`
}

func writeParquetRows[T any](t *testing.T, path string, rows []T) int64 {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	w := parquet.NewGenericWriter[T](f, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func TestLCMVersusPerTransactionColumns(t *testing.T) {
	if os.Getenv("MEASURE_LCM") == "" {
		t.Skip("measurement, not an assertion; set MEASURE_LCM=1 to run")
	}

	dir := t.TempDir()
	var (
		totalLCM, totalCols int64
		totalLedgers        int
		totalTxs            int
	)

	fmt.Printf("\n%-28s %8s %7s %12s %12s %8s\n",
		"fixture", "ledgers", "txs", "lcm-once", "per-tx-cols", "ratio")

	for _, name := range fixtureNames(t) {
		fixtureDir := filepath.Join("testdata", "fixtures", name)
		xdrPath := materializeLedgers(t, fixtureDir, dir)

		src, err := NewXDRStreamSource(xdrPath)
		if err != nil {
			t.Fatalf("%s: open stream: %v", name, err)
		}

		var lcmRows []lcmRow
		var txRows []txXDRRow

		// Deterministic order so the two shapes see identical ledgers and
		// the Parquet files are comparable run to run.
		seqs := make([]uint32, 0, len(src.ledgers))
		for seq := range src.ledgers {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

		for _, seq := range seqs {
			raw := src.ledgers[seq]
			var lcm xdr.LedgerCloseMeta
			if err := lcm.UnmarshalBinary(raw); err != nil {
				t.Fatalf("%s: decode lcm: %v", name, err)
			}

			// Shape A: the ledger, once, as stored bytes rendered base64 --
			// the same encoding row contract §6 requires of the columns it
			// replaces, so neither side gets an encoding advantage.
			lcmRows = append(lcmRows, lcmRow{
				LedgerSequence: seq,
				LedgerRange:    (seq / 10000) * 10000,
				LCM:            base64.StdEncoding.EncodeToString(raw),
			})

			// Shape B: the five columns, per transaction -- produced by the
			// production extractor, not a reimplementation, so the bytes
			// being compared are the bytes Bronze actually stores.
			data, err := extractBronzeLedger(LedgerMeta{
				LCM:            lcm,
				LedgerSequence: seq,
				ClosedAt:       time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC(),
				LedgerRange:    (seq / 10000) * 10000,
			}, "Public Global Stellar Network ; September 2015", ParseCSVSet("transactions"), 0)
			if err != nil {
				t.Fatalf("%s: extract ledger %d: %v", name, seq, err)
			}
			for _, tx := range data.Transactions {
				txRows = append(txRows, txXDRRow{
					LedgerSequence: tx.LedgerSequence,
					TransactionID:  tx.TransactionID,
					TxEnvelope:     tx.TxEnvelope,
					TxResult:       tx.TxResult,
					TxMeta:         tx.TxMeta,
					TxFeeMeta:      tx.TxFeeMeta,
					TxSigners:      tx.TxSigners,
				})
			}
		}
		src.Close()

		if len(lcmRows) == 0 {
			continue
		}

		a := writeParquetRows(t, filepath.Join(dir, name+"-lcm.parquet"), lcmRows)
		b := writeParquetRows(t, filepath.Join(dir, name+"-cols.parquet"), txRows)

		ratio := float64(a) / float64(b)
		fmt.Printf("%-28s %8d %7d %12d %12d %7.2fx\n",
			name, len(lcmRows), len(txRows), a, b, ratio)

		totalLCM += a
		totalCols += b
		totalLedgers += len(lcmRows)
		totalTxs += len(txRows)
	}

	if totalCols == 0 {
		t.Fatal("measured nothing; the fixture corpus produced no transactions")
	}

	ratio := float64(totalLCM) / float64(totalCols)
	fmt.Printf("\n%-28s %8d %7d %12d %12d %7.2fx\n",
		"TOTAL", totalLedgers, totalTxs, totalLCM, totalCols, ratio)

	switch {
	case ratio < 0.9:
		fmt.Printf("\nRule 5 holds: storing the LCM once is %.0f%% smaller.\n", (1-ratio)*100)
	case ratio > 1.1:
		fmt.Printf("\nRule 5 REVERSES: storing the LCM once is %.0f%% LARGER.\n"+
			"The five columns are the cheaper shape; keep row contract §6 as ratified.\n",
			(ratio-1)*100)
	default:
		fmt.Printf("\nInconclusive at %.2fx. Rule 5 is not worth its cost:\n"+
			"it is the one rule needing consumer changes that ship with the re-ingest.\n", ratio)
	}
}
