package main

// contract_events.transaction_id is what makes dropping transaction_hash
// (~700 GB) and event_id (~800 GB) safe: both are recovered by joining an
// event back to its transaction on this column. If it is wrong or zero, that
// drop destroys data with no way back, so the join is tested here -- in the
// repository that writes the column -- and not only in stellar-extract, where
// the equivalent test is fixture-gated and skips in CI.
//
// Two separate failures are covered, because they fail in different places:
// the id disagreeing with its transaction (an extract-side derivation bug)
// and the id not reaching the Parquet row (a mapping-side omission). The
// second is the F5 failure mode: a column that exists for months receiving
// nothing, digesting perfectly all the while.

import (
	"path/filepath"
	"testing"
)

// eventsWithTransactions extracts every ledger of every fixture and returns
// the events and transactions of those ledgers that carry contract events.
func eventsWithTransactions(t *testing.T) (events []ContractEventData, txs []TransactionData, ledgers int) {
	t.Helper()

	for _, name := range fixtureNames(t) {
		meta, _ := loadFixtureMeta(t, name)
		path := materializeLedgers(t, filepath.Join("testdata", "fixtures", name), t.TempDir())
		source, err := NewXDRStreamSource(path)
		if err != nil {
			t.Fatalf("%s: open stream: %v", name, err)
		}

		for seq := meta.StartLedger; seq <= meta.EndLedger; seq++ {
			raw, err := source.getLedger(seq)
			if err != nil {
				t.Fatalf("%s: ledger %d: %v", name, seq, err)
			}
			lm, err := decodeLedgerMeta(seq, raw, nil)
			if err != nil {
				t.Fatalf("%s: decode ledger %d: %v", name, seq, err)
			}
			data, err := extractBronzeLedger(lm, meta.NetworkPassphrase, nil, 0)
			if err != nil {
				t.Fatalf("%s: extract ledger %d: %v", name, seq, err)
			}
			if len(data.ContractEvents) == 0 {
				continue
			}
			ledgers++
			events = append(events, data.ContractEvents...)
			txs = append(txs, data.Transactions...)
		}
	}
	return events, txs, ledgers
}

// The join the storage contract depends on. Every event must resolve to
// exactly the transaction its hash names: an event whose id pointed at a
// neighbouring transaction in the same ledger would still join, still look
// like a real transaction, and silently attribute the event to the wrong one.
func TestContractEventTransactionIDResolvesToItsTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("extracts every fixture; skipped under -short")
	}

	events, txs, ledgers := eventsWithTransactions(t)
	if len(events) == 0 {
		t.Fatal("no fixture produced contract events; this test is verifying nothing")
	}

	// hash -> id, taken from the parent rows. This is the direction a
	// consumer runs it after transaction_hash is gone: id -> hash.
	wantByHash := make(map[string]int64, len(txs))
	idToHash := make(map[int64]string, len(txs))
	for _, tx := range txs {
		wantByHash[tx.TransactionHash] = tx.TransactionID
		idToHash[tx.TransactionID] = tx.TransactionHash
	}

	var zero, unjoinable, wrong int
	for _, ev := range events {
		want, ok := wantByHash[ev.TransactionHash]
		if !ok {
			unjoinable++
			continue
		}
		switch {
		case ev.TransactionID == 0:
			zero++
		case ev.TransactionID != want:
			if wrong == 0 {
				t.Errorf("event on %s: TransactionID = %d (which is transaction %s), "+
					"its own transaction is %d; the two derivations have diverged",
					ev.TransactionHash, ev.TransactionID, idToHash[ev.TransactionID], want)
			}
			wrong++
		}
	}

	if zero > 0 {
		t.Errorf("%d of %d events have TransactionID = 0; the column is declared but not populated",
			zero, len(events))
	}
	if unjoinable > 0 {
		t.Errorf("%d of %d events have no transaction row to join to; dropping transaction_hash "+
			"would lose these events' transaction entirely", unjoinable, len(events))
	}
	if wrong > 0 {
		t.Errorf("%d of %d events point at the wrong transaction", wrong, len(events))
	}
	t.Logf("joined %d events to %d transactions across %d ledgers", len(events), len(txs), ledgers)
}

// The mapping seam. parquetContractEventFromData is the only path by which
// the id reaches cold storage, and a dropped assignment there produces a
// column of zeros that every schema guard and every digest accepts.
func TestParquetContractEventCarriesTransactionID(t *testing.T) {
	if testing.Short() {
		t.Skip("extracts every fixture; skipped under -short")
	}

	events, _, _ := eventsWithTransactions(t)
	if len(events) == 0 {
		t.Fatal("no fixture produced contract events; this test is verifying nothing")
	}

	var dropped int
	for _, ev := range events {
		row := parquetContractEventFromData(ev, "test")
		if row.TransactionID != ev.TransactionID {
			if dropped == 0 {
				t.Errorf("event on %s: Parquet row has TransactionID = %d, extracted row has %d",
					ev.TransactionHash, row.TransactionID, ev.TransactionID)
			}
			dropped++
		}
	}
	if dropped > 0 {
		t.Errorf("%d of %d events lost their TransactionID crossing into the Parquet row", dropped, len(events))
	}
}
