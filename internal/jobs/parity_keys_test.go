package jobs

// The logical keys in internal/parity/keys.go are a contract, so they are
// verified against real ledgers rather than asserted.
//
// A key that is wrong is worse than no key. contract_events had one that
// omitted operation_index, and the deduplication built on it discarded 86%
// of the table — a defect no schema review and no cross-ingester comparison
// caught, because both sides extracted the same rows and the loss happened
// on the way to storage.

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/fixtures"

	"github.com/withObsrvr/stellar-ingest/internal/parity"
)

// TestLogicalKeysAreUnique extracts every fixture and checks that each
// table's key identifies exactly one row.
func TestLogicalKeysAreUnique(t *testing.T) {
	if testing.Short() {
		t.Skip("extracts every fixture; skipped under -short")
	}

	type stat struct{ rows, distinct int }
	totals := map[string]*stat{}

	for _, name := range fixtures.Names(t) {
		meta, _ := fixtures.LoadMeta(t, name)
		ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), t.TempDir())

		_, rows, err := BuildHistoricalParityRecord(ParityInput{
			Fixture:           name,
			NetworkPassphrase: meta.NetworkPassphrase,
			StartLedger:       meta.StartLedger,
			EndLedger:         meta.EndLedger,
		}, ledgerPath, nil)
		if err != nil {
			t.Fatalf("extract %s: %v", name, err)
		}

		for table, tableRows := range rows.Tables {
			if len(tableRows) == 0 {
				continue
			}
			if _, keyless := parity.KeylessTables[table]; keyless {
				continue
			}
			if _, ok := parity.KeyFor(table); !ok {
				t.Errorf("%s has %d rows but no logical key and no entry in KeylessTables",
					table, len(tableRows))
				continue
			}

			seen := map[string]bool{}
			for _, row := range tableRows {
				k, ok := parity.RowKey(table, row)
				if !ok {
					t.Fatalf("%s/%s: row is missing a key field; the key names a field the row does not have",
						name, table)
				}
				seen[k] = true
			}
			if totals[table] == nil {
				totals[table] = &stat{}
			}
			totals[table].rows += len(tableRows)
			totals[table].distinct += len(seen)
		}
	}

	tables := make([]string, 0, len(totals))
	for tbl := range totals {
		tables = append(tables, tbl)
	}
	sort.Strings(tables)

	for _, tbl := range tables {
		s := totals[tbl]
		if note, known := parity.KnownNonUniqueKeys[tbl]; known {
			rate := 100 * float64(s.rows-s.distinct) / float64(s.rows)
			t.Logf("%-20s %6d rows, %d distinct (%.1f%% collide) — %s",
				tbl, s.rows, s.distinct, rate, note)
			continue
		}
		if s.rows != s.distinct {
			t.Errorf("%s: %d rows collapse to %d distinct keys — %d rows share an identity with another, "+
				"so any sink that deduplicates on this key loses them",
				tbl, s.rows, s.distinct, s.rows-s.distinct)
			continue
		}
		t.Logf("%-20s %6d rows, key unique", tbl, s.rows)
	}
}

// TestEveryTableHasAKeyDecision keeps the registry honest: a table must
// either have a key or an explicit reason it has none.
func TestEveryTableHasAKeyDecision(t *testing.T) {
	for _, table := range parity.AllTables {
		_, keyed := parity.KeyFor(table)
		_, keyless := parity.KeylessTables[table]
		_, nonUnique := parity.KnownNonUniqueKeys[table]
		switch {
		case keyed && keyless:
			t.Errorf("%s is in both LogicalKeys and KeylessTables", table)
		case !keyed && !keyless:
			t.Errorf("%s has neither a logical key nor a recorded reason for lacking one", table)
		case nonUnique && !keyed:
			t.Errorf("%s is recorded as having a non-unique key but has no key", table)
		}
	}
}

// TestKeyFieldsExistOnRows catches a key naming a field no row carries,
// which would otherwise surface only as a confusing uniqueness failure.
func TestKeyFieldsExistOnRows(t *testing.T) {
	name := "pubnet-tiny-dump"
	meta, _ := fixtures.LoadMeta(t, name)
	ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), t.TempDir())

	_, rows, err := BuildHistoricalParityRecord(ParityInput{
		Fixture:           name,
		NetworkPassphrase: meta.NetworkPassphrase,
		StartLedger:       meta.StartLedger,
		EndLedger:         meta.EndLedger,
	}, ledgerPath, nil)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}

	for table, tableRows := range rows.Tables {
		if len(tableRows) == 0 {
			continue
		}
		cols, ok := parity.KeyFor(table)
		if !ok {
			continue
		}
		for _, c := range cols {
			if _, present := tableRows[0][c]; !present {
				t.Errorf("%s: logical key names %q, which is not a field on the row", table, c)
			}
		}
	}
}
