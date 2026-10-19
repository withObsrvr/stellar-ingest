package adapters

// Guard the handoff from Parquet file to DuckLake table.
//
// The loader writes Parquet, then ducklake.go inserts it by naming the
// columns it finds in the file. That makes the two schemas a contract:
//
//   - a Parquet column the Bronze table lacks fails the insert outright
//   - a Bronze column the Parquet file lacks is silently stored NULL, which
//     is indistinguishable downstream from the ledger not carrying the value
//
// The second is the one worth a test. Six columns reached this repository
// with stellar-extract v0.3.2 — liquidity_pool_id, executable_type,
// external_ref_owner, external_ref_tag, event_stream and event_index — and
// every one of them had to be added to a Parquet struct by hand after the
// library bump compiled cleanly without them.

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// parquetRowTypes maps each Bronze table to the struct written to its files.
var parquetRowTypes = map[string]any{
	"ledgers_row_v2":                 ParquetLedger{},
	"transactions_row_v2":            ParquetTransaction{},
	"operations_row_v2":              ParquetOperation{},
	"effects_row_v1":                 ParquetEffect{},
	"trades_row_v1":                  ParquetTrade{},
	"accounts_snapshot_v1":           ParquetAccount{},
	"offers_snapshot_v1":             ParquetOffer{},
	"trustlines_snapshot_v1":         ParquetTrustline{},
	"account_signers_snapshot_v1":    ParquetAccountSigner{},
	"claimable_balances_snapshot_v1": ParquetClaimableBalance{},
	"liquidity_pools_snapshot_v1":    ParquetLiquidityPool{},
	"config_settings_snapshot_v1":    ParquetConfigSetting{},
	"ttl_snapshot_v1":                ParquetTTL{},
	"evicted_keys_state_v1":          ParquetEvictedKey{},
	"contract_events_stream_v1":      ParquetContractEvent{},
	"contract_data_snapshot_v1":      ParquetContractData{},
	"contract_code_snapshot_v1":      ParquetContractCode{},
	"native_balances_snapshot_v1":    ParquetNativeBalance{},
	"restored_keys_state_v1":         ParquetRestoredKey{},
	"contract_creations_v1":          ParquetContractCreation{},
	"token_transfers_stream_v1":      ParquetTokenTransfer{},
}

// noProducer marks a Bronze column that v3_bronze_schema.sql declares and
// that nothing populates — not this loader, not stellar-extract, and not
// stellar-postgres-ingester, whose writer.go never names it either.
//
// The schema is described at its head as "matching PostgreSQL exactly", and
// the hot schema accumulated these columns from migrations that added them
// ahead of any extractor. They read NULL everywhere, from both producers, so
// they are a schema question rather than an extraction one: either an
// extractor should populate them or the column should be dropped. Listing
// them here keeps that decision visible instead of letting a permanently
// empty column look like missing data.
const noProducer = "declared in the schema; no producer populates it, hot or cold"

// libraryGap marks a column the live ingester populates from its own code
// and that stellar-extract does not expose, so this loader cannot produce it
// even now that it delegates. These are the genuine one-shape violations:
// the same Bronze column holds a value in hot and NULL in cold.
const libraryGap = "live ingester populates it; stellar-extract does not expose it"

// claimableBalanceIngestedAt is the asymmetry the Parquet coverage audit also
// records: ClaimableBalanceData carries no ingestion wall clock while the
// other snapshot types do.
const claimableBalanceIngestedAt = "no ingestion wall clock on this row type; see the Parquet coverage audit"

// knownParquetSchemaGaps records columns the Bronze schema declares that the
// Parquet layer does not write, and why. Shrinking it is the goal: an
// unlisted gap fails the test, and so does an entry that is no longer true.
var knownParquetSchemaGaps = map[string]string{
	"claimable_balances_snapshot_v1.ingested_at": claimableBalanceIngestedAt,

	"operations_row_v2.claimants_count": libraryGap,

	"operations_row_v2.authorize":                         noProducer,
	"operations_row_v2.authorize_to_maintain_liabilities": noProducer,
	"operations_row_v2.operation_trace_code":              noProducer,
	"operations_row_v2.trust_line_flags":                  noProducer,
	"operations_row_v2.trustor":                           noProducer,

	"transactions_row_v2.max_fee_bid":           noProducer,
	"transactions_row_v2.soroban_fee_base":      noProducer,
	"transactions_row_v2.soroban_fee_charged":   noProducer,
	"transactions_row_v2.soroban_fee_refund":    noProducer,
	"transactions_row_v2.soroban_fee_resources": noProducer,
	"transactions_row_v2.soroban_fee_wasted":    noProducer,
}

func TestParquetColumnsMatchBronzeSchema(t *testing.T) {
	schema := bronzeSchemaColumns(t)
	stillMissing := map[string]bool{}
	for table := range schema {
		if _, ok := parquetRowTypes[table]; !ok {
			t.Errorf("%s exists in v3_bronze_schema.sql but has no Parquet row type", table)
		}
	}

	for table, row := range parquetRowTypes {
		want, ok := schema[table]
		if !ok {
			t.Errorf("%s has a Parquet row type but v3_bronze_schema.sql does not define it", table)
			continue
		}
		got := parquetStructColumns(row)

		for _, c := range sortedColumnKeys(want) {
			if got[c] {
				continue
			}
			if why, known := knownParquetSchemaGaps[table+"."+c]; known {
				t.Logf("%s.%s: not written (%s)", table, c, why)
				stillMissing[table+"."+c] = true
				continue
			}
			t.Errorf("%s.%s is a Bronze column but no Parquet field writes it, "+
				"so it is stored NULL for every row this loader produces",
				table, c)
		}
		for _, c := range sortedColumnKeys(got) {
			if !want[c] {
				t.Errorf("%s.%s is written to Parquet but v3_bronze_schema.sql has no such column; "+
					"the DuckLake insert will fail", table, c)
			}
		}
	}

	// A baseline that outlives the gap it records is worse than no baseline:
	// it reports the loader as lossier than it is and hides the next real
	// gap behind an entry nobody rereads.
	for key := range knownParquetSchemaGaps {
		if !stillMissing[key] {
			t.Errorf("knownParquetSchemaGaps lists %s but the Parquet layer now writes it; remove the entry", key)
		}
	}
}

func parquetStructColumns(row any) map[string]bool {
	out := map[string]bool{}
	rt := reflect.TypeOf(row)
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("parquet")
		if tag == "" || tag == "-" {
			continue
		}
		out[strings.Split(tag, ",")[0]] = true
	}
	return out
}

func bronzeSchemaColumns(t *testing.T) map[string]map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(".", "v3_bronze_schema.sql"))
	if err != nil {
		t.Fatalf("read v3_bronze_schema.sql: %v", err)
	}
	table := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS bronze_store\.(\w+)\s*\((.*?)\n\);`)
	column := regexp.MustCompile(`^"?([a-z_0-9]+)"?\s+[A-Z]`)

	out := map[string]map[string]bool{}
	for _, m := range table.FindAllStringSubmatch(string(raw), -1) {
		cols := map[string]bool{}
		for _, line := range strings.Split(m[2], "\n") {
			if c := column.FindStringSubmatch(strings.TrimSpace(line)); c != nil {
				cols[c[1]] = true
			}
		}
		out[m[1]] = cols
	}
	if len(out) < 20 {
		t.Fatalf("parsed %d tables from v3_bronze_schema.sql; the parser is broken", len(out))
	}
	return out
}

func sortedColumnKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
