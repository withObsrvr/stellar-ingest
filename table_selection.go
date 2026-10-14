package main

import (
	"fmt"
	"sort"
	"strings"
)

var extractorOutputDirByName = map[string]string{
	"transactions":       "transactions",
	"operations":         "operations",
	"effects":            "effects",
	"trades":             "trades",
	"accounts":           "accounts_snapshot",
	"offers":             "offers_snapshot",
	"trustlines":         "trustlines_snapshot",
	"account_signers":    "account_signers_snapshot",
	"claimable_balances": "claimable_balances_snapshot",
	"liquidity_pools":    "liquidity_pools_snapshot",
	"config_settings":    "config_settings",
	"ttl":                "ttl_snapshot",
	"evicted_keys":       "evicted_keys",
	"contract_events":    "contract_events",
	"contract_data":      "contract_data_snapshot",
	"contract_code":      "contract_code_snapshot",
	"native_balances":    "native_balances",
	"restored_keys":      "restored_keys",
	"contract_creations": "contract_creations",
	"ledgers":            "ledgers",
	"token_transfers":    "token_transfers",
}

var duckLakeTableBySource = map[string]string{
	"ledgers":                     "ledgers_row_v2",
	"transactions":                "transactions_row_v2",
	"operations":                  "operations_row_v2",
	"effects":                     "effects_row_v1",
	"trades":                      "trades_row_v1",
	"accounts_snapshot":           "accounts_snapshot_v1",
	"offers_snapshot":             "offers_snapshot_v1",
	"trustlines_snapshot":         "trustlines_snapshot_v1",
	"account_signers_snapshot":    "account_signers_snapshot_v1",
	"claimable_balances_snapshot": "claimable_balances_snapshot_v1",
	"liquidity_pools_snapshot":    "liquidity_pools_snapshot_v1",
	"config_settings":             "config_settings_snapshot_v1",
	"ttl_snapshot":                "ttl_snapshot_v1",
	"evicted_keys":                "evicted_keys_state_v1",
	"contract_events":             "contract_events_stream_v1",
	"contract_data_snapshot":      "contract_data_snapshot_v1",
	"contract_code_snapshot":      "contract_code_snapshot_v1",
	"native_balances":             "native_balances_snapshot_v1",
	"restored_keys":               "restored_keys_state_v1",
	"contract_creations":          "contract_creations_v1",
	"token_transfers":             "token_transfers_stream_v1",
}

func extractorOutputDir(name string) string {
	if outputDir, ok := extractorOutputDirByName[name]; ok {
		return outputDir
	}
	return name
}

func tableSetIncludes(set map[string]bool, names ...string) bool {
	if len(set) == 0 {
		return true
	}
	for _, name := range names {
		if name != "" && set[name] {
			return true
		}
	}
	return false
}

func validateTableSelection(set map[string]bool, flagName string) error {
	if len(set) == 0 {
		return nil
	}
	known := map[string]bool{}
	for extractorName, outputDir := range extractorOutputDirByName {
		known[extractorName] = true
		known[outputDir] = true
		if duckTable := mapToDuckLakeTable(outputDir); duckTable != "" {
			known[duckTable] = true
		}
	}
	for sourceName, duckTable := range duckLakeTableBySource {
		known[sourceName] = true
		known[duckTable] = true
	}

	var unknown []string
	for name := range set {
		if !known[name] {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("--%s contains unknown table name(s): %s", flagName, strings.Join(unknown, ","))
}

func tableSetKey(set map[string]bool) string {
	if len(set) == 0 {
		return ""
	}
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}
