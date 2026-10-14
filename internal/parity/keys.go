package parity

// Logical keys for the Bronze tables.
//
// A logical key is the smallest set of columns that identifies one row
// within a ledger range. It is what makes a row addressable: without one a
// consumer cannot join, deduplicate, or replay a range idempotently, and a
// comparison between two stores can only match rows by sorted rank, which
// is a guess.
//
// Every key here is verified unique against the committed fixture corpus by
// TestLogicalKeysAreUnique, over roughly 300,000 rows spanning protocols 1
// through 29. A key that is not unique is recorded as absent rather than
// asserted, because a key that is wrong is worse than none: it invites
// exactly the deduplication that silently dropped 86% of contract_events.
//
// Field names are the adapter field names, which both ingesters share.
//
// Do not change a key without a contract version bump. These names are the
// identity downstream consumers build on, and the one time a key changed by
// accident — contract_events losing operation_index — it cost most of the
// table.
var LogicalKeys = map[string][]string{
	// Event-stream tables: identity is a position in the ledger.
	"ledgers":         {"Sequence"},
	"transactions":    {"LedgerSequence", "TransactionHash"},
	"operations":      {"LedgerSequence", "TransactionHash", "OperationIndex"},
	"effects":         {"LedgerSequence", "TransactionHash", "OperationIndex", "EffectIndex"},
	"trades":          {"LedgerSequence", "TransactionHash", "OperationIndex", "TradeIndex"},
	"contract_events": {"LedgerSequence", "TransactionHash", "OperationIndex", "EventIndex"},

	// Snapshot tables: identity is the entry, scoped to the ledger whose
	// close produced the snapshot. LedgerSequence is part of every one of
	// them because Bronze keeps the history, not just the latest state.
	"accounts":           {"AccountID", "LedgerSequence"},
	"native_balances":    {"AccountID", "LedgerSequence"},
	"account_signers":    {"AccountID", "Signer", "LedgerSequence"},
	"trustlines":         {"AccountID", "AssetCode", "AssetIssuer", "AssetType", "LedgerSequence"},
	"offers":             {"OfferID", "LedgerSequence"},
	"liquidity_pools":    {"LiquidityPoolID", "LedgerSequence"},
	"claimable_balances": {"BalanceID", "LedgerSequence"},
	"config_settings":    {"ConfigSettingID", "LedgerSequence"},
	"contract_data":      {"ContractId", "LedgerKeyHash", "LedgerSequence"},
	"contract_code":      {"ContractCodeHash", "LedgerSequence"},
	"ttl":                {"KeyHash", "LedgerSequence"},
	"evicted_keys":       {"KeyHash", "LedgerSequence"},
	"restored_keys":      {"KeyHash", "LedgerSequence"},

	// A contract is created once. The creating ledger is an attribute of
	// the row, not part of its identity.
	"contract_creations": {"ContractID"},
}

// KnownNonUniqueKeys are keys that are in use and are not unique, with the
// measured collision rate.
//
// A third state, distinct from "unique key" and "no key". The key is real —
// it is what the hot buffer's primary key and the parity comparison both
// use — and it is the best identity available, but it does not fully
// separate rows. Recording the rate means a regression shows up as the rate
// moving rather than as silence.
var KnownNonUniqueKeys = map[string]string{
	// The contract and diagnostic event streams index separately, so a
	// contract event and a diagnostic event from different contracts can
	// occupy the same operation and event index. Such rows also share an
	// event_id, so the library cannot distinguish them either: this key is
	// exactly the identity event_id claims, no more and no less.
	// Measured at 4.0% of rows across the fixture corpus — 4,165 of
	// 104,730. See Ticket 04 finding F13; the fix is upstream.
	"contract_events": "contract and diagnostic streams share an index space; ~4% of rows collide",
}

// KeylessTables are tables with no verified unique key, and why.
//
// Listed rather than omitted: "we have not decided" and "no key exists"
// are different states, and only one of them is a defect to fix.
var KeylessTables = map[string]string{
	// No event index. Two transfers of the same amount between the same
	// parties for the same asset in one operation are indistinguishable —
	// legitimate on-chain, and observed once in the fixture corpus even
	// with eight attributes in the key. The library would have to emit an
	// index within the operation. See Ticket 04 finding F16.
	"token_transfers": "no event index; eight-attribute key still collides",
}

// KeyFor returns a table's logical key, and whether it has one.
func KeyFor(table string) ([]string, bool) {
	k, ok := LogicalKeys[table]
	return k, ok
}

// RowKey renders a row's key as a single comparable string. Returns false
// when the table has no key, or the row is missing a key field — a missing
// key field is a harness fault, not a null value, and must not silently
// produce a key that collides with every other incomplete row.
func RowKey(table string, row Row) (string, bool) {
	cols, ok := LogicalKeys[table]
	if !ok {
		return "", false
	}
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		v, present := row[c]
		if !present {
			return "", false
		}
		parts = append(parts, c+"="+v)
	}
	return joinKey(parts), true
}

func joinKey(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "\x1f" // unit separator: cannot occur in a canonical value
		}
		out += p
	}
	return out
}
