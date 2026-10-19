package ingest

// Ticket 04 parity harness, historical side.
//
// This drives the production Bronze mapping — extractBronzeLedger, the same
// entry point the worker uses — over a committed fixture and snapshots all 21
// tables into the canonical form in internal/parity. The live ingester runs
// the mirror of this in its own repository, because the two pin different
// versions of stellar-extract (v0.1.2 here, v0.1.5 there) and cannot be
// linked into one binary.
//
// The code lives in a non-test file for the same reason golden_harness.go
// does: a future `stellar-ingest` subcommand or CI job should be able to call
// it without going through `go test`.

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/withObsrvr/stellar-ingest/internal/parity"
)

// ParityInput is the contract handed to the live side.
//
// It exists so the live harness consumes provably identical authoritative
// bytes: it re-hashes ledgers.xdr against SHA256 before extracting anything.
// Without that check a stale or truncated copy would produce a discrepancy
// report about the wrong input.
type ParityInput struct {
	Fixture           string `json:"fixture"`
	NetworkPassphrase string `json:"network_passphrase"`
	StartLedger       uint32 `json:"start_ledger"`
	EndLedger         uint32 `json:"end_ledger"`
	LedgerCount       int    `json:"ledger_count"`
	ProtocolVersion   uint32 `json:"protocol_version"`

	// SHA256 is the digest of the decompressed XDR, matching the committed
	// fixture manifest. Hashing the compressed form would let a recompression
	// hide a change to the ledger bytes.
	SHA256 string `json:"sha256_decompressed_xdr"`

	// LedgersFile is relative to the input directory.
	LedgersFile string `json:"ledgers_file"`
}

// bronzeTables maps each Ticket 04 table name to the LedgerData field that
// carries its rows.
//
// It is a function rather than a map literal because Go has no way to name a
// struct field generically; the point is that every table is listed in one
// place, so adding a Bronze table without adding it here fails
// TestParityHarnessCoversEveryBronzeTable rather than silently dropping out
// of the comparison.
func bronzeTables(d *LedgerData) map[string]any {
	return map[string]any{
		"transactions":       d.Transactions,
		"operations":         d.Operations,
		"effects":            d.Effects,
		"trades":             d.Trades,
		"accounts":           d.Accounts,
		"offers":             d.Offers,
		"trustlines":         d.Trustlines,
		"account_signers":    d.AccountSigners,
		"claimable_balances": d.ClaimableBalances,
		"liquidity_pools":    d.LiquidityPools,
		"config_settings":    d.ConfigSettings,
		"ttl":                d.TTLEntries,
		"evicted_keys":       d.EvictedKeys,
		"contract_events":    d.ContractEvents,
		"contract_data":      d.ContractData,
		"contract_code":      d.ContractCode,
		"native_balances":    d.NativeBalances,
		"restored_keys":      d.RestoredKeys,
		"contract_creations": d.ContractCreations,
		"ledgers":            d.Ledgers,
		"token_transfers":    d.TokenTransfers,
	}
}

// BuildHistoricalParityRecord extracts every ledger in [start, end] through
// extractBronzeLedger and returns the canonical snapshot plus the full rows.
//
// The fixture is extracted twice. The second pass is the control for the
// non-determinism probe in parity.SnapshotRuns: without it, every table whose
// created_at is an ingestion wall clock reports as a discrepancy, and the
// dozen real findings are lost among them.
//
// era_id is left nil deliberately. It is operator-supplied provenance with no
// bearing on what the ledger contained, it is excluded from every digest
// anyway, and stamping a value here would only invite a reader to compare it
// against whatever the live deployment happens to be configured with.
func BuildHistoricalParityRecord(in ParityInput, ledgerPath string, pins map[string]string) (*parity.Record, *parity.RowSet, error) {
	source, err := NewXDRStreamSource(ledgerPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open fixture ledgers: %w", err)
	}
	defer source.Close()

	first, err := extractFixture(source, in)
	if err != nil {
		return nil, nil, err
	}
	control, err := extractFixture(source, in)
	if err != nil {
		return nil, nil, fmt.Errorf("control pass: %w", err)
	}

	record := parity.NewRecord(parity.SideHistorical, in.Fixture, pins)
	rowSet := &parity.RowSet{
		CanonicalFormVersion: parity.CanonicalFormVersion,
		Side:                 parity.SideHistorical,
		Fixture:              in.Fixture,
		Tables:               make(map[string][]parity.Row, len(first)),
	}
	for table, rows := range first {
		record.Tables[table] = parity.SnapshotRuns(rows, control[table])
		rowSet.Tables[table] = rows
	}
	return record, rowSet, nil
}

// extractFixture runs the Bronze mapping over every ledger in the fixture and
// returns the accumulated canonical rows per table.
//
// Accumulating across the whole fixture before snapshotting matches the live
// side, which extracts a batch at a time; a per-ledger snapshot would not be
// comparable.
func extractFixture(source *XDRStreamSource, in ParityInput) (map[string][]parity.Row, error) {
	accumulated := make(map[string][]parity.Row, len(parity.AllTables))

	for seq := in.StartLedger; seq <= in.EndLedger; seq++ {
		raw, err := source.getLedger(seq)
		if err != nil {
			return nil, fmt.Errorf("ledger %d: %w", seq, err)
		}

		meta, err := decodeLedgerMeta(seq, raw, nil)
		if err != nil {
			return nil, fmt.Errorf("decode ledger %d: %w", seq, err)
		}

		// onlyTables nil means every extractor runs, which is what parity
		// requires: a table skipped by selection would report as absent and
		// read as a missing mapping.
		data, err := extractBronzeLedger(meta, in.NetworkPassphrase, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("extract ledger %d: %w", seq, err)
		}

		for table, rows := range bronzeTables(data) {
			accumulated[table] = append(accumulated[table], parity.Rows(rows)...)
		}
	}
	return accumulated, nil
}

// WriteJSON writes v as indented JSON, creating parent directories.
//
// Indented rather than compact because these artifacts are reviewed in diffs:
// a one-column change in a 21-table digest file should show as one changed
// line, not as a rewritten blob.
func WriteJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ReadJSON decodes a JSON artifact written by either side, transparently
// handling the gzipped row sets.
func ReadJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		defer gz.Close()
		r = gz
	}
	return json.NewDecoder(r).Decode(v)
}

// WriteJSONGzip writes v as gzipped JSON.
//
// The row sets are the bulk of a parity export — mostly base64 XDR, around
// half a gigabyte across the eight fixtures uncompressed — and they are
// build output rather than something to review line by line. The digest
// records stay plain JSON for exactly the opposite reason.
func WriteJSONGzip(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	if err := json.NewEncoder(gz).Encode(v); err != nil {
		return err
	}
	return gz.Close()
}

// RenderParityMarkdown renders one fixture's comparison as the status table
// Ticket 04's deliverable 1 calls for.
//
// The verdict column is the harness's mechanical reading. Mapping it to the
// ticket's review vocabulary — provenance-only, approved semantic difference,
// bug — is a decision a reviewer makes, and the renderer deliberately does
// not guess at it.
func RenderParityMarkdown(c parity.FixtureComparison) string {
	var b strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }

	p("### %s\n\n", c.Fixture)
	if c.FormMismatch != "" {
		p("> **Not comparable:** %s. Every digest below is meaningless until both sides are rebuilt against the same canonical form.\n\n", c.FormMismatch)
	}
	p("Historical pins: `%s`  \n", renderPins(c.HistoricalPins))
	p("Live pins: `%s`  \n", renderPins(c.LivePins))
	p("Columns excluded as run-dependent: `%v`\n\n", c.ExcludedColumns)

	p("| Table | Verdict | Hist rows | Live rows | Hist-only columns | Live-only columns | Differing shared columns | Skipped (non-deterministic) |\n")
	p("|---|---|---:|---:|---|---|---|---|\n")
	for _, t := range c.Tables {
		p("| `%s` | %s | %d | %d | %s | %s | %s | %s |\n",
			t.Table, t.Verdict, t.HistoricalRowCount, t.LiveRowCount,
			renderColumns(t.HistoricalOnlyColumns),
			renderColumns(t.LiveOnlyColumns),
			renderColumns(t.DifferingColumns),
			renderColumns(t.SkippedColumns))
	}
	p("\n")
	return b.String()
}

func renderPins(pins map[string]string) string {
	if len(pins) == 0 {
		return "unrecorded"
	}
	keys := make([]string, 0, len(pins))
	for k := range pins {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += " "
		}
		out += k + "=" + pins[k]
	}
	return out
}

func renderColumns(cols []string) string {
	if len(cols) == 0 {
		return "—"
	}
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += "`" + c + "`"
	}
	return out
}
