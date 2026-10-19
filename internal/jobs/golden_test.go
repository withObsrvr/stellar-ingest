package jobs

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/withObsrvr/stellar-ingest/internal/adapters"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"

	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

var updateGolden = flag.Bool("update", false, "regenerate golden files instead of comparing against them")

// scrubbedEnv lists environment variables that must not leak into a fixture
// run. ENABLE_FLOWCTL in particular is exported in at least one developer
// shell, and makes a standalone run fail closed with "FLOWCTL_ENDPOINT is
// required". The ledgerstream variables would silently redirect the source.
var scrubbedEnv = []string{
	"ENABLE_FLOWCTL", "FLOWCTL_ENDPOINT", "FLOWCTL_COMPONENT_ID",
	"FLOWCTL_RUN_ID", "FLOWCTL_ATTEMPT", "FLOWCTL_HEARTBEAT_INTERVAL_MS",
	"BACKEND_TYPE", "STORAGE_TYPE", "BUCKET_NAME", "BUCKET_PATH",
	"RPC_ENDPOINT", "RPC_AUTH_HEADER", "S3_ENDPOINT_URL",
	"STELLAR_CORE_BINARY_PATH", "STELLAR_CORE_CONFIG_PATH",
	"HISTORY_ARCHIVE_URLS", "CAPTIVE_CORE_STORAGE_PATH",
}

func scrubEnv(t *testing.T) {
	t.Helper()
	for _, k := range scrubbedEnv {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}

// runFixture extracts a fixture through the real orchestrator and returns the
// bronze output directory.
func runFixture(t *testing.T, meta fixtures.Meta, ledgerPath, outputDir string) string {
	t.Helper()

	// NewOrchestrator does not create its output directory; main.go does that
	// before constructing one. A test driving it directly has to do the same.
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	// --workers is pinned rather than left at runtime.NumCPU() so shard
	// boundaries — and therefore output layout — are identical on every host.
	cfg := domain.OrchestratorConfig{
		StartLedger:       meta.StartLedger,
		EndLedger:         meta.EndLedger,
		NumWorkers:        2,
		OutputDir:         outputDir,
		BatchSize:         1000,
		NetworkPassphrase: meta.NetworkPassphrase,
		StorageType:       "XDR",
		Bucket:            ledgerPath,
	}

	if err := NewOrchestrator(cfg).Run(); err != nil {
		t.Fatalf("extract %s: %v", meta.Name, err)
	}

	bronze, err := findAttemptDir(outputDir)
	if err != nil {
		t.Fatalf("locate attempt dir for %s: %v", meta.Name, err)
	}
	return bronze
}

// TestAllFixtureIntegrity checks decompressed authoritative XDR, not the
// compressor's output bytes, so fixture recompression cannot hide corruption.
func TestAllFixtureIntegrity(t *testing.T) {
	for _, name := range fixtures.Names(t) {
		t.Run(name, func(t *testing.T) {
			meta, _ := fixtures.LoadMeta(t, name)
			path := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), t.TempDir())
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := domain.SHA256Hex(data); got != meta.SHA256 {
				t.Fatalf("XDR SHA-256 = %s, want %s", got, meta.SHA256)
			}
		})
	}
}

func goldenPath(name string) string {
	return filepath.Join("..", "..", "testdata", "golden", name+".json")
}

// TestGoldenBronzeOutputs is the Phase 1 output-parity gate: every fixture is
// extracted through the real orchestrator and every Bronze table it emits is
// compared against a committed digest.
//
// Regenerate with:
//
//	go test -run TestGoldenBronzeOutputs -update .
//
// A regenerated golden must be reviewed as a diff. Never regenerate to make a
// failing test pass.
func TestGoldenBronzeOutputs(t *testing.T) {
	scrubEnv(t)

	db, err := adapters.OpenDuckDB()
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	for _, name := range fixtures.Names(t) {
		t.Run(name, func(t *testing.T) {
			meta, _ := fixtures.LoadMeta(t, name)
			fixtureDir := filepath.Join("..", "..", "testdata", "fixtures", name)

			work := t.TempDir()
			ledgerPath := fixtures.Materialize(t, fixtureDir, work)
			bronze := runFixture(t, meta, ledgerPath, filepath.Join(work, "out"))

			got, err := digestFixtureOutput(db, bronze, name)
			if err != nil {
				t.Fatalf("digest output: %v", err)
			}

			if *updateGolden {
				writeGolden(t, name, got)
				return
			}

			want := readGolden(t, name)
			compareDigests(t, want, got)
		})
	}
}

func writeGolden(t *testing.T, name string, d FixtureDigest) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join("..", "..", "testdata", "golden"), 0o755); err != nil {
		t.Fatalf("mkdir golden: %v", err)
	}
	blob, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		t.Fatalf("marshal golden: %v", err)
	}
	if err := os.WriteFile(goldenPath(name), append(blob, '\n'), 0o644); err != nil {
		t.Fatalf("write golden: %v", err)
	}
	t.Logf("wrote golden for %s: %d tables", name, len(d.Tables))
}

func readGolden(t *testing.T, name string) FixtureDigest {
	t.Helper()
	blob, err := os.ReadFile(goldenPath(name))
	if err != nil {
		t.Fatalf("read golden for %s (regenerate with -update): %v", name, err)
	}
	var d FixtureDigest
	if err := json.Unmarshal(blob, &d); err != nil {
		t.Fatalf("parse golden for %s: %v", name, err)
	}
	return d
}

// compareDigests reports table-set differences separately from content
// differences, because a table disappearing is a different kind of regression
// from a table's rows changing.
func compareDigests(t *testing.T, want, got FixtureDigest) {
	t.Helper()

	for _, name := range sortedKeys(want.Tables) {
		if _, ok := got.Tables[name]; !ok {
			t.Errorf("table %q was not emitted; golden expects %d rows", name, want.Tables[name].RowCount)
		}
	}
	for _, name := range sortedKeys(got.Tables) {
		if _, ok := want.Tables[name]; !ok {
			t.Errorf("table %q emitted but absent from golden (%d rows)", name, got.Tables[name].RowCount)
		}
	}

	for _, name := range sortedKeys(want.Tables) {
		w := want.Tables[name]
		g, ok := got.Tables[name]
		if !ok {
			continue
		}
		if w.RowCount != g.RowCount {
			t.Errorf("table %q row count = %d, want %d", name, g.RowCount, w.RowCount)
		}
		if diff := columnDiff(w.Columns, g.Columns); diff != "" {
			t.Errorf("table %q schema drift: %s", name, diff)
		}
		if w.ContentHash != g.ContentHash && w.RowCount == g.RowCount {
			// Name the columns that moved. Without this a mismatch says only
			// that something in a 30-column table changed, and localising it
			// means bisecting by hand.
			if drifted := driftedColumns(w.ColumnHashes, g.ColumnHashes); len(drifted) > 0 {
				t.Errorf("table %q changed in %d of %d columns: %s (row count unchanged at %d)",
					name, len(drifted), len(w.Columns), strings.Join(drifted, ", "), w.RowCount)
			} else {
				t.Errorf("table %q content hash = %s, want %s, but every column digest matches — "+
					"the values are the same and their association into rows changed",
					name, g.ContentHash, w.ContentHash)
			}
		}
	}
}

// driftedColumns lists columns whose value multiset differs, sorted.
func driftedColumns(want, got map[string]string) []string {
	var out []string
	for col, wh := range want {
		if gh, ok := got[col]; ok && gh != wh {
			out = append(out, col)
		}
	}
	sort.Strings(out)
	return out
}

func columnDiff(want, got []string) string {
	ws, gs := map[string]bool{}, map[string]bool{}
	for _, c := range want {
		ws[c] = true
	}
	for _, c := range got {
		gs[c] = true
	}
	var added, removed []string
	for _, c := range got {
		if !ws[c] {
			added = append(added, c)
		}
	}
	for _, c := range want {
		if !gs[c] {
			removed = append(removed, c)
		}
	}
	var parts []string
	if len(added) > 0 {
		parts = append(parts, "added "+strings.Join(added, ","))
	}
	if len(removed) > 0 {
		parts = append(parts, "removed "+strings.Join(removed, ","))
	}
	return strings.Join(parts, "; ")
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestFixturesCoverAllBronzeTables asserts the fixture set collectively
// touches every one of the 21 Bronze tables.
//
// No single ledger range exercises all of them — a modern range emits no
// config_settings, and an early range emits no contract data — so coverage is
// only meaningful across the whole set. Without this, a table could go
// permanently untested and nothing would say so.
func TestFixturesCoverAllBronzeTables(t *testing.T) {
	covered := map[string]bool{}
	for _, name := range fixtures.Names(t) {
		blob, err := os.ReadFile(goldenPath(name))
		if err != nil {
			t.Skipf("golden for %s missing; run with -update first", name)
		}
		var d FixtureDigest
		if err := json.Unmarshal(blob, &d); err != nil {
			t.Fatalf("parse golden %s: %v", name, err)
		}
		for table, digest := range d.Tables {
			if digest.RowCount > 0 {
				covered[table] = true
			}
		}
	}

	var missing []string
	for _, outputDir := range uniqueOutputDirs() {
		if !covered[outputDir] {
			missing = append(missing, outputDir)
		}
	}

	// Fail on any gap that is not a recorded one, so coverage can never
	// silently shrink.
	for _, table := range missing {
		if _, known := knownUncoveredTables[table]; !known {
			t.Errorf("Bronze table %q has no non-empty rows in any fixture, and is not a recorded gap", table)
			t.Logf("add a fixture that produces it, or add it to knownUncoveredTables with the reason")
		}
	}

	// Fail when a recorded gap closes, so the list cannot rot into a lie about
	// what is actually untested.
	for table, reason := range knownUncoveredTables {
		if covered[table] {
			t.Errorf("Bronze table %q is now covered by a fixture; remove it from knownUncoveredTables (recorded reason: %s)", table, reason)
		}
	}

	t.Logf("%d of %d Bronze tables covered, %d recorded gaps",
		len(uniqueOutputDirs())-len(missing), len(uniqueOutputDirs()), len(knownUncoveredTables))
}

// knownUncoveredTables records Bronze tables that no committed fixture
// produces a non-empty row for, with the reason.
//
// This is a visible debt list, not an exemption. TestFixturesCoverAllBronzeTables
// fails both when an unrecorded table is missing and when a recorded one
// becomes covered, so the list has to stay accurate.
var knownUncoveredTables = map[string]string{}

func uniqueOutputDirs() []string {
	seen := map[string]bool{}
	var out []string
	for _, d := range domain.ExtractorOutputDirByName {
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

// TestExtractionIsDeterministic extracts the same fixture twice and requires
// byte-identical digests.
//
// This is the test that catches non-reproducibility at its source, rather than
// as a confusing golden mismatch later. It found operations.contracts_involved
// being built by ranging over a Go map, whose iteration order is randomised —
// so the same ledger produced a different JSON array on every run.
//
// It runs against a Soroban fixture because the classic-era fixtures do not
// populate the affected columns and would not have caught it.
func TestExtractionIsDeterministic(t *testing.T) {
	scrubEnv(t)

	db, err := adapters.OpenDuckDB()
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	const name = "pubnet-tiny-dump"
	meta, _ := fixtures.LoadMeta(t, name)
	fixtureDir := filepath.Join("..", "..", "testdata", "fixtures", name)

	work := t.TempDir()
	ledgerPath := fixtures.Materialize(t, fixtureDir, work)

	digests := make([]FixtureDigest, 2)
	for i := range digests {
		bronze := runFixture(t, meta, ledgerPath, filepath.Join(work, fmt.Sprintf("out%d", i)))
		d, err := digestFixtureOutput(db, bronze, name)
		if err != nil {
			t.Fatalf("digest run %d: %v", i, err)
		}
		digests[i] = d
	}

	first, second := digests[0], digests[1]
	for _, table := range sortedKeys(first.Tables) {
		a := first.Tables[table]
		b, ok := second.Tables[table]
		if !ok {
			t.Errorf("table %q emitted on the first run but not the second", table)
			continue
		}
		if a.ContentHash != b.ContentHash {
			t.Errorf("table %q is not reproducible: run 1 hash %s, run 2 hash %s (%d rows both runs)",
				table, a.ContentHash, b.ContentHash, a.RowCount)
		}
	}
	for _, table := range sortedKeys(second.Tables) {
		if _, ok := first.Tables[table]; !ok {
			t.Errorf("table %q emitted on the second run but not the first", table)
		}
	}
}
