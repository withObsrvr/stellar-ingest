package jobs

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/parity"
)

// parityOutEnv names the directory the Ticket 04 artifacts are written to.
// The export is opt-in because it writes tens of megabytes of canonical rows
// per fixture, which has no place in a normal `go test ./...`.
const parityOutEnv = "PARITY_OUT"

// parityFixturesEnv optionally restricts the export to a comma-separated
// subset, for iterating on one table without re-extracting every fixture.
const parityFixturesEnv = "PARITY_FIXTURES"

// TestParityHarnessCoversEveryBronzeTable is the guard that keeps a new
// Bronze table from quietly dropping out of the Ticket 04 comparison.
//
// bronzeTables has to be written out by hand because Go cannot enumerate
// struct fields by name generically. This test is what makes that hand-written
// list trustworthy: add a table to domain.LedgerData without adding it here and the
// comparison would report it as absent on both sides, which reads as "nothing
// to check" rather than "nobody looked".
func TestParityHarnessCoversEveryBronzeTable(t *testing.T) {
	covered := bronzeTables(&domain.LedgerData{})

	if len(covered) != len(parity.AllTables) {
		t.Fatalf("harness covers %d tables, Ticket 04 names %d", len(covered), len(parity.AllTables))
	}
	for _, name := range parity.AllTables {
		if _, ok := covered[name]; !ok {
			t.Errorf("Bronze table %q has no entry in bronzeTables", name)
		}
	}

	// Every domain.LedgerData row slice must be reachable. Meta is the ledger's
	// decode context, not a Bronze output, so it is the one exemption.
	fields := exportedSliceFields(t, domain.LedgerData{})
	if len(fields) != len(covered) {
		t.Errorf("domain.LedgerData has %d row slices but bronzeTables maps %d; a table was added without registering it",
			len(fields), len(covered))
	}
}

// TestParityExportHistorical writes the historical side's artifacts.
//
//	PARITY_OUT=/tmp/parity go test -run TestParityExportHistorical .
//
// It produces, per fixture: the decompressed authoritative XDR and an
// input manifest for the live side to consume, a committable digest record,
// and the full canonical rows used to explain a difference.
func TestParityExportHistorical(t *testing.T) {
	out := os.Getenv(parityOutEnv)
	if out == "" {
		t.Skipf("set %s=<dir> to export Ticket 04 parity artifacts", parityOutEnv)
	}

	pins := historicalPins(t)
	for _, name := range selectedFixtures(t) {
		t.Run(name, func(t *testing.T) {
			meta, _ := fixtures.LoadMeta(t, name)

			inputDir := filepath.Join(out, "input", name)
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("create input dir: %v", err)
			}
			// Fixtures are committed either zstd-compressed or plain.
			// materializeLedgers decompresses into inputDir in the first
			// case but returns the committed path in the second, so the
			// plain ones have to be copied in: the live side is handed a
			// directory, not this repository's testdata tree.
			ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), inputDir)
			if filepath.Dir(ledgerPath) != inputDir {
				ledgerPath = copyFile(t, ledgerPath, filepath.Join(inputDir, "ledgers.xdr"))
			}

			// Hash what was actually written, not what the manifest claims.
			// A fixture whose bytes no longer match its recorded digest must
			// stop the run here rather than seed a parity report.
			sum := sha256File(t, ledgerPath)
			if sum != meta.SHA256 {
				t.Fatalf("fixture %s: decompressed XDR hashes to %s, manifest says %s", name, sum, meta.SHA256)
			}

			in := ParityInput{
				Fixture:           name,
				NetworkPassphrase: meta.NetworkPassphrase,
				StartLedger:       meta.StartLedger,
				EndLedger:         meta.EndLedger,
				LedgerCount:       meta.LedgerCount,
				ProtocolVersion:   meta.ProtocolVersion,
				SHA256:            sum,
				LedgersFile:       filepath.Base(ledgerPath),
			}
			if err := WriteJSON(filepath.Join(inputDir, "input.json"), in); err != nil {
				t.Fatalf("write input manifest: %v", err)
			}

			record, rows, err := BuildHistoricalParityRecord(in, ledgerPath, pins)
			if err != nil {
				t.Fatalf("extract %s: %v", name, err)
			}
			if err := WriteJSON(filepath.Join(out, "historical", name+".digest.json"), record); err != nil {
				t.Fatalf("write digest: %v", err)
			}
			if err := WriteJSONGzip(filepath.Join(out, "historical", name+".rows.json.gz"), rows); err != nil {
				t.Fatalf("write rows: %v", err)
			}

			t.Logf("exported %s: %d tables emitted", name, emittedCount(record))
		})
	}
}

// TestParityExportIsDeterministic checks that two extractions of the same
// fixture produce identical digests.
//
// The historical extractor fans out to 21 concurrent goroutines and merges
// their results, so row order varies between runs by construction. If the
// canonical form did not absorb that, every parity report would be noise and
// a real discrepancy would be unprovable.
func TestParityExportIsDeterministic(t *testing.T) {
	name := "pubnet-genesis-p1" // smallest fixture; this runs in the default suite
	meta, _ := fixtures.LoadMeta(t, name)
	ledgerPath := fixtures.Materialize(t, filepath.Join("..", "..", "testdata", "fixtures", name), t.TempDir())

	in := ParityInput{
		Fixture:           name,
		NetworkPassphrase: meta.NetworkPassphrase,
		StartLedger:       meta.StartLedger,
		EndLedger:         meta.EndLedger,
	}

	first, _, err := BuildHistoricalParityRecord(in, ledgerPath, nil)
	if err != nil {
		t.Fatalf("first extraction: %v", err)
	}
	second, _, err := BuildHistoricalParityRecord(in, ledgerPath, nil)
	if err != nil {
		t.Fatalf("second extraction: %v", err)
	}

	for _, table := range parity.AllTables {
		a, b := first.Tables[table], second.Tables[table]
		if a.Emitted != b.Emitted {
			t.Errorf("%s: emitted %v then %v", table, a.Emitted, b.Emitted)
			continue
		}
		if a.TableDigest != b.TableDigest {
			t.Errorf("%s: digest changed between runs (%d then %d rows)", table, a.RowCount, b.RowCount)
		}
	}
}

// historicalPins records what the numbers came from. The handoff is explicit
// that a branch name is not a pin: a report that cannot be tied to a revision
// cannot be re-checked after either tree moves.
func historicalPins(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		"repo":            "stellar-ingest",
		"revision":        gitRevision(t, "."),
		"stellar-extract": modulePin(t, "github.com/withObsrvr/stellar-extract"),
	}
}

func gitRevision(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	rev := strings.TrimSpace(string(out))

	// A dirty tree makes the revision a half-truth; say so rather than
	// record a commit the artifacts were not actually produced from.
	status, err := exec.Command("git", "-C", dir, "status", "--porcelain").Output()
	if err == nil && len(strings.TrimSpace(string(status))) > 0 {
		rev += "-dirty"
	}
	return rev
}

func modulePin(t *testing.T, module string) string {
	t.Helper()
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == module {
			return fields[1]
		}
	}
	return "unknown"
}

func selectedFixtures(t *testing.T) []string {
	t.Helper()
	if sel := os.Getenv(parityFixturesEnv); sel != "" {
		return strings.Split(sel, ",")
	}
	return fixtures.Names(t)
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func emittedCount(r *parity.Record) int {
	n := 0
	for _, s := range r.Tables {
		if s.Emitted {
			n++
		}
	}
	return n
}

// exportedSliceFields lists domain.LedgerData's row-slice fields. Meta is excluded:
// it is the ledger's decode context, not a Bronze output.
func exportedSliceFields(t *testing.T, v any) []string {
	t.Helper()
	rt := reflect.TypeOf(v)
	var out []string
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() || f.Type.Kind() != reflect.Slice {
			continue
		}
		out = append(out, f.Name)
	}
	return out
}

// copyFile copies src to dst and returns dst.
func copyFile(t *testing.T, src, dst string) string {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open %s: %v", src, err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		t.Fatalf("create %s: %v", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
	return dst
}
