// Package fixtures loads the committed pubnet ledger fixtures.
//
// It exists because the engine split into layers and these helpers did not:
// a test helper cannot cross a package boundary, and tests in domain,
// adapters and jobs all read the same eight fixtures. Duplicating the loader
// three times would mean three chances for them to disagree about what a
// fixture is.
//
// Not a _test.go file, so it compiles into any package that imports it --
// which is the point. Nothing in the production path imports it.
package fixtures

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// Dir is where the fixtures live, relative to a package inside internal/.
//
// Two levels up, because the fixtures are shared by every layer and so sit
// above all of them rather than inside one.
const Dir = "../../testdata"

// Meta is a fixture's manifest: which ledgers it holds, on which network,
// and what the committed bytes should hash to.
//
// Every field the tests read, which is how it was arrived at: the previous
// per-package copies each declared a subset, and a field one copy lacked read
// as zero rather than as an error.
type Meta struct {
	Name              string `json:"name"`
	Description       string `json:"description"`
	NetworkPassphrase string `json:"network_passphrase"`
	ProtocolVersion   uint32 `json:"protocol_version"`
	StartLedger       uint32 `json:"start_ledger"`
	EndLedger         uint32 `json:"end_ledger"`
	LedgerCount       int    `json:"ledger_count"`
	SHA256            string `json:"sha256"`
	Bytes             int64  `json:"bytes"`
	CompressedBytes   int64  `json:"compressed_bytes"`
	Format            string `json:"format"`
	Source            string `json:"source"`
	FetchedWith       string `json:"fetched_with"`
}

// Path returns a path inside the fixture tree.
func Path(parts ...string) string {
	return filepath.Join(append([]string{Dir}, parts...)...)
}

// LoadMeta reads one fixture's manifest and returns it with the path to its
// uncompressed ledger stream.
//
// The second return is the ledgers.xdr path, not the directory, because that
// is what the per-package helpers this replaced returned. A helper that
// changed its contract while being consolidated would have been a silent
// behaviour change dressed up as a refactor.
func LoadMeta(t *testing.T, name string) (Meta, string) {
	t.Helper()
	dir := Path("fixtures", name)
	raw, err := os.ReadFile(filepath.Join(dir, "fixture.json"))
	if err != nil {
		t.Fatalf("read fixture manifest: %v", err)
	}
	var meta Meta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("parse fixture manifest: %v", err)
	}
	if meta.Name == "" {
		meta.Name = name
	}
	return meta, filepath.Join(dir, "ledgers.xdr")
}

// Names lists every committed fixture.
func Names(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(Path("fixtures"))
	if err != nil {
		t.Fatalf("read fixtures dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names
}

// Materialize returns a path to a fixture's uncompressed ledger stream,
// decompressing into dir when the fixture is stored as .zst.
//
// Most fixtures are committed compressed; one is not, because it is small
// enough to read in a diff. Callers should not care which.
func Materialize(t *testing.T, fixtureDir, dir string) string {
	t.Helper()

	if plain := filepath.Join(fixtureDir, "ledgers.xdr"); exists(plain) {
		return plain
	}

	src := filepath.Join(fixtureDir, "ledgers.xdr.zst")
	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open %s: %v", src, err)
	}
	defer in.Close()

	dec, err := zstd.NewReader(in)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()

	out := filepath.Join(dir, "ledgers.xdr")
	f, err := os.Create(out)
	if err != nil {
		t.Fatalf("create %s: %v", out, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, dec.IOReadCloser()); err != nil {
		t.Fatalf("decompress %s: %v", src, err)
	}
	return out
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
