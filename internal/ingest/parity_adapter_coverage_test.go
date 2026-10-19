package ingest

// Guard against this repository's Bronze mapping silently discarding data
// that stellar-extract provides.
//
// Most tables map a library row with a direct struct conversion —
// `TradeData(r)` — which the compiler enforces field-for-field: the
// conversion stops compiling the moment the two layouts differ. That is a
// stronger guarantee than any test, and the first half of this file exists
// to keep it, by failing if a table ever moves off that pattern to a
// hand-written mapping where fields can go missing unnoticed.
//
// The second half audits the one table that already uses a hand-written
// mapping. A field the library populates and the mapping never reads is lost
// before it reaches Parquet, and nothing downstream can tell: the column is
// absent, which is indistinguishable from the ledger not having the value.
//
// Comparing against the live ingester does not catch this. Where both drop
// the same field they agree, and the comparison reports the table as equal.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	extract "github.com/withObsrvr/stellar-extract"
)

// conversionMappedTables are the Bronze tables whose library rows are mapped
// by direct struct conversion.
//
// Listed by the local type name that appears in `rows[i] = X(r)`. Keeping a
// table here is a commitment that its local row type mirrors the library's
// exactly — which is precisely why a library field addition breaks the build
// rather than silently vanishing.
var conversionMappedTables = []string{
	"AccountSignerData",
	"ClaimableBalanceData",
	"ContractCodeData",
	"ContractCreationData",
	"ContractDataData",
	"ContractEventData",
	"EffectData",
	"EvictedKeyData",
	"LiquidityPoolData",
	"NativeBalanceData",
	"OfferData",
	"TokenTransferData",
	"TradeData",
	"TrustlineData",
	"TTLData",
}

// TestLibraryRowsStayConversionMapped keeps the compiler as the guard.
//
// A direct conversion cannot drop a field; a hand-written mapping can, and
// does — the live ingester loses fifty library fields that way. If one of
// these tables moves to a mapping function, the compile-time guarantee is
// gone and the table needs an explicit field audit instead.
func TestLibraryRowsStayConversionMapped(t *testing.T) {
	src := readSource(t, "worker.go")

	for _, typeName := range conversionMappedTables {
		if !strings.Contains(src, "rows[i] = "+typeName+"(r)") {
			t.Errorf("%s is no longer mapped by direct struct conversion in worker.go. "+
				"The compiler was the only thing guaranteeing it carries every library "+
				"field; either restore the conversion, or add the table to the audited "+
				"mappings below.", typeName)
		}
	}
}

// mappedConversions are the hand-written mappings from a library row, which
// the compiler does not check.
var mappedConversions = map[string]reflect.Type{
	"convertExtractedConfigSetting": reflect.TypeOf(extract.ConfigSettingData{}),
}

// deliberatelyNotMapped records library fields a mapping does not read, with
// the reason. An entry here is a decision that the cold store will not carry
// the field.
var deliberatelyNotMapped = map[string]string{}

// knownUnmapped records fields lost by accident rather than decision, each
// awaiting a Parquet column. Kept apart from deliberatelyNotMapped because a
// pending bug and a settled decision are not the same thing.
// Empty at the pinned stellar-extract v0.1.2: every field the library
// exposes today is carried. Bumping the pin is expected to add entries here
// — TxMaxFootprintEntries arrives with it — which is the point.
var knownUnmapped = map[string]string{}

// TestOutstandingUnmappedFields reports known losses on every run without
// failing: these are tracked, and a suite that is always red stops being
// read. It exists so the count cannot drift upward unnoticed.
func TestOutstandingUnmappedFields(t *testing.T) {
	keys := make([]string, 0, len(knownUnmapped))
	for k := range knownUnmapped {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("library field not yet stored: %s (%s)", k, knownUnmapped[k])
	}
	t.Logf("%d library fields awaiting a Parquet column", len(keys))
}

func TestMappedConversionsReadEveryLibraryField(t *testing.T) {
	reads := mappingFieldReads(t)

	var lost []string
	for fn, libType := range mappedConversions {
		read, ok := reads[fn]
		if !ok {
			t.Errorf("%s is in mappedConversions but was not found in the source", fn)
			continue
		}
		for i := 0; i < libType.NumField(); i++ {
			f := libType.Field(i)
			if !f.IsExported() || read[f.Name] {
				continue
			}
			key := fn + "." + f.Name
			if _, ok := deliberatelyNotMapped[key]; ok {
				continue
			}
			if _, ok := knownUnmapped[key]; ok {
				continue
			}
			lost = append(lost, key)
		}
	}
	sort.Strings(lost)

	for _, key := range lost {
		t.Errorf("%s: stellar-extract populates this field and the mapping never reads it, "+
			"so it is lost before it reaches Parquet. Map it, or record the reason in "+
			"deliberatelyNotMapped or knownUnmapped.", key)
	}
}

// A stale allowlist suppresses the alarm on a field that has since been
// mapped, or one the library no longer has.
func TestUnmappedAllowlistsAreCurrent(t *testing.T) {
	reads := mappingFieldReads(t)

	all := map[string]string{}
	for k, v := range deliberatelyNotMapped {
		all[k] = v
	}
	for k, v := range knownUnmapped {
		if _, dup := all[k]; dup {
			t.Errorf("%s is in both allowlists; it is either a decision or a bug, not both", k)
		}
		all[k] = v
	}

	for key, reason := range all {
		fn, field, _ := strings.Cut(key, ".")
		libType, ok := mappedConversions[fn]
		if !ok {
			t.Errorf("allowlist names %s, but there is no such mapping function", key)
			continue
		}
		if _, exists := libType.FieldByName(field); !exists {
			t.Errorf("allowlist names %s (%s), but stellar-extract no longer has that field", key, reason)
			continue
		}
		if reads[fn][field] {
			t.Errorf("%s is allowlisted but the mapping now reads it; remove the entry", key)
		}
	}
}

// mappingFieldReads returns, per mapping function, the library fields it
// reads off its parameter.
func mappingFieldReads(t *testing.T) map[string]map[string]bool {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "worker.go", nil, 0)
	if err != nil {
		t.Fatalf("parse worker.go: %v", err)
	}

	out := map[string]map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		if _, audited := mappedConversions[fn.Name.Name]; !audited {
			continue
		}
		param := libraryRowParam(fn)
		if param == "" {
			t.Errorf("%s takes no stellar-extract row parameter", fn.Name.Name)
			continue
		}

		read := map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == param {
				read[sel.Sel.Name] = true
			}
			return true
		})
		out[fn.Name.Name] = read
	}
	return out
}

func libraryRowParam(fn *ast.FuncDecl) string {
	if fn.Type.Params == nil {
		return ""
	}
	for _, p := range fn.Type.Params.List {
		sel, ok := p.Type.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "extract" && len(p.Names) > 0 {
			return p.Names[0].Name
		}
	}
	return ""
}

func readSource(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
