package main

// Guard the last mapping layer: LedgerData rows into the Parquet row structs.
//
// This repository maps a Bronze row three times before it reaches a file —
// stellar-extract row, then LedgerData row in types.go, then a Parquet*
// struct in parquet_writer.go. Only the first of those is checked by the
// compiler, through the direct struct conversions. The other two are
// hand-written, and a field dropped here reaches types.go and never reaches
// a file, which is indistinguishable downstream from the ledger not having
// the value.
//
// The live ingester lost fifty fields to exactly this shape one layer up.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// deliberatelyNotWritten records LedgerData fields the Parquet layer does
// not read, and why.
var deliberatelyNotWritten = map[string]string{
	// Written from the writer's own pipelineVersion rather than the row, so
	// a single run cannot emit rows claiming different build versions. The
	// Parquet column is version_label.
	"LedgerRowData.PipelineVersion": "written from ParquetWriter.pipelineVersion, not the row",

	// See claimableBalanceCreatedAtNote below.
	"ClaimableBalanceData.CreatedAt": claimableBalanceCreatedAtNote,

	// See tokenTransferHashNote below.
	"TokenTransferData.TransactionHash": tokenTransferHashNote,
}

// knownUnwritten records fields lost by accident rather than decision, each
// awaiting a Parquet column. Separate from the list above because a pending
// bug and a settled decision are not the same thing.
var knownUnwritten = map[string]string{}

// tokenTransferHashNote explains why the transfer stream drops the hash.
//
// transaction_hash was 775 GB of cold Bronze on its own -- 34.7 compressed
// bytes per row across 23.95 billion rows. A 64-character hex string is
// incompressible: random hex carries no redundancy for a block compressor to
// exploit, so it costs close to its full width in every file forever. Across
// cold Bronze the hash columns total roughly 2.9 TB, more than every XDR
// payload column combined.
//
// transaction_id is the same identity as an int64 at ~4 bytes per row, and it
// is non-null on all 23,950,927,112 rows in cold, so nothing is lost:
// transactions_row_v2 joined on transaction_id recovers the hash.
//
// Dropping it is safe here specifically because this table had no reader.
// stellar-query-api never selects it; serving-cold-backfill never selects it;
// the silver transformer builds token_transfers_raw from
// contract_events_stream_v1 and joins transactions on *that* table's hash.
// The column was written 23.95 billion times and read zero times.
//
// The same argument applies to operations_row_v2 (714 GB) but not the same
// freedom: query-api projects that hash and orders by it, so it needs the
// consumer change first. contract_events_stream_v1 (700 GB) has no
// transaction_id at all yet, so it needs the column added before its hash can
// go. transactions_row_v2 keeps its hash -- it is the lookup key for
// Horizon's tx-by-hash endpoint -- but could store it as 32-byte binary.
const tokenTransferHashNote = "identity carried by transaction_id; 775 GB of incompressible hex with no reader"

// claimableBalanceCreatedAtNote explains the one asymmetry the scan finds.
//
// ClaimableBalanceData.CreatedAt is an ingestion wall clock — the library
// stamps time.Now() — and ParquetClaimableBalance is the only snapshot type
// with no created_at column, where offers, trustlines, liquidity_pools and
// account_signers all have one.
//
// Adding the column for symmetry would add a field finding F7 already
// identifies as unreproducible from the ledger. Removing it from the others
// is a schema break. Neither belongs in a coverage audit, so it sits here
// until the canonical row contract decides whether ingestion wall clocks
// belong in Bronze at all.
const claimableBalanceCreatedAtNote = "ingestion wall clock; see Ticket 04 F7 and the canonical row contract"

func TestParquetLayerWritesEveryRowField(t *testing.T) {
	read, sourceType := parquetFieldReads(t)
	rowFields := ledgerDataRowFields(t)

	if len(read) < 20 {
		t.Fatalf("found %d Parquet conversions; the scan is broken, not the code", len(read))
	}

	var lost []string
	for ptype, fields := range read {
		src := sourceType[ptype]
		for _, f := range rowFields[src] {
			if fields[f] {
				continue
			}
			key := src + "." + f
			if _, ok := deliberatelyNotWritten[key]; ok {
				continue
			}
			if _, ok := knownUnwritten[key]; ok {
				continue
			}
			lost = append(lost, key+" (via "+ptype+")")
		}
	}
	sort.Strings(lost)

	for _, key := range lost {
		t.Errorf("%s: the row carries this field and the Parquet layer never reads it, "+
			"so it never reaches a file. Map it, or record the reason in "+
			"deliberatelyNotWritten or knownUnwritten.", key)
	}
}

// TestParquetAllowlistsAreCurrent keeps an entry from outliving the field it
// excuses, or from suppressing a field that has since been mapped.
func TestParquetAllowlistsAreCurrent(t *testing.T) {
	read, sourceType := parquetFieldReads(t)
	rowFields := ledgerDataRowFields(t)

	bySource := map[string]map[string]bool{}
	for ptype, fields := range read {
		bySource[sourceType[ptype]] = fields
	}

	all := map[string]string{}
	for k, v := range deliberatelyNotWritten {
		all[k] = v
	}
	for k, v := range knownUnwritten {
		if _, dup := all[k]; dup {
			t.Errorf("%s is in both allowlists; it is either a decision or a bug, not both", k)
		}
		all[k] = v
	}

	for key, reason := range all {
		src, field, _ := strings.Cut(key, ".")
		fields, ok := rowFields[src]
		if !ok {
			t.Errorf("allowlist names %s, but there is no such row type", key)
			continue
		}
		if !containsStr(fields, field) {
			t.Errorf("allowlist names %s (%s), but that field no longer exists", key, reason)
			continue
		}
		if bySource[src][field] {
			t.Errorf("%s is allowlisted but the Parquet layer now reads it; remove the entry", key)
		}
	}
}

// TestEveryBronzeTableHasAParquetConversion catches a table that reaches
// LedgerData and never reaches a writer.
func TestEveryBronzeTableHasAParquetConversion(t *testing.T) {
	_, sourceType := parquetFieldReads(t)

	converted := map[string]bool{}
	for _, src := range sourceType {
		converted[src] = true
	}
	for _, src := range ledgerDataRowTypeNames(t) {
		if !converted[src] {
			t.Errorf("%s rows are accumulated in LedgerData but no Parquet conversion reads them", src)
		}
	}
}

// parquetFieldReads returns, per Parquet struct, the source-row fields its
// conversion reads, and the source row type each conversion draws from.
//
// The conversions are written in several shapes — one field per line, several
// per line, some with helper calls around the value — so this walks the AST
// for selectors on the range variable rather than matching text.
func parquetFieldReads(t *testing.T) (map[string]map[string]bool, map[string]string) {
	t.Helper()

	fset := token.NewFileSet()
	// The whole package, not just parquet_writer.go: a conversion may
	// delegate to a helper in another file, as contract_events does to
	// parquetContractEventFromData in contract_event_utf8.go.
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	pkg, ok := pkgs["main"]
	if !ok {
		t.Fatal("package main not found")
	}
	files := make([]*ast.File, 0, len(pkg.Files))
	for _, f := range pkg.Files {
		files = append(files, f)
	}
	file := mustFile(t, pkg.Files, "parquet_writer.go")

	batchField := ledgerDataFieldTypes(t)
	reads := map[string]map[string]bool{}
	source := map[string]string{}

	ast.Inspect(file, func(n ast.Node) bool {
		rng, ok := n.(*ast.RangeStmt)
		if !ok || rng.Value == nil {
			return true
		}
		// range batch.<Field>
		sel, ok := rng.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		recv, ok := sel.X.(*ast.Ident)
		if !ok || recv.Name != "batch" {
			return true
		}
		srcType, ok := batchField[sel.Sel.Name]
		if !ok {
			return true
		}
		varName, ok := rng.Value.(*ast.Ident)
		if !ok {
			return true
		}

		// Find the Parquet composite literal inside, and every selector on
		// the range variable anywhere in the loop body — some conversions
		// compute a value above the literal and reference it inside.
		//
		// A conversion may also delegate to a helper, as contract_events
		// does. The literal is then in another function, and the loop body
		// reads no fields at all; without following the call the table
		// would silently escape the audit.
		var ptype string
		fields := map[string]bool{}
		if helper := delegatedConversion(rng.Body); helper != "" {
			hp, hf := helperConversion(files, helper)
			if hp != "" {
				reads[hp] = hf
				source[hp] = srcType
				return true
			}
		}
		ast.Inspect(rng.Body, func(m ast.Node) bool {
			switch node := m.(type) {
			case *ast.CompositeLit:
				if id, ok := node.Type.(*ast.Ident); ok && strings.HasPrefix(id.Name, "Parquet") {
					ptype = id.Name
				}
			case *ast.SelectorExpr:
				if id, ok := node.X.(*ast.Ident); ok && id.Name == varName.Name {
					fields[node.Sel.Name] = true
				}
			}
			return true
		})
		if ptype == "" {
			return true
		}
		reads[ptype] = fields
		source[ptype] = srcType
		return true
	})
	return reads, source
}

// ledgerDataFieldTypes maps a LedgerData field name to its element row type.
func ledgerDataFieldTypes(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	rt := reflect.TypeOf(LedgerData{})
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Type.Kind() != reflect.Slice {
			continue
		}
		out[f.Name] = f.Type.Elem().Name()
	}
	return out
}

func ledgerDataRowTypeNames(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	var out []string
	for _, v := range ledgerDataFieldTypes(t) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// ledgerDataRowFields returns the exported fields of each row type.
func ledgerDataRowFields(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	rt := reflect.TypeOf(LedgerData{})
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Type.Kind() != reflect.Slice {
			continue
		}
		el := f.Type.Elem()
		var fields []string
		for j := 0; j < el.NumField(); j++ {
			if el.Field(j).IsExported() {
				fields = append(fields, el.Field(j).Name)
			}
		}
		out[el.Name()] = fields
	}
	return out
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// delegatedConversion returns the name of a function the loop body assigns
// straight into rows[i], or "" when the conversion is inline.
func delegatedConversion(body *ast.BlockStmt) string {
	var name string
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 {
			return true
		}
		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok {
			name = id.Name
		}
		return true
	})
	return name
}

// helperConversion reads the Parquet struct and source fields out of a
// delegated conversion function.
func helperConversion(files []*ast.File, name string) (string, map[string]bool) {
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != name || fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
				continue
			}
			if len(fn.Type.Params.List[0].Names) == 0 {
				continue
			}
			param := fn.Type.Params.List[0].Names[0].Name

			var ptype string
			fields := map[string]bool{}
			ast.Inspect(fn.Body, func(m ast.Node) bool {
				switch node := m.(type) {
				case *ast.CompositeLit:
					if id, ok := node.Type.(*ast.Ident); ok && strings.HasPrefix(id.Name, "Parquet") {
						ptype = id.Name
					}
				case *ast.SelectorExpr:
					if id, ok := node.X.(*ast.Ident); ok && id.Name == param {
						fields[node.Sel.Name] = true
					}
				}
				return true
			})
			return ptype, fields
		}
	}
	return "", nil
}

// mustFile picks one parsed file out of the package by base name.
func mustFile(t *testing.T, files map[string]*ast.File, name string) *ast.File {
	t.Helper()
	for path, f := range files {
		if strings.HasSuffix(path, name) {
			return f
		}
	}
	t.Fatalf("%s not found in package", name)
	return nil
}
