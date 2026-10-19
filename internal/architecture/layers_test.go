// Package architecture holds no code. It holds the one test that keeps the
// layering §2 describes from quietly decaying.
//
// §2: "Recommended layers: domain models and validation → application
// operations (commands, queries, jobs) → adapters (CLI, API, TUI, GUI, agent
// tools) → persistence, streaming, and external integrations. Cobra handlers
// must not own business rules."
//
// PR #22 established these edges by moving 69 files, and nothing has
// enforced them since: the only thing stopping internal/domain importing
// internal/jobs was that nobody had typed it. Go's compiler catches an
// import CYCLE, which is the symptom, long after the design has gone --
// a domain package that reaches into adapters is already wrong while still
// compiling fine.
//
// The rule that matters most is the direction of the arrow. Anything may
// depend on a leaf; nothing may depend on a layer above it.
package architecture

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const modulePath = "github.com/withObsrvr/stellar-ingest/internal/"

// allowed lists, for each package, every internal package it may import.
//
// Deliberately an allowlist rather than a set of forbidden pairs: a new
// package added to the tree starts with no permissions and has to be given
// them here, which is a sentence of thought at the moment the dependency is
// introduced rather than an archaeology exercise a year later.
var allowed = map[string][]string{
	// Leaves. They are the vocabulary everything else is written in, so
	// they may depend on nothing: a leaf that grows an edge stops being
	// safe for the layers below to use.
	"status":     {},
	"exit":       {},
	"logging":    {},
	"events":     {},
	"config":     {},
	"credential": {},
	"parity":     {},
	"fixtures":   {},
	"bronze":     {},

	// Domain: value objects and invariants. It may narrate and report
	// status, and nothing else -- a domain type that knows about DuckLake
	// is not a domain type.
	"domain": {"logging", "status"},

	// Adapters: the outside world. Parquet, DuckDB, PostgreSQL, archives.
	"adapters": {"domain", "logging"},

	// Jobs: execution, status and checkpoint coordination.
	"jobs": {"adapters", "domain", "logging", "parity", "status"},

	// App: the use cases. It may use everything below it and knows nothing
	// about a terminal.
	"app": {"adapters", "credential", "domain", "events", "exit", "jobs", "logging", "status"},

	// CLI: flags, parsing, rendering. The top of the tree.
	"cli": {"app", "adapters", "config", "credential", "domain", "events", "exit", "jobs", "logging", "status"},

	// The architecture package itself imports nothing.
	"architecture": {},
}

// universal packages may be imported from any layer.
//
// fixtures is test support with no dependencies of its own. It was made a
// non-test package in the layer split precisely so every layer's tests
// could load the same manifest fixtures -- a test helper cannot cross a
// package boundary, so the alternative was a copy per layer. Allowing it
// everywhere is the honest encoding of that decision; it cannot create a
// cycle because it imports nothing.
var universal = map[string]bool{"fixtures": true}

// forbiddenImporters names packages that must not be imported from inside
// internal/ at all. cmd/ is the only legitimate consumer of cli.
var topOfTree = map[string]string{
	"cli": "only cmd/ may import internal/cli; a package that reaches back into the CLI has " +
		"business logic in an adapter, which is what §2 forbids",
}

func TestLayerBoundaries(t *testing.T) {
	root := filepath.Join("..", "..", "internal")
	packages, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read internal/: %v", err)
	}

	checked := 0
	for _, entry := range packages {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		permitted, known := allowed[name]
		if !known {
			t.Errorf("internal/%s has no entry in the allowed map. Add one naming exactly what "+
				"it may import; a new package starts with no permissions on purpose.", name)
			continue
		}
		checked++

		permittedSet := make(map[string]bool, len(permitted))
		for _, p := range permitted {
			permittedSet[p] = true
		}

		for _, imported := range internalImports(t, filepath.Join(root, name)) {
			if imported == name {
				continue
			}
			if universal[imported] {
				continue
			}
			if !permittedSet[imported] {
				t.Errorf("internal/%s imports internal/%s, which the layering does not allow.\n"+
					"  %s may import: %s\n"+
					"  If this edge is right, the layering changed and this list should say so.",
					name, imported, name, describe(permitted))
			}
			if reason, forbidden := topOfTree[imported]; forbidden {
				t.Errorf("internal/%s imports internal/%s: %s", name, imported, reason)
			}
		}
	}

	if checked < 10 {
		t.Fatalf("only checked %d packages; this test is not reading the tree", checked)
	}
}

// The arrow points one way. If A may import B then B must not import A, and
// stating it separately catches a pair of allowlist entries that are each
// individually plausible.
func TestNoDeclaredCycles(t *testing.T) {
	for pkg, imports := range allowed {
		for _, imported := range imports {
			reverse, known := allowed[imported]
			if !known {
				t.Errorf("%s is allowed to import %s, which has no entry of its own", pkg, imported)
				continue
			}
			for _, back := range reverse {
				if back == pkg {
					t.Errorf("the allowlist permits a cycle: %s imports %s and %s imports %s",
						pkg, imported, imported, pkg)
				}
			}
		}
	}
}

// A universal package is importable from every layer, so it must import
// nothing: one edge out of it would be an edge every layer inherits.
func TestUniversalPackagesImportNothing(t *testing.T) {
	for name := range universal {
		deps, known := allowed[name]
		if !known {
			t.Errorf("%s is universal but has no allowlist entry", name)
			continue
		}
		if len(deps) != 0 {
			t.Errorf("%s is importable from every layer but imports %s; that edge would be "+
				"inherited by every layer at once", name, describe(deps))
		}
		if found := internalImports(t, filepath.Join("..", "..", "internal", name)); len(found) != 0 {
			t.Errorf("%s is universal but actually imports %s", name, describe(found))
		}
	}
}

// The leaves are the vocabulary the rest of the tree is written in. One of
// them growing a dependency is how a cycle starts.
func TestLeavesStayLeaves(t *testing.T) {
	for _, leaf := range []string{"status", "exit", "logging", "events", "config", "credential"} {
		if deps := allowed[leaf]; len(deps) != 0 {
			t.Errorf("%s is a leaf but is allowed to import %s; the layers below it depend on "+
				"it being safe to use from anywhere", leaf, describe(deps))
		}
	}
}

// internalImports returns the internal packages a directory's Go files
// import, test files included: a test that reaches across a boundary is the
// usual way the first illegal edge arrives.
func internalImports(t *testing.T, dir string) []string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}

	seen := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, spec := range file.Imports {
				path := strings.Trim(spec.Path.Value, `"`)
				if !strings.HasPrefix(path, modulePath) {
					continue
				}
				// internal/bronze/restored nests, so take the first segment.
				rest := strings.TrimPrefix(path, modulePath)
				if i := strings.IndexByte(rest, '/'); i >= 0 {
					rest = rest[:i]
				}
				seen[rest] = true
			}
		}
	}

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func describe(names []string) string {
	if len(names) == 0 {
		return "nothing"
	}
	return strings.Join(names, ", ")
}
