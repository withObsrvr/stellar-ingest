package adapters

// splitSQLStatements once split on ";" before stripping comments. The schema
// file happened to contain no semicolon inside a comment, so it worked; the
// views file has one in its first paragraph, and the parser received
// "presentation is a" as a statement. The order is the whole behaviour.

import (
	"strings"
	"testing"
)

func TestSplitSQLStatementsIgnoresSemicolonsInComments(t *testing.T) {
	sql := `
-- Bronze stores the compact representation; presentation is a view.
-- Another; comment; with; several.
CREATE TABLE a (x INT);

-- trailing note; still a comment
CREATE VIEW b AS SELECT x FROM a;
`
	got := splitSQLStatements(sql)
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2:\n%#v", len(got), got)
	}
	if !strings.HasPrefix(got[0], "CREATE TABLE a") {
		t.Errorf("first statement is %q", got[0])
	}
	if !strings.HasPrefix(got[1], "CREATE VIEW b") {
		t.Errorf("second statement is %q", got[1])
	}
}

// The original reason the function strips line by line rather than skipping
// fragments that start with "--": a comment between two semicolons would
// otherwise swallow the statement that follows it.
func TestSplitSQLStatementsKeepsStatementsAfterComments(t *testing.T) {
	got := splitSQLStatements("CREATE TABLE a (x INT);\n-- Core tables\nCREATE TABLE b (y INT);")
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2: %#v", len(got), got)
	}
}

// Both real files must survive it, and yield one statement per object.
func TestSchemaAndViewFilesSplitIntoTheExpectedStatements(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want int
	}{
		{"tables", embeddedBronzeSchema, 21},
		// 21 views plus two COMMENT ON statements. STL-1 asks for the spec a
		// dataset implements to be named in its schema metadata, and a
		// comment on the view is where a consumer inspecting the lake will
		// actually find it.
		{"views", embeddedBronzeViews, 23},
	} {
		got := splitSQLStatements(tc.sql)
		if len(got) != tc.want {
			t.Errorf("%s: %d statements, want %d", tc.name, len(got), tc.want)
		}
		for _, stmt := range got {
			if strings.HasPrefix(stmt, "--") {
				t.Errorf("%s: a comment reached the parser as a statement: %q", tc.name, stmt)
			}
		}
	}
}

// A semicolon inside a string literal is not a statement terminator.
//
// The splitter already handled semicolons in comments. String literals are
// the other place prose appears in a schema file: COMMENT ON ... IS '...'
// carries the spec a dataset implements into the lake's own metadata
// (STL-1), and documentation has semicolons in it. Cut mid-literal, the
// parser sees an unterminated string and the whole file fails to apply.
func TestSemicolonsInsideStringLiteralsDoNotSplit(t *testing.T) {
	sql := `CREATE VIEW v AS SELECT 1;
COMMENT ON VIEW v IS 'first clause; second clause; third';
CREATE VIEW w AS SELECT 2;`

	got := splitSQLStatements(sql)
	if len(got) != 3 {
		t.Fatalf("got %d statements, want 3: %#v", len(got), got)
	}
	if !strings.Contains(got[1], "third") {
		t.Errorf("the comment was cut short: %q", got[1])
	}
}

// SQL escapes a quote by doubling it. The splitter needs no special case --
// the first closes the literal and the second reopens it -- but that is a
// property worth pinning rather than rediscovering.
func TestDoubledQuotesKeepTheSplitterInSync(t *testing.T) {
	sql := `COMMENT ON VIEW v IS 'it''s fine; really';
CREATE VIEW w AS SELECT 2;`

	got := splitSQLStatements(sql)
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2: %#v", len(got), got)
	}
}
