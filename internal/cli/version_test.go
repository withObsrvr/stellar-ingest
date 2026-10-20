package cli

// `version` is a contract: a release pipeline reads it, and CLI-13 asks for
// the semantic version, the commit and the build date, with a machine
// readable form.
//
// The split between semver and commit is the part worth pinning. Manifests,
// lineage rows and checkpoints record the COMMIT, because two builds of
// v0.1.0 can decode differently; reporting only the release would make a
// provenance record ambiguous, which is the gap the cold-authority work had
// to untangle after the fact.

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

func runVersion(t *testing.T, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	cmd := newVersionCommand(&out)
	cmd.SetArgs(args)
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("version %v: %v", args, err)
	}
	return out.String()
}

func TestVersionJSONCarriesSchemaSemverAndCommit(t *testing.T) {
	var got struct {
		Schema  string `json:"schema"`
		SemVer  string `json:"semver"`
		Commit  string `json:"commit"`
		BuiltAt string `json:"built_at"`
	}
	raw := runVersion(t, "--output", "json")
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("not one JSON document: %v\n%s", err, raw)
	}

	// CLI-6: every machine-readable output names its schema.
	if got.Schema != SchemaVersion {
		t.Errorf("schema = %q, want %q", got.Schema, SchemaVersion)
	}
	if got.SemVer != domain.SemVer {
		t.Errorf("semver = %q, want %q", got.SemVer, domain.SemVer)
	}
	if got.Commit != domain.Version {
		t.Errorf("commit = %q, want the build stamp %q", got.Commit, domain.Version)
	}
	// One document, not a stream: exactly one newline, at the end.
	if strings.Count(strings.TrimRight(raw, "\n"), "\n") != 0 {
		t.Errorf("--output json emitted more than one line:\n%s", raw)
	}
}

// The release and the build are different questions, and the human form has
// to answer both -- otherwise "which version is deployed" and "which commit
// produced this manifest" have the same answer, and only one of them is
// right.
func TestVersionTableReportsBothReleaseAndCommit(t *testing.T) {
	out := runVersion(t)
	if !strings.Contains(out, domain.SemVer) {
		t.Errorf("table output omits the release %q:\n%s", domain.SemVer, out)
	}
	if !strings.Contains(out, domain.Version) {
		t.Errorf("table output omits the commit %q:\n%s", domain.Version, out)
	}
}

// SemVer 2.0.0 (VER-2). A release pipeline sorts on this.
func TestSemVerIsAValidSemanticVersion(t *testing.T) {
	parts := strings.SplitN(strings.TrimPrefix(domain.SemVer, "v"), ".", 3)
	if len(parts) != 3 {
		t.Fatalf("SemVer %q is not MAJOR.MINOR.PATCH", domain.SemVer)
	}
	for _, p := range parts {
		if p == "" || strings.TrimLeft(p, "0123456789") != "" {
			// Pre-release and build metadata are allowed on PATCH only.
			if base := strings.SplitN(p, "-", 2)[0]; base == "" ||
				strings.TrimLeft(base, "0123456789") != "" {
				t.Errorf("SemVer %q has a non-numeric component %q", domain.SemVer, p)
			}
		}
	}
	if strings.HasPrefix(domain.SemVer, "v") {
		t.Errorf("SemVer %q carries a leading v; the tag does, the value does not", domain.SemVer)
	}
}

// An unknown format must be a usage error, not a silent fallback to the
// human table -- a pipeline asking for a format it misspelled would parse
// the table as data.
func TestVersionRejectsAnUnknownFormat(t *testing.T) {
	var out bytes.Buffer
	cmd := newVersionCommand(&out)
	cmd.SetArgs([]string{"--output", "yaml"})
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	if err := cmd.Execute(); err == nil {
		t.Error("--output yaml was accepted")
	}
}
