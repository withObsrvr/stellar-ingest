package main

// The resolver is tested in internal/credential with an injected
// environment. This covers the wiring: that main actually reads the real
// environment, under the names documented in the README, and warns only for
// the credentials that arrived on the command line.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/credential"
)

func TestResolveCredentialsReadsTheEnvironment(t *testing.T) {
	t.Setenv("DUCKLAKE_CATALOG_DSN", "postgresql://u:p@h/d")
	t.Setenv("B2_KEY_ID", "key-id")
	t.Setenv("B2_KEY_SECRET", "key-secret")
	t.Setenv("PG_PASSWORD", "pg-pass")

	got, err := resolveCredentials("", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		r    credential.Resolution
		want string
	}{
		{"catalog DSN", got.CatalogDSN, "postgresql://u:p@h/d"},
		{"B2 key id", got.B2KeyID, "key-id"},
		{"B2 key secret", got.B2KeySecret, "key-secret"},
		{"PG password", got.PGPassword, "pg-pass"},
	} {
		if tc.r.Value != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.r.Value, tc.want)
		}
		if tc.r.Origin != credential.FromEnv {
			t.Errorf("%s came from %v, want environment", tc.name, tc.r.Origin)
		}
	}
	if n := got.Notices(); len(n) != 0 {
		t.Errorf("credentials from the environment produced warnings: %v", n)
	}
}

func TestResolveCredentialsReadsFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "b2")
	if err := os.WriteFile(path, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("B2_KEY_SECRET_FILE", path)
	t.Setenv("B2_KEY_SECRET", "from-env-should-lose")

	got, err := resolveCredentials("", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.B2KeySecret.Value != "from-file" {
		t.Errorf("value = %q, want from-file (the file form wins, and the newline is trimmed)",
			got.B2KeySecret.Value)
	}
	if got.B2KeySecret.Origin != credential.FromFile {
		t.Errorf("origin = %v, want file", got.B2KeySecret.Origin)
	}
}

// One warning per credential that came from argv, and no secret in any of
// them. This is the check that keeps the fix from undoing itself.
func TestNoticesWarnOnlyForFlagsAndNeverEchoTheSecret(t *testing.T) {
	const flagSecret = "SUPERSECRET-ARGV"
	t.Setenv("B2_KEY_ID", "safe-from-env")

	got, err := resolveCredentials("", "", flagSecret, "")
	if err != nil {
		t.Fatal(err)
	}

	notices := got.Notices()
	if len(notices) != 1 {
		t.Fatalf("got %d warnings, want 1 (only --b2-key-secret came from argv): %v", len(notices), notices)
	}
	if !strings.Contains(notices[0], "--b2-key-secret") {
		t.Errorf("the warning does not name the flag: %s", notices[0])
	}
	for _, n := range notices {
		if strings.Contains(n, flagSecret) {
			t.Errorf("a warning echoes the secret: %s", n)
		}
	}
}

// Every credential this tool takes must have an environment route. A flag
// without one leaves argv as the only way to pass it, which is the gap being
// closed.
func TestEveryCredentialHasAnEnvironmentRoute(t *testing.T) {
	for _, spec := range []credential.Spec{
		credentialSpecs.CatalogDSN,
		credentialSpecs.B2KeyID,
		credentialSpecs.B2KeySecret,
		credentialSpecs.PGPassword,
	} {
		if spec.EnvVar == "" {
			t.Errorf("--%s has no environment variable", spec.Flag)
		}
		if spec.Flag == "" {
			t.Errorf("%s has no flag name, so its warning cannot name one", spec.EnvVar)
		}
	}
}
