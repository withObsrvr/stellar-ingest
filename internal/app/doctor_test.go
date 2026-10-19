package app

// The property that matters most here is the one that is invisible when it
// breaks: §5 says "Avoid secrets in process arguments, logs, event payloads,
// or JSON inspect output", and a doctor is exactly the thing an operator
// pastes into a support ticket. Echoing a DSN to prove it was read would put
// the catalog password in every one of them.

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/logging"
)

const (
	secretPassword = "hunter2-the-catalog-password"
	secretKeyID    = "0045bf11223344556677889"
	secretKey      = "K004zzzzzzzzzzzzzzzzzzzzzzzzzzz"
)

func secretRequest(outputDir string) RunRequest {
	return RunRequest{
		OutputDir:    outputDir,
		DlCatalog:    "postgresql://obsrvr:" + secretPassword + "@127.0.0.1:5432/catalog?sslmode=disable",
		DlDataPath:   "s3://bucket/",
		DlMetaSchema: "bronze_meta",
		B2KeyID:      secretKeyID,
		B2KeySecret:  secretKey,
		PgHost:       "db.example",
		PgPassword:   secretPassword,
		RunDuckLake:  true,
	}
}

// Every stream the doctor can write to, checked for every secret it was
// given. The catalog here does not answer, which is the interesting case:
// the failure path is where a DSN is most likely to be quoted verbatim.
func TestDoctorNeverPrintsASecret(t *testing.T) {
	var results, narration bytes.Buffer
	deps := Deps{
		Out: &results,
		Log: logging.New(logging.Options{Level: logging.LevelVerbose, Err: &narration}),
	}

	result, err := Doctor(context.Background(), secretRequest(t.TempDir()), deps)
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}

	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	surfaces := map[string]string{
		"results stream": results.String(),
		"narration":      narration.String(),
		"json payload":   string(encoded),
	}
	for _, secret := range []string{secretPassword, secretKeyID, secretKey} {
		for name, text := range surfaces {
			if strings.Contains(text, secret) {
				t.Errorf("%s contains a credential value; a doctor is what an operator pastes "+
					"into a support ticket", name)
			}
		}
	}
}

// The redaction must not be achieved by saying nothing: a doctor that
// reports no detail is safe and useless.
func TestDoctorStillIdentifiesTheCatalogItDialled(t *testing.T) {
	var narration bytes.Buffer
	deps := Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration})}

	result, _ := Doctor(context.Background(), secretRequest(t.TempDir()), deps)

	detail := checkDetail(t, result, "catalog")
	for _, want := range []string{"127.0.0.1", "5432", "catalog"} {
		if !strings.Contains(detail, want) {
			t.Errorf("catalog check does not mention %q, so it cannot be acted on: %q", want, detail)
		}
	}
	if !strings.Contains(detail, "***") {
		t.Errorf("catalog check does not show a redaction marker: %q", detail)
	}
}

// Credential presence must be reported, because "did it resolve" is the
// question the doctor exists to answer.
func TestDoctorReportsCredentialPresenceWithoutContent(t *testing.T) {
	var narration bytes.Buffer
	result, _ := Doctor(context.Background(), secretRequest(t.TempDir()),
		Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration})})

	detail := checkDetail(t, result, "credential.b2-key-secret")
	if !strings.Contains(detail, "set") {
		t.Errorf("credential presence not reported: %q", detail)
	}
	if !strings.Contains(detail, "31") {
		t.Errorf("credential length not reported, so an empty file cannot be told from a real "+
			"secret: %q", detail)
	}
}

// Reporting a missing B2 key as a failure on an extract-only machine trains
// an operator to ignore the output.
func TestMissingCredentialsAreWarningsUntilTheyAreNeeded(t *testing.T) {
	var narration bytes.Buffer
	deps := Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration})}

	extractOnly, _ := Doctor(context.Background(), RunRequest{OutputDir: t.TempDir()}, deps)
	if got := statusOf(t, extractOnly, "credential.b2-key-id"); got != StatusWarn {
		t.Errorf("extract-only: b2-key-id is %v, want %v", got, StatusWarn)
	}
	if extractOnly.Failed != 0 {
		t.Errorf("extract-only machine reported %d failures; nothing it needs is missing", extractOnly.Failed)
	}

	pushing, _ := Doctor(context.Background(), RunRequest{OutputDir: t.TempDir(), RunDuckLake: true}, deps)
	if got := statusOf(t, pushing, "credential.b2-key-id"); got != StatusFail {
		t.Errorf("with --ducklake: b2-key-id is %v, want %v", got, StatusFail)
	}
	if pushing.Failed == 0 {
		t.Error("a push configured with no credentials reported no failures")
	}
}

// An unwritable output directory fails after the archive is open, which on a
// large chunk is a long way in. The doctor exists to find it first.
func TestDoctorCatchesAnUnwritableOutputDirectory(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	var narration bytes.Buffer
	result, _ := Doctor(context.Background(), RunRequest{OutputDir: filepath.Join(parent, "out")},
		Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration})})

	if got := statusOf(t, result, "output-dir"); got != StatusFail {
		t.Errorf("output-dir is %v, want %v", got, StatusFail)
	}
}

// A directory that exists but cannot be written is a different code path
// from one that cannot be created, and it is the one a read-only mount
// produces. Without this case the write probe is never exercised and a
// mutation that reports it as ok survives.
func TestDoctorCatchesAnExistingButUnwritableOutputDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not apply")
	}
	dir := filepath.Join(t.TempDir(), "readonly")
	if err := os.Mkdir(dir, 0o555); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	var narration bytes.Buffer
	result, _ := Doctor(context.Background(), RunRequest{OutputDir: dir},
		Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration})})

	if got := statusOf(t, result, "output-dir"); got != StatusFail {
		t.Errorf("output-dir is %v, want %v; a read-only directory fails at the first shard "+
			"write, which on a large chunk is a long way in", got, StatusFail)
	}
	if detail := checkDetail(t, result, "output-dir"); !strings.Contains(detail, "cannot write") {
		t.Errorf("detail does not say the directory is unwritable: %q", detail)
	}
}

// DuckDB and its extensions are what a push needs; a binary without them
// fails at the first push rather than at startup.
func TestDoctorChecksDuckDBAndItsExtensions(t *testing.T) {
	var narration bytes.Buffer
	result, _ := Doctor(context.Background(), RunRequest{OutputDir: t.TempDir()},
		Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration})})

	for _, name := range []string{"duckdb", "duckdb.timezone", "duckdb.extension.ducklake", "duckdb.extension.httpfs"} {
		if got := statusOf(t, result, name); got != StatusOK {
			t.Errorf("%s is %v, want %v; this environment runs the test suite, so it should pass",
				name, got, StatusOK)
		}
	}
}

// The counts let a caller branch without walking the list, so they must
// agree with the list.
func TestDoctorCountsMatchTheChecks(t *testing.T) {
	var narration bytes.Buffer
	result, _ := Doctor(context.Background(), secretRequest(t.TempDir()),
		Deps{Log: logging.New(logging.Options{Level: logging.LevelNormal, Err: &narration})})

	failed, warned := 0, 0
	for _, check := range result.Checks {
		switch check.Status {
		case StatusFail:
			failed++
		case StatusWarn:
			warned++
		}
	}
	if result.Failed != failed {
		t.Errorf("Failed = %d, but %d checks are at fail", result.Failed, failed)
	}
	if result.Warned != warned {
		t.Errorf("Warned = %d, but %d checks are at warn", result.Warned, warned)
	}
}

func checkDetail(t *testing.T, result *DoctorResult, name string) string {
	t.Helper()
	for _, check := range result.Checks {
		if check.Name == name {
			return check.Detail
		}
	}
	t.Fatalf("no check named %q; the doctor ran %d checks", name, len(result.Checks))
	return ""
}

func statusOf(t *testing.T, result *DoctorResult, name string) CheckStatus {
	t.Helper()
	for _, check := range result.Checks {
		if check.Name == name {
			return check.Status
		}
	}
	t.Fatalf("no check named %q", name)
	return ""
}
