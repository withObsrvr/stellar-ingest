package app

// These tests are the point of the change.
//
// While Run called log.Fatal there was no way to assert any of this: a
// function that terminates the process cannot be driven by a test, so the 20
// input rules below were only ever checked by running the binary and reading
// the message. §10's first release gate is "core business logic testable
// without terminal or network transport"; every case here runs in-process
// with no catalog, no bucket and no archive.
//
// Each case must also return BEFORE doing any work. A rule that rejects bad
// input only after creating directories and dialling a catalog is a rule that
// costs money to enforce.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/exit"
)

// valid is a request that passes every input rule, so a case can change one
// field and know the rejection came from that field.
func valid(outputDir string) RunRequest {
	return RunRequest{
		Start:             1,
		End:               10,
		Workers:           1,
		BatchSize:         1,
		ChunkConcurrency:  1,
		OutputDir:         outputDir,
		StorageType:       "FS",
		Bucket:            "testdata",
		NetworkPassphrase: "Test SDF Network ; September 2015",
	}
}

func TestInvalidInputIsRejectedAsUsage(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*RunRequest)
		mentions string
	}{
		{"no start", func(r *RunRequest) { r.Start = 0 }, "--start"},
		{"no end", func(r *RunRequest) { r.End = 0 }, "--end"},
		{"no output dir", func(r *RunRequest) { r.OutputDir = "" }, "--output-dir"},
		{"no source and no ducklake", func(r *RunRequest) { r.Bucket = ""; r.StorageType = "FS" }, "--bucket"},
		{"zero workers", func(r *RunRequest) { r.Workers = 0 }, "--workers"},
		{"zero batch size", func(r *RunRequest) { r.BatchSize = 0 }, "--batch-size"},
		{"zero chunk concurrency", func(r *RunRequest) { r.ChunkConcurrency = 0 }, "--chunk-concurrency"},
		{"concurrency without chunks", func(r *RunRequest) { r.ChunkConcurrency = 2 }, "--chunk-size"},
		{"concurrency without range state", func(r *RunRequest) {
			r.ChunkConcurrency = 2
			r.ChunkSize = 5
		}, "--range-state"},
		{"chunk size with hot buffer", func(r *RunRequest) {
			r.ChunkSize = 5
			r.PgHost = "db.example"
		}, "--pg-host"},
		{"unknown storage type", func(r *RunRequest) { r.StorageType = "FTP" }, "--storage-type"},
		{"unknown table in only-tables", func(r *RunRequest) { r.OnlyTables = "no_such_table" }, "only-tables"},
		{"range state without catalog", func(r *RunRequest) {
			r.RangeState = true
			r.RunDuckLake = true
		}, "DUCKLAKE_CATALOG_DSN"},
		{"plan without catalog", func(r *RunRequest) { r.PlanOnly = true }, "--plan"},
		{"ducklake without credentials", func(r *RunRequest) {
			r.Bucket = ""
			r.RunDuckLake = true
		}, "DUCKLAKE_CATALOG_DSN"},
		{"enrichment without range", func(r *RunRequest) {
			r.EnrichContractBalances = true
			r.Start = 0
		}, "--enrich-contract-balances"},
		{"enrichment without chunk size", func(r *RunRequest) {
			r.EnrichContractBalances = true
			r.EnrichmentChunkSize = 0
		}, "--enrichment-chunk-size"},
		{"enrichment without run id", func(r *RunRequest) {
			r.EnrichContractBalances = true
			r.EnrichmentChunkSize = 100
			r.DlCatalog = "postgres://x"
			r.DlDataPath = "s3://x/"
			r.B2KeyID = "k"
			r.B2KeySecret = "s"
		}, "--enrichment-run-id"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// A directory that does not exist yet, so creating it is
			// observable. Nothing valid should be created by a rejected run.
			dir := filepath.Join(t.TempDir(), "out")
			req := valid(dir)
			c.mutate(&req)

			_, err := Run(context.Background(), req, Deps{})
			if err == nil {
				t.Fatal("accepted invalid input")
			}
			if got := exit.ClassOf(err); got != exit.Usage {
				t.Errorf("exit class = %v, want %v (a scheduler must not retry invalid input); error was: %v",
					got, exit.Usage, err)
			}
			if !strings.Contains(err.Error(), c.mentions) {
				t.Errorf("message %q does not mention %q, so it does not say what to fix",
					err.Error(), c.mentions)
			}
			if _, statErr := os.Stat(dir); statErr == nil {
				t.Errorf("rejected run created %s; input rules must be enforced before doing work", dir)
			}
		})
	}
}

// A negative control. Without it every assertion above would pass on a Run
// that returned exit.Usagef unconditionally.
func TestNonInputFailureIsNotUsage(t *testing.T) {
	// A regular file standing where the output directory's parent must be,
	// so MkdirAll fails for a reason that is not the caller's input.
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	req := valid(filepath.Join(parent, "out"))

	_, err := Run(context.Background(), req, Deps{})
	if err == nil {
		t.Fatal("expected the unwritable output directory to fail")
	}
	if got := exit.ClassOf(err); got == exit.Usage {
		t.Errorf("exit class = %v; a filesystem failure is not invalid input, and if every "+
			"failure classifies as Usage the classes carry no information", got)
	}
}

// The old message told the operator to pass --ducklake-catalog, --b2-key-id
// and --b2-key-secret. Those flags are deprecated precisely because they put
// secrets in argv, so following the error moved you into the state the tool
// warns about. §5: "Avoid secrets in process arguments."
func TestCredentialErrorNamesEnvironmentNotDeprecatedFlags(t *testing.T) {
	err := requireDuckLakeCredentials(RunRequest{})
	if err == nil {
		t.Fatal("expected missing credentials to be reported")
	}
	message := err.Error()

	for _, want := range []string{"DUCKLAKE_CATALOG_DSN", "B2_KEY_ID", "B2_KEY_SECRET"} {
		if !strings.Contains(message, want) {
			t.Errorf("message does not name %s: %q", want, message)
		}
	}
	for _, forbidden := range []string{"--ducklake-catalog", "--b2-key-id", "--b2-key-secret"} {
		if strings.Contains(message, forbidden) {
			t.Errorf("message tells the operator to use %s, which is deprecated for putting a "+
				"secret in argv: %q", forbidden, message)
		}
	}
	if got := exit.ClassOf(err); got != exit.Usage {
		t.Errorf("exit class = %v, want %v", got, exit.Usage)
	}
}

// Only what is actually missing should be named, or the operator chases
// credentials that are already set.
func TestCredentialErrorNamesOnlyWhatIsMissing(t *testing.T) {
	req := RunRequest{DlCatalog: "postgres://x", DlDataPath: "s3://b/", B2KeyID: "k"}
	err := requireDuckLakeCredentials(req)
	if err == nil {
		t.Fatal("expected the missing secret to be reported")
	}
	if !strings.Contains(err.Error(), "B2_KEY_SECRET") {
		t.Errorf("missing credential not named: %q", err.Error())
	}
	for _, present := range []string{"DUCKLAKE_CATALOG_DSN", "B2_KEY_ID", "--ducklake-data-path"} {
		if strings.Contains(err.Error(), present) {
			t.Errorf("message names %s, which is set: %q", present, err.Error())
		}
	}
}

func TestCredentialErrorIsNilWhenAllPresent(t *testing.T) {
	req := RunRequest{DlCatalog: "postgres://x", DlDataPath: "s3://b/", B2KeyID: "k", B2KeySecret: "s"}
	if err := requireDuckLakeCredentials(req); err != nil {
		t.Errorf("complete credentials rejected: %v", err)
	}
}
