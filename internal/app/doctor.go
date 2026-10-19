package app

// doctor: the diagnostic §3 asks for under Discoverability -- "--help with
// examples, grouped commands, completion, version, and doctor/config
// diagnostics".
//
// It answers the question an operator actually has before dispatching a
// multi-day backfill: is this machine configured to do the thing I am about
// to ask it to do. Every failure it reports is one that would otherwise
// surface an hour into a chunk -- an unreadable credential file, a catalog
// that does not resolve, a metadata schema that is not there.
//
// IT NEVER PRINTS A CREDENTIAL VALUE. §5: "Avoid secrets in process
// arguments, logs, event payloads, or JSON inspect output." A doctor that
// echoes a DSN to prove it was read is a doctor that puts the catalog
// password into every support ticket, so it reports presence and source and
// redacts the DSN it dials. A test holds it to that.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/withObsrvr/stellar-ingest/internal/adapters"
	"github.com/withObsrvr/stellar-ingest/internal/credential"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

// SchemaDoctor identifies the doctor-result schema.
const SchemaDoctor = "stellar-ingest.doctor/v1"

// CheckStatus is the outcome of one diagnostic.
type CheckStatus string

const (
	// StatusOK means the check passed.
	StatusOK CheckStatus = "ok"
	// StatusWarn means something is missing but the run may not need it.
	// Credentials for a push nobody asked for are the common case.
	StatusWarn CheckStatus = "warn"
	// StatusFail means a run configured this way will not work.
	StatusFail CheckStatus = "fail"
	// StatusSkipped means the check did not apply.
	StatusSkipped CheckStatus = "skipped"
)

// DoctorCheck is one diagnostic and what it found.
type DoctorCheck struct {
	Name   string      `json:"name"`
	Status CheckStatus `json:"status"`
	Detail string      `json:"detail"`
}

// DoctorResult is the whole diagnostic.
type DoctorResult struct {
	Schema  string        `json:"schema"`
	Version string        `json:"version"`
	Checks  []DoctorCheck `json:"checks"`
	// Failed counts checks at StatusFail, so a caller can branch without
	// walking the list.
	Failed int `json:"failed"`
	Warned int `json:"warned"`
}

// Doctor runs the diagnostics.
//
// It returns a result and no error for a machine that is merely
// misconfigured: "your catalog is unreachable" is the answer to the
// question, not a failure to answer it. The command decides the exit code
// from Failed.
func Doctor(ctx context.Context, req RunRequest, deps Deps) (*DoctorResult, error) {
	log := deps.log()
	result := &DoctorResult{Schema: SchemaDoctor, Version: domain.Version}

	add := func(name string, status CheckStatus, format string, args ...any) {
		check := DoctorCheck{Name: name, Status: status, Detail: fmt.Sprintf(format, args...)}
		result.Checks = append(result.Checks, check)
		switch status {
		case StatusFail:
			result.Failed++
			log.Warn("%-28s %s: %s", check.Name, check.Status, check.Detail)
		case StatusWarn:
			result.Warned++
			log.Info("%-28s %s: %s", check.Name, check.Status, check.Detail)
		default:
			log.Info("%-28s %s: %s", check.Name, check.Status, check.Detail)
		}
	}

	log.Section("Diagnostics")
	add("build", StatusOK, "version %s", domain.Version)

	checkCredentials(req, add)
	checkOutputDir(req, add)
	checkDuckDB(ctx, add)
	checkCatalog(ctx, req, add)

	return result, nil
}

// checkCredentials reports presence, never value.
func checkCredentials(req RunRequest, add func(string, CheckStatus, string, ...any)) {
	// Needed is what this configuration will actually use. Reporting a
	// missing B2 key as a failure on an extract-only machine would train an
	// operator to ignore the output.
	needed := req.RunDuckLake || req.EnrichContractBalances

	for _, c := range []struct {
		name  string
		value string
		env   string
	}{
		{"credential.catalog", req.DlCatalog, "DUCKLAKE_CATALOG_DSN"},
		{"credential.b2-key-id", req.B2KeyID, "B2_KEY_ID"},
		{"credential.b2-key-secret", req.B2KeySecret, "B2_KEY_SECRET"},
	} {
		switch {
		case c.value != "":
			// Length, not content. Enough to tell an empty file from a real
			// secret without putting the secret anywhere.
			add(c.name, StatusOK, "set (%d characters)", len(c.value))
		case needed:
			add(c.name, StatusFail, "not set; export %s or %s_FILE", c.env, c.env)
		default:
			add(c.name, StatusWarn, "not set; needed only with --ducklake or --enrich-contract-balances")
		}
	}

	if req.PgHost != "" && req.PgPassword == "" {
		add("credential.pg-password", StatusFail, "--pg-host is set but no password; export PG_PASSWORD or PG_PASSWORD_FILE")
	} else if req.PgHost != "" {
		add("credential.pg-password", StatusOK, "set (%d characters)", len(req.PgPassword))
	} else {
		add("credential.pg-password", StatusSkipped, "no --pg-host, so no hot buffer")
	}
}

// checkOutputDir reports whether the run can actually write where it was
// told to. An unwritable output directory fails after the archive has been
// opened, which on a large chunk is a long way in.
func checkOutputDir(req RunRequest, add func(string, CheckStatus, string, ...any)) {
	if req.OutputDir == "" {
		add("output-dir", StatusWarn, "not set; required for extract, load and run")
		return
	}
	if err := os.MkdirAll(req.OutputDir, 0o755); err != nil {
		add("output-dir", StatusFail, "cannot create %s: %v", req.OutputDir, err)
		return
	}
	probe := filepath.Join(req.OutputDir, ".stellar-ingest-doctor")
	if err := os.WriteFile(probe, []byte("probe"), 0o600); err != nil {
		add("output-dir", StatusFail, "cannot write in %s: %v", req.OutputDir, err)
		return
	}
	_ = os.Remove(probe)
	add("output-dir", StatusOK, "writable: %s", req.OutputDir)
}

// checkDuckDB opens a session and loads the extensions a push needs. This is
// the check that catches a binary built without the DuckDB extensions
// available, which otherwise fails at the first push.
func checkDuckDB(ctx context.Context, add func(string, CheckStatus, string, ...any)) {
	db, err := adapters.OpenDuckDB()
	if err != nil {
		add("duckdb", StatusFail, "cannot open: %v", err)
		return
	}
	defer db.Close()

	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		add("duckdb", StatusFail, "cannot query: %v", err)
		return
	}
	add("duckdb", StatusOK, "%s", version)

	// The session timezone is pinned to UTC on purpose: DuckDB 2.0.0
	// defaults it to the host zone, which shifted every timestamp by the
	// host's offset and broke the goldens.
	var zone string
	if err := db.QueryRowContext(ctx, "SELECT current_setting('TimeZone')").Scan(&zone); err != nil {
		add("duckdb.timezone", StatusWarn, "cannot read: %v", err)
	} else if zone != "UTC" {
		add("duckdb.timezone", StatusFail, "session timezone is %s, want UTC; timestamps would shift by the host offset", zone)
	} else {
		add("duckdb.timezone", StatusOK, "UTC")
	}

	for _, extension := range []string{"ducklake", "httpfs"} {
		if _, err := db.ExecContext(ctx, "INSTALL "+extension); err != nil {
			add("duckdb.extension."+extension, StatusFail, "install: %v", err)
			continue
		}
		if _, err := db.ExecContext(ctx, "LOAD "+extension); err != nil {
			add("duckdb.extension."+extension, StatusFail, "load: %v", err)
			continue
		}
		add("duckdb.extension."+extension, StatusOK, "available")
	}
}

// checkCatalog dials the catalog and looks for the metadata schema.
func checkCatalog(ctx context.Context, req RunRequest, add func(string, CheckStatus, string, ...any)) {
	if req.DlCatalog == "" {
		add("catalog", StatusSkipped, "no catalog DSN set")
		return
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(dialCtx, req.DlCatalog)
	if err != nil {
		// Redacted: the DSN carries the password, and an error that quotes
		// it puts the catalog password in the operator's scrollback.
		add("catalog", StatusFail, "cannot configure %s: %v", credential.RedactDSN(req.DlCatalog), err)
		return
	}
	defer pool.Close()

	if err := pool.Ping(dialCtx); err != nil {
		add("catalog", StatusFail, "cannot reach %s: %v", credential.RedactDSN(req.DlCatalog), err)
		return
	}
	add("catalog", StatusOK, "reachable: %s", credential.RedactDSN(req.DlCatalog))

	var tables int
	err = pool.QueryRow(dialCtx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1`,
		req.DlMetaSchema).Scan(&tables)
	switch {
	case err != nil:
		add("catalog.metadata-schema", StatusWarn, "cannot inspect %s: %v", req.DlMetaSchema, err)
	case tables == 0:
		// Not a failure: the first push into a fresh catalog creates it.
		add("catalog.metadata-schema", StatusWarn,
			"%s has no tables; a first push will create them", req.DlMetaSchema)
	default:
		add("catalog.metadata-schema", StatusOK, "%s has %d tables", req.DlMetaSchema, tables)
	}
}
