package cli

// Credential and status-reporting setup: the parts of starting a run that are
// about the command line rather than about ingestion.
//
// They live here because §2 of the CLI standard puts flags, parsing and
// rendering in the adapter and keeps business rules out of it. Resolving
// where a secret comes from is an adapter concern; what the engine does with
// it is not.

import (
	"fmt"
	"os"

	"github.com/withObsrvr/stellar-ingest/internal/credential"
	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// NewStatusEmitter builds the OSC 7501 emitter from the --program-status mode.
//
// auto is the default and reports only when stderr is a terminal: the escape
// sequence is meaningful to a terminal or a supervising tool and is noise in a
// log file or a pipe. always is for a consumer that reads a captured stream;
// never is for anyone who wants the stream clean.
//
// Reports go to stderr rather than stdout because stdout carries results, and
// an escape sequence written into a result stream corrupts it.
func NewStatusEmitter(mode string) (*status.Emitter, error) {
	var enabled bool
	switch mode {
	case "auto":
		enabled = status.StderrIsTerminal()
	case "always":
		enabled = true
	case "never":
		enabled = false
	default:
		return nil, fmt.Errorf("--program-status must be auto, always or never, got %q", mode)
	}
	return status.NewEmitter(os.Stderr, "stellar-ingest", enabled), nil
}

// CredentialSet is every credential this run needs, resolved.
type CredentialSet struct {
	CatalogDSN  credential.Resolution
	B2KeyID     credential.Resolution
	B2KeySecret credential.Resolution
	PGPassword  credential.Resolution
}

// CredentialSpecs names the four credentials and the variables that supply
// them. Unprefixed SCREAMING_SNAKE matches the house convention already used
// by RPC_AUTH_HEADER, S3_ENDPOINT_URL and STELLAR_CORE_BINARY_PATH.
//
// DUCKLAKE_CATALOG_DSN rather than DUCKLAKE_CATALOG because that is the name
// the README's own examples already pass to --ducklake-catalog.
var CredentialSpecs = struct {
	CatalogDSN  credential.Spec
	B2KeyID     credential.Spec
	B2KeySecret credential.Spec
	PGPassword  credential.Spec
}{
	CatalogDSN:  credential.Spec{Flag: "ducklake-catalog", EnvVar: "DUCKLAKE_CATALOG_DSN"},
	B2KeyID:     credential.Spec{Flag: "b2-key-id", EnvVar: "B2_KEY_ID"},
	B2KeySecret: credential.Spec{Flag: "b2-key-secret", EnvVar: "B2_KEY_SECRET"},
	PGPassword:  credential.Spec{Flag: "pg-password", EnvVar: "PG_PASSWORD"},
}

// ResolveCredentials resolves all four, from flag, file or environment.
func ResolveCredentials(catalogDSN, b2KeyID, b2KeySecret, pgPassword string) (CredentialSet, error) {
	env := credential.Environment{Getenv: os.Getenv, ReadFile: os.ReadFile}

	var set CredentialSet
	for _, item := range []struct {
		spec  credential.Spec
		value string
		into  *credential.Resolution
	}{
		{CredentialSpecs.CatalogDSN, catalogDSN, &set.CatalogDSN},
		{CredentialSpecs.B2KeyID, b2KeyID, &set.B2KeyID},
		{CredentialSpecs.B2KeySecret, b2KeySecret, &set.B2KeySecret},
		{CredentialSpecs.PGPassword, pgPassword, &set.PGPassword},
	} {
		resolved, err := credential.Resolve(item.spec, item.value, env)
		if err != nil {
			return CredentialSet{}, err
		}
		*item.into = resolved
	}
	return set, nil
}

// Notices lists the warnings for credentials that arrived on the command
// line, in a stable order so output is comparable.
func (c CredentialSet) Notices() []string {
	var out []string
	for _, r := range []credential.Resolution{c.CatalogDSN, c.B2KeyID, c.B2KeySecret, c.PGPassword} {
		if notice := r.DeprecationNotice(); notice != "" {
			out = append(out, notice)
		}
	}
	return out
}
