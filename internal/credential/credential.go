// Package credential resolves a secret from a flag, a file or the
// environment, and reports which of those it came from.
//
// The Obsrvr CLI Engineering Standard prohibits secrets in process arguments
// outright, and this tool has passed four of them that way: the B2 key and
// secret, the PostgreSQL password, and the catalog DSN, which contains a
// password of its own. An argument is visible in `ps` to every user on the
// box, lands in shell history, and is inherited by child processes -- this
// loader re-invokes itself per chunk, so a secret on the parent's command line
// is on every child's as well.
//
// Resolution order is flag, then file, then environment. The flag wins when
// it is set, because silently preferring the environment over something the
// caller typed is its own surprise; it simply warns. The file form is for
// Nomad, Kubernetes and Docker, which all mount secrets as files, and is
// preferred over the plain environment variable because an environment is
// readable from a process listing on some systems and inherited everywhere.
package credential

import (
	"fmt"
	"strings"
)

// Origin says where a resolved value came from.
type Origin int

const (
	// Unset means no source supplied a value.
	Unset Origin = iota
	// FromFlag means a command-line argument, which is the deprecated path.
	FromFlag
	// FromFile means the contents of the file named by the *_FILE variable.
	FromFile
	// FromEnv means the environment variable itself.
	FromEnv
)

// String names the origin for a diagnostic. Never includes a value.
func (o Origin) String() string {
	switch o {
	case FromFlag:
		return "flag"
	case FromFile:
		return "file"
	case FromEnv:
		return "environment"
	default:
		return "unset"
	}
}

// Spec describes one credential and where it may come from.
type Spec struct {
	// Flag is the command-line flag name, without dashes, used in messages.
	Flag string
	// EnvVar is the environment variable holding the value.
	EnvVar string
	// FileEnvVar is the environment variable holding a path to read the value
	// from. Defaults to EnvVar + "_FILE".
	FileEnvVar string
}

// fileEnvVar is FileEnvVar, or the conventional EnvVar_FILE.
func (s Spec) fileEnvVar() string {
	if s.FileEnvVar != "" {
		return s.FileEnvVar
	}
	return s.EnvVar + "_FILE"
}

// Resolution is a resolved credential.
type Resolution struct {
	Value  string
	Origin Origin
	Spec   Spec
}

// Environment is the environment a resolution reads from. Injected so a test
// needs neither a real environment nor a real file.
type Environment struct {
	Getenv   func(string) string
	ReadFile func(string) ([]byte, error)
}

// Resolve works out the value for one credential.
//
// A file that is named but unreadable is an error rather than a fallthrough:
// silently continuing to the environment would turn a misconfigured secret
// mount into an authentication failure somewhere further away, which is
// harder to diagnose than the mount itself.
func Resolve(spec Spec, flagValue string, env Environment) (Resolution, error) {
	if flagValue != "" {
		return Resolution{Value: flagValue, Origin: FromFlag, Spec: spec}, nil
	}

	if path := env.Getenv(spec.fileEnvVar()); path != "" {
		raw, err := env.ReadFile(path)
		if err != nil {
			return Resolution{Spec: spec}, fmt.Errorf("%s names %s, which could not be read: %w",
				spec.fileEnvVar(), path, err)
		}
		// Trailing newline is how every editor and `echo` writes a file, and
		// a credential with one appended fails authentication for a reason
		// nobody can see.
		value := strings.TrimRight(string(raw), "\r\n")
		if value == "" {
			return Resolution{Spec: spec}, fmt.Errorf("%s names %s, which is empty",
				spec.fileEnvVar(), path)
		}
		return Resolution{Value: value, Origin: FromFile, Spec: spec}, nil
	}

	if value := env.Getenv(spec.EnvVar); value != "" {
		return Resolution{Value: value, Origin: FromEnv, Spec: spec}, nil
	}

	return Resolution{Spec: spec}, nil
}

// DeprecationNotice is what to say when a credential arrived on the command
// line, and the empty string otherwise.
//
// Names the flag and the alternatives, never the value. A warning that echoed
// the secret would put it in exactly the logs this change exists to keep it
// out of.
func (r Resolution) DeprecationNotice() string {
	if r.Origin != FromFlag {
		return ""
	}
	return fmt.Sprintf(
		"--%s passes a credential as a command-line argument, which is visible in `ps`, "+
			"recorded in shell history, and inherited by child processes. Set %s, or %s with a "+
			"path to read it from, instead. The flag is accepted for now.",
		r.Spec.Flag, r.Spec.EnvVar, r.fileEnvVar())
}

// fileEnvVar forwards to the Spec so a caller holding only a Resolution can
// build a message without carrying the Spec separately.
func (r Resolution) fileEnvVar() string { return r.Spec.fileEnvVar() }

// RedactDSN replaces the password in a PostgreSQL URL with a placeholder, so
// a DSN can appear in a diagnostic without its secret.
//
// Only the URL form carries a password inline. A key=value DSN is returned
// unchanged apart from a password= field, which is handled the same way.
func RedactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}

	// URL form: scheme://user:password@host/...
	if scheme := strings.Index(dsn, "://"); scheme >= 0 {
		rest := dsn[scheme+3:]
		at := strings.LastIndex(rest, "@")
		if at < 0 {
			return dsn
		}
		userinfo := rest[:at]
		if colon := strings.Index(userinfo, ":"); colon >= 0 {
			return dsn[:scheme+3] + userinfo[:colon] + ":***" + rest[at:]
		}
		return dsn
	}

	// key=value form.
	parts := strings.Fields(dsn)
	for i, part := range parts {
		if strings.HasPrefix(part, "password=") {
			parts[i] = "password=***"
		}
	}
	return strings.Join(parts, " ")
}
