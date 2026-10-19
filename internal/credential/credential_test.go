package credential

import (
	"fmt"
	"strings"
	"testing"
)

const secret = "s3cr3t-value"

func fakeEnv(vars map[string]string, files map[string]string) Environment {
	return Environment{
		Getenv: func(k string) string { return vars[k] },
		ReadFile: func(p string) ([]byte, error) {
			v, ok := files[p]
			if !ok {
				return nil, fmt.Errorf("no such file: %s", p)
			}
			return []byte(v), nil
		},
	}
}

var spec = Spec{Flag: "b2-key-secret", EnvVar: "B2_KEY_SECRET"}

func TestResolutionOrder(t *testing.T) {
	for _, tc := range []struct {
		name       string
		flag       string
		vars       map[string]string
		files      map[string]string
		wantValue  string
		wantOrigin Origin
	}{
		{
			name: "nothing set", wantOrigin: Unset,
		},
		{
			name:      "environment only",
			vars:      map[string]string{"B2_KEY_SECRET": secret},
			wantValue: secret, wantOrigin: FromEnv,
		},
		{
			name:      "file beats the plain environment variable",
			vars:      map[string]string{"B2_KEY_SECRET": "from-env", "B2_KEY_SECRET_FILE": "/run/secret"},
			files:     map[string]string{"/run/secret": secret},
			wantValue: secret, wantOrigin: FromFile,
		},
		{
			// The flag wins when set. Preferring the environment over
			// something the caller typed would be its own surprise; the
			// warning is how this gets discouraged.
			name:      "the flag wins over both",
			flag:      secret,
			vars:      map[string]string{"B2_KEY_SECRET": "from-env", "B2_KEY_SECRET_FILE": "/run/secret"},
			files:     map[string]string{"/run/secret": "from-file"},
			wantValue: secret, wantOrigin: FromFlag,
		},
	} {
		got, err := Resolve(spec, tc.flag, fakeEnv(tc.vars, tc.files))
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got.Value != tc.wantValue {
			t.Errorf("%s: value = %q, want %q", tc.name, got.Value, tc.wantValue)
		}
		if got.Origin != tc.wantOrigin {
			t.Errorf("%s: origin = %v, want %v", tc.name, got.Origin, tc.wantOrigin)
		}
	}
}

// A trailing newline is how every editor and `echo` writes a file. A
// credential with one appended fails authentication for a reason nobody can
// see in a log.
func TestFileValueIsTrimmedOfTrailingNewlines(t *testing.T) {
	for _, raw := range []string{secret, secret + "\n", secret + "\r\n", secret + "\n\n"} {
		got, err := Resolve(spec, "", fakeEnv(
			map[string]string{"B2_KEY_SECRET_FILE": "/run/s"},
			map[string]string{"/run/s": raw},
		))
		if err != nil {
			t.Fatal(err)
		}
		if got.Value != secret {
			t.Errorf("raw %q resolved to %q", raw, got.Value)
		}
	}
}

// A named file that cannot be read is an error, not a fallthrough. Falling
// through would turn a misconfigured secret mount into an authentication
// failure somewhere further away.
func TestUnreadableOrEmptyFileIsAnError(t *testing.T) {
	_, err := Resolve(spec, "", fakeEnv(
		map[string]string{"B2_KEY_SECRET_FILE": "/run/missing", "B2_KEY_SECRET": "would-have-worked"},
		nil,
	))
	if err == nil {
		t.Error("an unreadable secret file fell through to the environment")
	}

	_, err = Resolve(spec, "", fakeEnv(
		map[string]string{"B2_KEY_SECRET_FILE": "/run/empty"},
		map[string]string{"/run/empty": "\n"},
	))
	if err == nil {
		t.Error("an empty secret file was accepted")
	}
}

// The whole point of the warning is to get the secret off the command line,
// so the warning itself must never contain it.
func TestDeprecationNoticeNeverContainsTheValue(t *testing.T) {
	got, err := Resolve(spec, secret, fakeEnv(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	notice := got.DeprecationNotice()
	if notice == "" {
		t.Fatal("a credential from a flag produced no warning")
	}
	if strings.Contains(notice, secret) {
		t.Errorf("the warning echoes the secret: %s", notice)
	}
	for _, want := range []string{"--b2-key-secret", "B2_KEY_SECRET", "B2_KEY_SECRET_FILE", "ps"} {
		if !strings.Contains(notice, want) {
			t.Errorf("the warning does not mention %q: %s", want, notice)
		}
	}

	// And no warning when it came from a safe source.
	for _, env := range []Environment{
		fakeEnv(map[string]string{"B2_KEY_SECRET": secret}, nil),
		fakeEnv(map[string]string{"B2_KEY_SECRET_FILE": "/s"}, map[string]string{"/s": secret}),
	} {
		r, err := Resolve(spec, "", env)
		if err != nil {
			t.Fatal(err)
		}
		if r.DeprecationNotice() != "" {
			t.Errorf("origin %v produced a warning", r.Origin)
		}
	}
}

func TestRedactDSN(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{
			"url form",
			"postgresql://obsrvr:hunter2@127.0.0.1:5432/catalog?sslmode=disable",
			"postgresql://obsrvr:***@127.0.0.1:5432/catalog?sslmode=disable",
		},
		{
			"url with an @ in the password",
			"postgres://u:p@ss@host/db",
			"postgres://u:***@host/db",
		},
		{
			"url with no password",
			"postgresql://obsrvr@127.0.0.1:5432/catalog",
			"postgresql://obsrvr@127.0.0.1:5432/catalog",
		},
		{
			"key=value form",
			"host=127.0.0.1 user=obsrvr password=hunter2 dbname=catalog",
			"host=127.0.0.1 user=obsrvr password=*** dbname=catalog",
		},
		{"empty", "", ""},
		{"a bare path, which is a DuckDB file catalog", "/data/catalog.ducklake", "/data/catalog.ducklake"},
	} {
		if got := RedactDSN(tc.in); got != tc.want {
			t.Errorf("%s:\n got %q\nwant %q", tc.name, got, tc.want)
		}
	}
}

// Redaction has to actually remove it, whatever the shape.
func TestRedactDSNLeavesNoPassword(t *testing.T) {
	for _, dsn := range []string{
		"postgresql://u:hunter2@h/d",
		"postgres://u:hunter2@h:5432/d?sslmode=require",
		"host=h user=u password=hunter2 dbname=d",
	} {
		if strings.Contains(RedactDSN(dsn), "hunter2") {
			t.Errorf("password survived redaction of %q: %q", dsn, RedactDSN(dsn))
		}
	}
}
