package cli

// The whole of the --output transition lives in ResolveOutput, so this is
// where it is checked. Getting it wrong either breaks a deployed job that
// passes a path to --output, or writes Parquet into a directory called
// "json".

import "testing"

func TestResolveOutput(t *testing.T) {
	for _, tc := range []struct {
		name       string
		outputDir  string
		output     string
		wantDir    string
		wantFormat OutputFormat
		wantDeprec bool
		wantErr    bool
	}{
		{
			name:       "the new shape",
			outputDir:  "/data/bronze",
			output:     "json",
			wantDir:    "/data/bronze",
			wantFormat: FormatJSON,
		},
		{
			name:       "no format given defaults to a table",
			outputDir:  "/data/bronze",
			wantDir:    "/data/bronze",
			wantFormat: FormatTable,
		},
		{
			name:       "ndjson for streams",
			outputDir:  "/data/bronze",
			output:     "ndjson",
			wantDir:    "/data/bronze",
			wantFormat: FormatNDJSON,
		},
		{
			// Exactly what the deployed Nomad jobs pass today. This must keep
			// working, which is the entire reason for the value-based rule.
			name:       "a path in --output still works, with a warning",
			output:     "/data/output-validation",
			wantDir:    "/data/output-validation",
			wantFormat: FormatTable,
			wantDeprec: true,
		},
		{
			name:       "a relative path in --output still works",
			output:     "./bronze",
			wantDir:    "./bronze",
			wantFormat: FormatTable,
			wantDeprec: true,
		},
		{
			// Ambiguous on purpose: two different directories were named, and
			// guessing which one wins would write data somewhere unexpected.
			name:      "a directory in both flags is an error",
			outputDir: "/data/a",
			output:    "/data/b",
			wantErr:   true,
		},
		{
			// Harmless repetition during a migration -- a script passing both
			// the same value should not fail.
			name:       "the same directory in both flags is accepted",
			outputDir:  "/data/a",
			output:     "/data/a",
			wantDir:    "/data/a",
			wantFormat: FormatTable,
			wantDeprec: true,
		},
		{
			name:       "neither flag resolves to an empty directory for the caller to reject",
			wantDir:    "",
			wantFormat: FormatTable,
		},
	} {
		got, err := ResolveOutput(tc.outputDir, tc.output)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
			continue
		}
		if tc.wantErr {
			continue
		}
		if got.Dir != tc.wantDir {
			t.Errorf("%s: dir = %q, want %q", tc.name, got.Dir, tc.wantDir)
		}
		if got.Format != tc.wantFormat {
			t.Errorf("%s: format = %q, want %q", tc.name, got.Format, tc.wantFormat)
		}
		if got.DeprecatedDirFlag != tc.wantDeprec {
			t.Errorf("%s: deprecated = %v, want %v", tc.name, got.DeprecatedDirFlag, tc.wantDeprec)
		}
	}
}

// An unknown format must not be silently accepted as a directory and then
// fail somewhere further away. It is accepted as a directory by design, so
// the protection is that the caller still requires a directory -- which this
// documents by asserting the value lands in Dir.
func TestUnknownFormatIsTreatedAsADirectory(t *testing.T) {
	got, err := ResolveOutput("", "yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Dir != "yaml" || !got.DeprecatedDirFlag {
		t.Errorf("got %+v; an unrecognised value is the old directory meaning", got)
	}
}

// The notice has to name both the old and the new spelling, because its whole
// job is telling somebody what to change.
func TestDeprecationNoticeNamesBothFlags(t *testing.T) {
	s, err := ResolveOutput("", "/data/out")
	if err != nil {
		t.Fatal(err)
	}
	notice := s.DeprecationNotice()
	for _, want := range []string{"--output", "--output-dir", "/data/out", "json"} {
		if !contains(notice, want) {
			t.Errorf("notice does not mention %q: %s", want, notice)
		}
	}

	clean, err := ResolveOutput("/data/out", "json")
	if err != nil {
		t.Fatal(err)
	}
	if clean.DeprecationNotice() != "" {
		t.Errorf("the new shape produced a deprecation notice: %s", clean.DeprecationNotice())
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
