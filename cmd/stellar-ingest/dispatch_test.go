package main

// Which surface an invocation reaches is decided by one function, and getting
// it wrong is not subtle: a deployed job passing twenty flags with no verb
// must reach the flat form, and a mistyped verb must reach Cobra and be told
// so rather than fall through and complain about a missing --start.

import "testing"

func TestSubcommandClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		verb  string
		isCmd bool
	}{
		{
			// The shape every deployed Nomad job uses.
			name:  "flags with no verb is the flat form",
			args:  []string{"stellar-ingest", "--start", "1", "--end", "2", "--output-dir", "/x"},
			isCmd: false,
		},
		{
			name:  "a single-dash long flag is still the flat form",
			args:  []string{"stellar-ingest", "-output-dir", "/x"},
			isCmd: false,
		},
		{
			name:  "a bare invocation gets the command list",
			args:  []string{"stellar-ingest"},
			isCmd: true,
		},
		{"extract", []string{"stellar-ingest", "extract", "--start", "1"}, "extract", true},
		{"validate", []string{"stellar-ingest", "validate"}, "validate", true},
		{"load", []string{"stellar-ingest", "load"}, "load", true},
		{"repair", []string{"stellar-ingest", "repair"}, "repair", true},
		{"run", []string{"stellar-ingest", "run"}, "run", true},
		{"version", []string{"stellar-ingest", "version"}, "version", true},
		{"help", []string{"stellar-ingest", "help"}, "help", true},
		{
			// Must not fall through: "unknown command" is a better answer
			// than "--output-dir is required".
			name:  "a mistyped verb goes to Cobra",
			args:  []string{"stellar-ingest", "extarct", "--start", "1"},
			verb:  "extarct",
			isCmd: true,
		},
		{
			name:  "an empty first argument is not a verb",
			args:  []string{"stellar-ingest", ""},
			isCmd: false,
		},
	} {
		verb, ok := subcommand(tc.args)
		if ok != tc.isCmd {
			t.Errorf("%s: isCommand = %v, want %v", tc.name, ok, tc.isCmd)
			continue
		}
		if tc.verb != "" && verb != tc.verb {
			t.Errorf("%s: verb = %q, want %q", tc.name, verb, tc.verb)
		}
	}
}

// --version must work on the flat form, because that is where it was added
// and where a script would already be calling it.
func TestVersionFlagIsOnTheFlatForm(t *testing.T) {
	if _, ok := subcommand([]string{"stellar-ingest", "--version"}); ok {
		t.Error("--version was routed to Cobra, where it is a verb rather than a flag")
	}
}
