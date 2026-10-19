package cli

// Exit classes, asserted through the real command tree.
//
// §3 wants invalid input to have its own exit class, and the most common
// invalid inputs are a mistyped flag value and a mistyped verb. Both arrived
// at the composition root as plain errors and became exit 1 -- the same code
// as a bug in the pusher, so a scheduler would retry a typo forever.
//
// Classified without matching on Cobra's message text, which changes between
// releases: flag errors come through SetFlagErrorFunc, and a verb is checked
// against CommandNames.

import (
	"bytes"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/exit"
)

func TestRejectedFlagValuesAreUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"unparseable duration", []string{"run", "--timeout", "nope"}},
		{"unparseable number", []string{"run", "--start", "x"}},
		{"unknown flag", []string{"run", "--nosuchflag"}},
		{"unparseable bool", []string{"run", "--ducklake=maybe"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			root := NewRootCommand(&out, &errOut)
			root.SetArgs(c.args)

			err := root.Execute()
			if err == nil {
				t.Fatalf("%v was accepted", c.args)
			}
			if got := exit.ClassOf(err); got != exit.Usage {
				t.Errorf("exit class = %v, want %v; a scheduler must not retry a typo. error: %v",
					got, exit.Usage, err)
			}
		})
	}
}

func TestIsCommand(t *testing.T) {
	for _, name := range CommandNames() {
		if !IsCommand(name) {
			t.Errorf("IsCommand(%q) = false for a name CommandNames reports", name)
		}
	}
	for _, name := range []string{"frobnicate", "", "--start", "Run", "ru"} {
		if IsCommand(name) {
			t.Errorf("IsCommand(%q) = true; a typo must be classified as invalid input", name)
		}
	}
}

// Every verb CommandNames advertises must actually resolve, or the flat-form
// dispatcher sends a real verb to Cobra and gets "unknown command".
func TestEveryAdvertisedCommandExists(t *testing.T) {
	var out, errOut bytes.Buffer
	root := NewRootCommand(&out, &errOut)

	available := map[string]bool{"help": true, "completion": true}
	for _, c := range root.Commands() {
		available[c.Name()] = true
	}
	for _, name := range CommandNames() {
		if !available[name] {
			t.Errorf("CommandNames advertises %q but the command tree has no such command; the "+
				"flat-form dispatcher would hand it to Cobra and get \"unknown command\"", name)
		}
	}
}
