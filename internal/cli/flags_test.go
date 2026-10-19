package cli

// The flat form and the subcommands are parsed by different libraries, for a
// reason given in flags.go. The hazard that creates is drift: a flag added to
// one surface and not the other, silently accepted in one invocation and
// rejected in the next. One table feeds both registrations, and these tests
// are what keep it that way.

import (
	"flag"
	"sort"
	"testing"

	"github.com/spf13/pflag"
)

func TestBothRegistrationsRegisterTheSameFlags(t *testing.T) {
	var goValues, pValues Values

	goSet := flag.NewFlagSet("go", flag.ContinueOnError)
	goValues.RegisterGoFlags(goSet)
	var goNames []string
	goSet.VisitAll(func(f *flag.Flag) { goNames = append(goNames, f.Name) })

	pSet := pflag.NewFlagSet("pflag", pflag.ContinueOnError)
	pValues.RegisterPFlags(pSet)
	var pNames []string
	pSet.VisitAll(func(f *pflag.Flag) { pNames = append(pNames, f.Name) })

	sort.Strings(goNames)
	sort.Strings(pNames)

	if len(goNames) != len(pNames) {
		t.Fatalf("flat form registers %d flags, subcommands register %d", len(goNames), len(pNames))
	}
	for i := range goNames {
		if goNames[i] != pNames[i] {
			t.Errorf("flag %d differs: flat has %q, subcommands have %q", i, goNames[i], pNames[i])
		}
	}

	// And both agree with the table they came from.
	table := FlagNames()
	sort.Strings(table)
	if len(table) != len(goNames) {
		t.Errorf("the table has %d flags but %d were registered", len(table), len(goNames))
	}
}

// Defaults have to match too. A flag registered on both surfaces with
// different defaults behaves differently depending on which verb was used,
// which is worse than not having the flag at all.
func TestBothRegistrationsShareDefaults(t *testing.T) {
	var goValues, pValues Values
	goSet := flag.NewFlagSet("go", flag.ContinueOnError)
	goValues.RegisterGoFlags(goSet)
	pSet := pflag.NewFlagSet("pflag", pflag.ContinueOnError)
	pValues.RegisterPFlags(pSet)

	goDefaults := map[string]string{}
	goSet.VisitAll(func(f *flag.Flag) { goDefaults[f.Name] = f.DefValue })
	pSet.VisitAll(func(f *pflag.Flag) {
		if want, ok := goDefaults[f.Name]; ok && want != f.DefValue {
			t.Errorf("--%s defaults to %q in the flat form and %q in subcommands",
				f.Name, want, f.DefValue)
		}
	})
}

// Parsing the same arguments through either surface must produce the same
// request, because they call the same operation.
func TestBothSurfacesProduceTheSameRequest(t *testing.T) {
	args := []string{
		"--start", "100", "--end", "200", "--output-dir", "/tmp/x",
		"--workers", "3", "--ducklake", "--era-id", "e1",
	}

	var goValues Values
	goSet := flag.NewFlagSet("go", flag.ContinueOnError)
	goValues.RegisterGoFlags(goSet)
	if err := goSet.Parse(args); err != nil {
		t.Fatal(err)
	}
	goReq, _, err := goValues.Resolve()
	if err != nil {
		t.Fatal(err)
	}

	var pValues Values
	pSet := pflag.NewFlagSet("pflag", pflag.ContinueOnError)
	pValues.RegisterPFlags(pSet)
	if err := pSet.Parse(args); err != nil {
		t.Fatal(err)
	}
	pReq, _, err := pValues.Resolve()
	if err != nil {
		t.Fatal(err)
	}

	if goReq != pReq {
		t.Errorf("the two surfaces disagree:\n flat: %+v\n verb: %+v", goReq, pReq)
	}
}

// The flat form accepts a single-dash long flag, which is what --help has
// always rendered and what some scripts will have copied. This is the
// incompatibility that keeps it off pflag, so it is worth pinning.
func TestFlatFormAcceptsSingleDashLongFlags(t *testing.T) {
	var values Values
	fs := flag.NewFlagSet("go", flag.ContinueOnError)
	values.RegisterGoFlags(fs)
	if err := fs.Parse([]string{"-output-dir", "/tmp/x", "-start", "5"}); err != nil {
		t.Fatalf("the flat form rejected a single-dash long flag: %v", err)
	}
	if values.OutputDir != "/tmp/x" || values.Start != 5 {
		t.Errorf("parsed into %+v", values)
	}

	// pflag would read -output-dir as a cluster of shorthands. Demonstrated
	// rather than asserted in prose, so the reason for two parsers is visible.
	var pValues Values
	pSet := pflag.NewFlagSet("pflag", pflag.ContinueOnError)
	pSet.SetOutput(discard{})
	pValues.RegisterPFlags(pSet)
	if err := pSet.Parse([]string{"-output-dir", "/tmp/x"}); err == nil {
		t.Error("pflag accepted a single-dash long flag; the flat form could use it after all")
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
