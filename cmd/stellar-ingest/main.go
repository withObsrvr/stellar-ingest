// Command stellar-ingest is the composition root.
//
// It decides which command surface it was given, resolves what belongs to the
// command line -- which flag named the output directory, where each
// credential came from, whether program status should be reported -- and
// hands a typed request to the application layer. It owns no ingestion logic,
// which is what §2 of the CLI Engineering Standard asks of an adapter.
//
// TWO SURFACES, ON PURPOSE. `stellar-ingest extract --start ...` goes through
// Cobra; `stellar-ingest --start ...` goes through the standard library's flag
// package, exactly as it always has. Deployed jobs pass about twenty flags
// with no verb, and breaking them to gain a noun would be the dismantling §8
// warns against for this tool. The flat form warns once and keeps working.
//
// The two are not parsed by the same library, and that is also on purpose:
// pflag reads a single dash followed by several characters as a cluster of
// shorthands, so `-output-dir x` -- which the standard library accepts and
// which --help has always rendered -- would fail under it. One flag table
// feeds both registrations so they cannot drift.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/withObsrvr/stellar-ingest/internal/app"
	"github.com/withObsrvr/stellar-ingest/internal/cli"
	"github.com/withObsrvr/stellar-ingest/internal/ingest"
)

func main() {
	ctx := context.Background()

	if verb, ok := subcommand(os.Args); ok {
		root := cli.NewRootCommand(os.Stdout, os.Stderr)
		if err := root.ExecuteContext(ctx); err != nil {
			// Cobra's own usage output is silenced so the message is not
			// buried under a flag list.
			log.Fatalf("%s: %v", verb, err)
		}
		return
	}

	runFlatForm(ctx)
}

// subcommand reports the verb, if the invocation names one.
//
// A first argument that starts with a dash is a flag, so the invocation is
// the flat form. Anything else is checked against the known verbs rather than
// assumed to be one: a typo should reach Cobra and get "unknown command"
// rather than silently fall through to the flat form and complain about a
// missing --start.
func subcommand(args []string) (string, bool) {
	// A bare invocation is somebody looking around, so it gets the command
	// list rather than "--output-dir is required". Nothing scripts a bare
	// invocation expecting that error; every caller passes flags.
	if len(args) < 2 {
		return "", true
	}
	candidate := args[1]
	if candidate == "" || candidate[0] == '-' {
		return "", false
	}
	for _, name := range cli.CommandNames() {
		if candidate == name {
			return candidate, true
		}
	}
	// Not a known verb, but not a flag either. Hand it to Cobra so the error
	// names the problem.
	return candidate, true
}

// runFlatForm is the original interface: flags, no verb.
func runFlatForm(ctx context.Context) {
	values := &cli.Values{}
	values.RegisterGoFlags(flag.CommandLine)
	flag.Parse()

	if values.ShowVersion {
		// stdout: this is the result, not a diagnostic.
		fmt.Println(ingest.Version)
		return
	}

	req, notices, err := values.Resolve()
	if err != nil {
		log.Fatal(err)
	}
	for _, notice := range notices {
		// stderr, and never a credential's value.
		log.Print(notice)
	}

	log.Printf("stellar-ingest was invoked without a command. The verbs -- extract, " +
		"validate, load, repair, run -- call the same operations and will become the " +
		"documented surface; `stellar-ingest run` is this invocation. The flag-only " +
		"form keeps working.")

	emitter, err := cli.NewStatusEmitter(values.ProgramStatus)
	if err != nil {
		log.Fatal(err)
	}

	// Everything the command line had to decide is decided. From here the
	// request is data, and the operation behind it has no idea a terminal
	// exists.
	app.Run(ctx, req, emitter)
}
