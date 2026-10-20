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
	"os"
	"os/signal"
	"syscall"

	"github.com/withObsrvr/stellar-ingest/internal/app"
	"github.com/withObsrvr/stellar-ingest/internal/cli"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/exit"
)

func main() {
	// CLI-11: "stop starting new work, finish or roll back the current unit,
	// write its checkpoint and exit with 130 or 143." A cancelled context
	// reaches every blocking layer, so a stopped Nomad allocation unwinds its
	// defers -- closing the pusher, releasing the range claim -- instead of
	// being killed mid-push.
	//
	// Handled directly rather than with signal.NotifyContext, which discards
	// which signal arrived. CLI-15 distinguishes 130 (SIGINT, a person at a
	// terminal) from 143 (SIGTERM, a supervisor reclaiming the allocation),
	// and a scheduler reads that difference.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	// received is written once by the watcher and read once below, after
	// run returns. The buffered channel is the handoff: a nil read means no
	// signal arrived.
	received := make(chan syscall.Signal, 1)
	go func() {
		first, ok := <-signals
		if !ok {
			return
		}
		if sig, isUnix := first.(syscall.Signal); isUnix {
			received <- sig
		} else {
			received <- syscall.SIGINT
		}
		cancel()
		// A second signal means the operator has stopped waiting for a
		// clean shutdown. CLI-11 allows forcing the exit, and the code
		// still names the signal that caused it.
		if second, ok := <-signals; ok {
			if sig, isUnix := second.(syscall.Signal); isUnix {
				os.Exit(128 + int(sig))
			}
			os.Exit(130)
		}
	}()

	err := run(ctx)
	if err != nil && ctx.Err() != nil {
		// The run failed because it was stopped. Reporting the operation's
		// own error class here would call a deliberate shutdown an internal
		// failure, and a scheduler would retry it as though something were
		// broken.
		err = fmt.Errorf("%w (run was interrupted: %v)", ctx.Err(), err)
		select {
		case sig := <-received:
			err = exit.Interrupt(sig, err)
		default:
			// Cancelled without a signal: a caller's deadline or a
			// programmatic stop. ClassOf already reports 130 for that.
		}
	}
	if err != nil {
		// stderr: this is a diagnostic. One line, no timestamp, no stack --
		// the message says what to fix and the exit code says what kind of
		// problem it is.
		fmt.Fprintf(os.Stderr, "stellar-ingest: %v\n", err)
	}
	// The exit code is the error's class. Every failure used to be log.Fatal,
	// so every failure was 1; see internal/exit for the table. os.Exit runs
	// no deferred functions, which is why the operation returns here rather
	// than exiting in place: its own defers -- closing the pusher, the range
	// store, the enricher -- have already run by the time we get here. Under
	// log.Fatal they never ran at all.
	exit.Exit(err)
}

// run dispatches to whichever surface the invocation used and returns the
// failure rather than ending the process, so one place owns the exit code.
func run(ctx context.Context) error {
	if verb, ok := subcommand(os.Args); ok {
		root := cli.NewRootCommand(os.Stdout, os.Stderr)
		if err := root.ExecuteContext(ctx); err != nil {
			// A verb that is not a verb is a typo, which is invalid input.
			// subcommand hands anything non-flag to Cobra so the error names
			// the problem, but the class has to be set here: Cobra reports
			// "unknown command" as a plain error and exit 1 would tell a
			// scheduler to retry the typo.
			if verb != "" && !cli.IsCommand(verb) {
				err = exit.Wrap(exit.Usage, err)
			}
			// Cobra's own usage output is silenced so the message is not
			// buried under a flag list.
			if verb == "" {
				return err
			}
			return fmt.Errorf("%s: %w", verb, err)
		}
		return nil
	}
	return runFlatForm(ctx)
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
	if cli.IsCommand(candidate) {
		return candidate, true
	}
	// Not a known verb, but not a flag either. Hand it to Cobra so the error
	// names the problem.
	return candidate, true
}

// runFlatForm is the original interface: flags, no verb.
func runFlatForm(ctx context.Context) error {
	values := &cli.Values{}
	values.RegisterGoFlags(flag.CommandLine)
	flag.Parse()

	if values.ShowVersion {
		// stdout: this is the result, not a diagnostic.
		fmt.Println(domain.Version)
		return nil
	}

	// Which flags the caller actually typed. The standard library has no
	// Changed, so the set is collected from Visit, which walks only the
	// flags that were given.
	given := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { given[f.Name] = true })

	configNotices, err := cli.ApplyConfig(values, func(name string) bool { return given[name] })
	if err != nil {
		return err
	}

	req, notices, err := values.Resolve()
	if err != nil {
		return err
	}

	log, err := cli.ResolveLogging(values, os.Stderr)
	if err != nil {
		return err
	}
	for _, notice := range configNotices {
		// Informational: which context was used, and which flags overrode
		// it. Not a warning -- nothing is wrong.
		log.Info("%s", notice)
	}
	for _, notice := range notices {
		// stderr, and never a credential's value.
		log.Warn("%s", notice)
	}

	log.Warn("stellar-ingest was invoked without a command. The verbs -- extract, " +
		"validate, load, repair, run -- call the same operations and will become the " +
		"documented surface; `stellar-ingest run` is this invocation. The flag-only " +
		"form keeps working.")

	emitter, err := cli.NewStatusEmitter(values.ProgramStatus)
	if err != nil {
		return err
	}

	ctx, cancel := cli.WithTimeout(ctx, values.Timeout)
	defer cancel()

	outputs, err := cli.ResolveOutput(values.OutputDir, values.Output)
	if err != nil {
		return err
	}

	// --doctor is a mode switch rather than a verb here, so the flat form
	// can reach the diagnostic without growing a subcommand. Both surfaces
	// call the same function, so they cannot disagree about what a failing
	// check means.
	if values.Doctor {
		return cli.RunDoctor(ctx, req, app.Deps{Out: os.Stdout, Log: log}, outputs.Format)
	}

	// Everything the command line had to decide is decided. From here the
	// request is data, and the operation behind it has no idea a terminal
	// exists.
	jobID := cli.NewJobID()
	eventStream, closeEvents, err := cli.OpenEventStream(values.EventsPath, jobID, os.Stdout)
	if err != nil {
		return err
	}
	defer closeEvents()

	result, err := app.Run(ctx, req, app.Deps{
		Out: os.Stdout, Log: log, Status: emitter, Events: eventStream,
	})
	if err != nil {
		return err
	}
	return cli.RenderResult(os.Stdout, outputs.Format, result)
}
