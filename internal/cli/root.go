package cli

// The command surface, in the vocabulary §9 of the standard proposes.
//
// Every subcommand calls an operation in internal/app and owns no ingestion
// logic itself, which is §2's requirement of an adapter: "Cobra handlers must
// not own business rules."
//
// The flat flag form is retained. Deployed jobs pass about twenty flags with
// no verb, and breaking them to gain a noun would be exactly the dismantling
// the standard's §8 warns against for this tool. cmd/stellar-ingest decides
// which form it was given; this file only describes the verbs.

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/withObsrvr/stellar-ingest/internal/app"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/exit"
)

// operation is one of the application's use cases. It returns an error
// carrying an exit class; the composition root turns that into an exit code.
type operation func(context.Context, app.RunRequest, app.Deps) (*app.RunResult, error)

// CommandNames are the verbs the root command accepts. The flat-form
// dispatcher needs this list to tell a verb from a flag.
func CommandNames() []string {
	return []string{"extract", "validate", "load", "repair", "run", "doctor", "inspect", "wait", "version", "help", "completion"}
}

// NewRootCommand builds the command tree.
func NewRootCommand(out, errOut io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:   "stellar-ingest",
		Short: "Transform historical Stellar ledgers into analytical Bronze datasets",
		Long: `stellar-ingest acquires Stellar ledgers from archives, RPC, Captive Core or
local files, decodes them once, extracts 21 Bronze datasets in parallel, and
writes them to Parquet, DuckLake and optionally a PostgreSQL hot buffer.

Credentials come from the environment, not from arguments:

  DUCKLAKE_CATALOG_DSN   B2_KEY_ID   B2_KEY_SECRET   PG_PASSWORD

each with a _FILE form naming a file to read instead.`,
		Example: "  " + joinExamples([]string{
			"# Check this machine before committing to a backfill",
			"stellar-ingest doctor --ducklake",
			"",
			"# Extract one range to Parquet and nothing else",
			"stellar-ingest extract --network mainnet --start 64000000 --end 64001000 --output-dir ./bronze",
			"",
			"# A full pass: extract, push to DuckLake, record the range",
			"stellar-ingest run --config ingest.yaml --start 64000000 --end 64010000 \\",
			"    --chunk-size 1000 --chunk-concurrency 4 --ducklake --range-state",
			"",
			"# What is left to build, without taking a claim",
			"stellar-ingest run --config ingest.yaml --start 1 --end 64000000 --chunk-size 10000 --plan",
			"",
			"# Where each range stands, and blocking until they are done",
			"stellar-ingest inspect --start 64000000 --end 64010000 --chunk-size 1000 --output json",
			"stellar-ingest wait --start 64000000 --end 64010000 --chunk-size 1000 --for succeeded --timeout 6h",
			"",
			"# Machine-readable result, and an append-only event log",
			"stellar-ingest run --config ingest.yaml --start 64000000 --end 64001000 \\",
			"    --output json --events ./events.ndjson --quiet",
		}),
		SilenceUsage:  true,
		SilenceErrors: true,
		// A bare invocation should say what it offers rather than do
		// something surprising.
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	root.SetOut(out)
	root.SetErr(errOut)
	// A rejected flag value is invalid input, which §3 gives its own exit
	// class. Without this it arrived at the composition root unclassified
	// and became exit 1 -- the same code as a bug in the pusher, so a
	// scheduler would retry a typo forever.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return exit.Wrap(exit.Usage, err)
	})

	root.AddCommand(
		newOperationCommand("extract", "Acquire ledgers and write Bronze Parquet",
			`Extract writes Parquet only. It does not push to DuckLake or populate the
hot buffer, so it is the operation to use when the destination is decided
later, or not at all.`,
			[]string{
				"stellar-ingest extract --network mainnet --start 64000000 --end 64001000 --output-dir ./bronze",
			}, app.Extract, out, errOut),

		newOperationCommand("validate", "Check an existing output directory",
			`Validate reads an output directory and reports on it. It writes nothing and
pushes nowhere.`,
			[]string{
				"stellar-ingest validate --output-dir ./bronze --output json",
			}, app.Validate, out, errOut),

		newOperationCommand("load", "Push an existing output directory into DuckLake",
			`Load skips extraction and ingests Parquet that is already on disk, using the
manifest the extraction wrote.`,
			[]string{
				"stellar-ingest load --output-dir ./bronze --ducklake --ducklake-data-path s3://bucket/",
			}, app.Load, out, errOut),

		newOperationCommand("repair", "Backfill missing Bronze contract balance fields",
			`Repair rewrites contract balance_holder and balance in place and returns
without extracting. It needs a durable --enrichment-run-id unless
--enrichment-dry-run is set.`,
			[]string{
				"stellar-ingest repair --start 61790000 --end 61891000 --enrichment-dry-run",
			}, app.Repair, out, errOut),

		newOperationCommand("run", "Extract, load and populate in one pass",
			`Run performs the whole sequence the flat flag form has always performed:
acquire, extract to Parquet, push to DuckLake, populate the hot buffer. Which
phases happen is still decided by the flags.`,
			[]string{
				"stellar-ingest run --start 64000000 --end 64001000 --output-dir ./bronze --ducklake",
			}, app.Run, out, errOut),

		newDoctorCommand(out, errOut),
		newQueryCommand("inspect", out, errOut),
		newQueryCommand("wait", out, errOut),
		newVersionCommand(out),
	)
	return root
}

// newOperationCommand builds one verb over one application operation.
func newOperationCommand(use, short, long string, examples []string, op operation, out, errOut io.Writer) *cobra.Command {
	values := &Values{}
	cmd := &cobra.Command{
		Use:     use,
		Short:   short,
		Long:    long,
		Example: "  " + joinExamples(examples),
		RunE: func(cmd *cobra.Command, args []string) error {
			configNotices, err := ApplyConfig(values, cmd.Flags().Changed)
			if err != nil {
				return err
			}
			req, notices, err := values.Resolve()
			if err != nil {
				return err
			}
			outputs, err := ResolveOutput(values.OutputDir, values.Output)
			if err != nil {
				return err
			}
			emitter, err := NewStatusEmitter(values.ProgramStatus)
			if err != nil {
				return err
			}
			log, err := ResolveLogging(values, errOut)
			if err != nil {
				return err
			}
			for _, notice := range configNotices {
				// Informational: which context was used, and which flags
				// overrode it. Nothing is wrong, so not a warning.
				log.Info("%s", notice)
			}
			for _, notice := range notices {
				// stderr, and never a credential's value.
				log.Warn("%s", notice)
			}
			// --timeout bounds the operation. Zero is no limit, which is
			// what every deployed invocation relies on.
			ctx, cancel := WithTimeout(cmd.Context(), values.Timeout)
			defer cancel()

			jobID := NewJobID()
			eventStream, closeEvents, err := OpenEventStream(values.EventsPath, jobID, out)
			if err != nil {
				return err
			}
			defer closeEvents()

			result, err := op(ctx, req, app.Deps{
				Out: out, Log: log, Status: emitter, Events: eventStream,
			})
			if err != nil {
				return err
			}
			// Rendering belongs here, not in the operation: the same result
			// is what an API or a UI would serve, in whatever shape it
			// wants.
			return RenderResult(out, outputs.Format, result)
		},
	}
	values.RegisterPFlags(cmd.Flags())
	return cmd
}

func newVersionCommand(out io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the build version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(out, domain.Version)
			return nil
		},
	}
}

func joinExamples(examples []string) string {
	out := ""
	for i, e := range examples {
		if i > 0 {
			out += "\n  "
		}
		out += e
	}
	return out
}

// IsCommand reports whether name is one of the verbs. The composition root
// uses it to classify an unknown command as invalid input rather than
// matching on Cobra's message text, which changes between releases.
func IsCommand(name string) bool {
	for _, candidate := range CommandNames() {
		if name == candidate {
			return true
		}
	}
	return false
}

// newDoctorCommand builds the diagnostic §3 asks for under Discoverability.
//
// It is its own command rather than a mode of run because it answers a
// different question -- "is this machine configured" rather than "ingest
// this range" -- and because an operator wants to ask it before committing
// to a multi-day backfill, not instead of one.
func newDoctorCommand(out, errOut io.Writer) *cobra.Command {
	values := &Values{}
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check credentials, DuckDB extensions and catalog reachability",
		Long: `Doctor reports whether this machine is configured to run an ingestion:
whether each credential resolved and from where, whether DuckDB opens with
the extensions a push needs, whether the session is pinned to UTC, and
whether the catalog answers.

It never prints a credential value. Presence and length only, and any DSN it
reports is redacted.

Exit status is 2 when any check fails, so CI can gate on it.`,
		Example: "  " + joinExamples([]string{
			"stellar-ingest doctor",
			"stellar-ingest doctor --ducklake --output json",
			"stellar-ingest doctor --output-dir ./bronze",
		}),
		RunE: func(cmd *cobra.Command, args []string) error {
			configNotices, err := ApplyConfig(values, cmd.Flags().Changed)
			if err != nil {
				return err
			}
			req, notices, err := values.Resolve()
			if err != nil {
				return err
			}
			outputs, err := ResolveOutput(values.OutputDir, values.Output)
			if err != nil {
				return err
			}
			log, err := ResolveLogging(values, errOut)
			if err != nil {
				return err
			}
			for _, notice := range configNotices {
				// Informational: which context was used, and which flags
				// overrode it. Nothing is wrong, so not a warning.
				log.Info("%s", notice)
			}
			for _, notice := range notices {
				log.Warn("%s", notice)
			}
			ctx, cancel := WithTimeout(cmd.Context(), values.Timeout)
			defer cancel()
			return RunDoctor(ctx, req, app.Deps{Out: out, Log: log}, outputs.Format)
		},
	}
	values.RegisterPFlags(cmd.Flags())
	return cmd
}

// RunDoctor runs the diagnostics and renders them, returning a usage-class
// error when any check failed.
//
// Shared with the flat form's --doctor so the two surfaces cannot disagree
// about what a failing check means.
func RunDoctor(ctx context.Context, req app.RunRequest, deps app.Deps, format OutputFormat) error {
	result, err := app.Doctor(ctx, req, deps)
	if err != nil {
		return err
	}
	if err := RenderDoctor(deps.Out, format, result); err != nil {
		return err
	}
	if result.Failed > 0 {
		// Usage class: the operator has to change something, and retrying
		// the same invocation on the same machine cannot help.
		return exit.Usagef("%d diagnostic check(s) failed", result.Failed)
	}
	return nil
}

// newQueryCommand builds inspect and wait, §4's query and synchronisation
// primitives.
//
// One constructor for both because they differ only in whether they return
// the first answer or block for a condition, and splitting them would mean
// two copies of the same flag wiring drifting apart.
func newQueryCommand(verb string, out, errOut io.Writer) *cobra.Command {
	values := &Values{}
	cmd := &cobra.Command{Use: verb}

	switch verb {
	case "inspect":
		cmd.Short = "Report the lifecycle state of each range in the catalog"
		cmd.Long = `Inspect reads the range catalog and reports where each range in the
requested span stands: queued, running, succeeded or failed.

It is a query. It takes no claim and changes nothing, so running it while a
backfill is in flight is safe and shows that backfill's live claims.`
		cmd.Example = "  " + joinExamples([]string{
			"stellar-ingest inspect --start 64000000 --end 64010000 --chunk-size 1000",
			"stellar-ingest inspect --start 64000000 --end 64010000 --output json",
		})
	case "wait":
		cmd.Short = "Block until every range in the span reaches a condition"
		cmd.Long = `Wait polls the range catalog until the condition holds, the run is
cancelled, or --timeout elapses.

  --for succeeded   every range is frozen
  --for settled     no range is still queued or running

Exit status is 5 when the deadline passes, so CI can tell a timeout from a
failure.`
		cmd.Example = "  " + joinExamples([]string{
			"stellar-ingest wait --start 64000000 --end 64010000 --chunk-size 1000 --for succeeded --timeout 6h",
		})
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		configNotices, err := ApplyConfig(values, cmd.Flags().Changed)
		if err != nil {
			return err
		}
		req, notices, err := values.Resolve()
		if err != nil {
			return err
		}
		outputs, err := ResolveOutput(values.OutputDir, values.Output)
		if err != nil {
			return err
		}
		log, err := ResolveLogging(values, errOut)
		if err != nil {
			return err
		}
		for _, notice := range configNotices {
			// Informational: which context was used, and which flags
			// overrode it. Nothing is wrong, so not a warning.
			log.Info("%s", notice)
		}
		for _, notice := range notices {
			log.Warn("%s", notice)
		}
		ctx, cancel := WithTimeout(cmd.Context(), values.Timeout)
		defer cancel()

		deps := app.Deps{Out: out, Log: log}
		var result *app.InspectResult
		if verb == "wait" {
			result, err = app.Wait(ctx, req, deps, app.WaitCondition(values.WaitFor))
		} else {
			result, err = app.Inspect(ctx, req, deps)
		}
		// A timed-out wait still renders what it last saw: "it did not
		// finish" is more useful with the state attached than without.
		if renderErr := RenderInspect(out, outputs.Format, result); renderErr != nil && err == nil {
			return renderErr
		}
		return err
	}
	values.RegisterPFlags(cmd.Flags())
	return cmd
}
