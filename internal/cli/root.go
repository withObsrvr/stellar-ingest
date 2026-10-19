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
	return []string{"extract", "validate", "load", "repair", "run", "doctor", "version", "help", "completion"}
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
			req, notices, err := values.Resolve()
			if err != nil {
				return err
			}
			outputs, err := ResolveOutput(values.OutputDir, values.Output)
			if err != nil {
				return err
			}
			for _, notice := range notices {
				fmt.Fprintln(errOut, notice)
			}
			emitter, err := NewStatusEmitter(values.ProgramStatus)
			if err != nil {
				return err
			}
			log, err := ResolveLogging(values, errOut)
			if err != nil {
				return err
			}
			// --timeout bounds the operation. Zero is no limit, which is
			// what every deployed invocation relies on.
			ctx, cancel := WithTimeout(cmd.Context(), values.Timeout)
			defer cancel()

			result, err := op(ctx, req, app.Deps{Out: out, Log: log, Status: emitter})
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
