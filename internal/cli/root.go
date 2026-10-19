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
	"github.com/withObsrvr/stellar-ingest/internal/status"
)

// operation is one of the application's use cases.
type operation func(context.Context, app.RunRequest, *status.Emitter)

// CommandNames are the verbs the root command accepts. The flat-form
// dispatcher needs this list to tell a verb from a flag.
func CommandNames() []string {
	return []string{"extract", "validate", "load", "repair", "run", "version", "help", "completion"}
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

	root.AddCommand(
		newOperationCommand("extract", "Acquire ledgers and write Bronze Parquet",
			`Extract writes Parquet only. It does not push to DuckLake or populate the
hot buffer, so it is the operation to use when the destination is decided
later, or not at all.`,
			[]string{
				"stellar-ingest extract --network mainnet --start 64000000 --end 64001000 --output-dir ./bronze",
			}, app.Extract, errOut),

		newOperationCommand("validate", "Check an existing output directory",
			`Validate reads an output directory and reports on it. It writes nothing and
pushes nowhere.`,
			[]string{
				"stellar-ingest validate --output-dir ./bronze --output json",
			}, app.Validate, errOut),

		newOperationCommand("load", "Push an existing output directory into DuckLake",
			`Load skips extraction and ingests Parquet that is already on disk, using the
manifest the extraction wrote.`,
			[]string{
				"stellar-ingest load --output-dir ./bronze --ducklake --ducklake-data-path s3://bucket/",
			}, app.Load, errOut),

		newOperationCommand("repair", "Backfill missing Bronze contract balance fields",
			`Repair rewrites contract balance_holder and balance in place and returns
without extracting. It needs a durable --enrichment-run-id unless
--enrichment-dry-run is set.`,
			[]string{
				"stellar-ingest repair --start 61790000 --end 61891000 --enrichment-dry-run",
			}, app.Repair, errOut),

		newOperationCommand("run", "Extract, load and populate in one pass",
			`Run performs the whole sequence the flat flag form has always performed:
acquire, extract to Parquet, push to DuckLake, populate the hot buffer. Which
phases happen is still decided by the flags.`,
			[]string{
				"stellar-ingest run --start 64000000 --end 64001000 --output-dir ./bronze --ducklake",
			}, app.Run, errOut),

		newVersionCommand(out),
	)
	return root
}

// newOperationCommand builds one verb over one application operation.
func newOperationCommand(use, short, long string, examples []string, op operation, errOut io.Writer) *cobra.Command {
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
			for _, notice := range notices {
				fmt.Fprintln(errOut, notice)
			}
			emitter, err := NewStatusEmitter(values.ProgramStatus)
			if err != nil {
				return err
			}
			op(cmd.Context(), req, emitter)
			return nil
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
