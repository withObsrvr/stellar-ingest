package main

// --output used to name the output directory. The Obsrvr CLI Engineering
// Standard reserves it for the response format -- `--output json` -- so the
// directory moves to --output-dir and --output becomes the format.
//
// Both cannot be --output at once, and every deployed job passes a path to it
// today, so the transition is resolved by VALUE rather than by a flag day:
//
//	--output table|json|ndjson   a format, the new meaning
//	--output <anything else>     a directory, the old meaning, with a warning
//
// That is deterministic and documented rather than clever. The only value it
// gets wrong is an output directory named exactly "table", "json" or
// "ndjson", which fails loudly and immediately with --output-dir unset rather
// than writing somewhere surprising.
//
// The deprecated path is kept for one release so no running job breaks the day
// this lands, which is what §5 of the standard means by a deprecated flag
// window.

import (
	"fmt"
	"sort"
	"strings"
)

// OutputFormat is how a result is rendered.
type OutputFormat string

const (
	// FormatTable is human-readable output, the default.
	FormatTable OutputFormat = "table"
	// FormatJSON is one machine-readable document for a single result.
	FormatJSON OutputFormat = "json"
	// FormatNDJSON is one document per line, for streams of events.
	FormatNDJSON OutputFormat = "ndjson"
)

// outputFormats is the set --output accepts, and therefore the set of values
// that are NOT treated as a directory.
var outputFormats = map[string]OutputFormat{
	string(FormatTable):  FormatTable,
	string(FormatJSON):   FormatJSON,
	string(FormatNDJSON): FormatNDJSON,
}

// outputFormatNames lists the accepted formats, sorted, for error messages.
func outputFormatNames() string {
	names := make([]string, 0, len(outputFormats))
	for name := range outputFormats {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// outputSelection is the resolved pair: where files go, and how results are
// rendered.
type outputSelection struct {
	Dir    string
	Format OutputFormat
	// DeprecatedDirFlag is true when the directory arrived through --output
	// rather than --output-dir, so the caller can warn once.
	DeprecatedDirFlag bool
}

// resolveOutput works out the directory and the format from the two flags.
//
// Separate from the flag parsing so the rule can be read and tested on its
// own: this is the whole of the transition, and getting it wrong either
// breaks a running job or writes output to a directory called "json".
func resolveOutput(outputDir, output string) (outputSelection, error) {
	if format, ok := outputFormats[output]; ok {
		// --output names a format. The directory must come from --output-dir.
		return outputSelection{Dir: outputDir, Format: format}, nil
	}

	if output == "" {
		return outputSelection{Dir: outputDir, Format: FormatTable}, nil
	}

	// --output holds something that is not a format, so it is a directory
	// under the old meaning.
	if outputDir != "" && outputDir != output {
		return outputSelection{}, fmt.Errorf(
			"--output-dir is %q and --output is %q; --output is now the response format (%s), "+
				"so pass the directory only as --output-dir",
			outputDir, output, outputFormatNames())
	}
	return outputSelection{Dir: output, Format: FormatTable, DeprecatedDirFlag: true}, nil
}

// deprecationNotice is what to tell someone still passing a directory to
// --output. Returned rather than logged so the caller decides the stream.
func (s outputSelection) deprecationNotice() string {
	if !s.DeprecatedDirFlag {
		return ""
	}
	return fmt.Sprintf(
		"--output %s is deprecated: --output now selects the response format (%s). "+
			"Pass the directory as --output-dir %s. The old meaning is accepted for one release.",
		s.Dir, outputFormatNames(), s.Dir)
}
