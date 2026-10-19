package ingest

// Input rules that belong to the domain rather than to the command line.
//
// They moved here when the engine became importable: the engine's own tests
// already exercised them, and a rule enforced only by a CLI handler is a rule
// an API or a UI can skip -- which §2 of the CLI standard is about.

import (
	"fmt"
	"strings"

	flowctlpb "github.com/withobsrvr/flowctl/proto"
)

// This is a function rather than three lines in main so it can be tested:
// the rule cost a real dispatch, where four chunks succeeded and the trailing
// single-ledger chunk failed.
func ValidateLedgerRange(start, end uint32) error {
	if end < start {
		return fmt.Errorf("--end (%d) must not be less than --start (%d)", end, start)
	}
	return nil
}

func ExtractionRequested(storageType, bucket string) bool {
	return bucket != "" || storageType == "CAPTIVE_CORE"
}

func ParseCSVSet(value string) map[string]bool {
	result := map[string]bool{}
	for _, part := range strings.Split(value, ",") {
		item := strings.TrimSpace(part)
		if item != "" {
			result[item] = true
		}
	}
	return result
}

func ClassifyFlowctlFailure(err error) flowctlpb.FailureClass {
	msg := strings.ToLower(err.Error())
	switch {
	// Check non-retryable data/schema buckets first. Infrastructure substrings
	// like "catalog"/"postgres" routinely appear inside DuckLake schema/data error
	// messages; if the broad infrastructure case were evaluated first, a genuinely
	// non-retryable poison chunk would be classed retryable and an orchestrator
	// would retry it forever instead of surfacing it to a human.
	case strings.Contains(msg, "schema"), strings.Contains(msg, "column"), strings.Contains(msg, "type"), strings.Contains(msg, "parquet"):
		return flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_SCHEMA
	case strings.Contains(msg, "utf"), strings.Contains(msg, "encoding"), strings.Contains(msg, "xdr"):
		return flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_DATA
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "connection"), strings.Contains(msg, "temporar"), strings.Contains(msg, "503"), strings.Contains(msg, "catalog"), strings.Contains(msg, "postgres"), strings.Contains(msg, "b2"), strings.Contains(msg, "s3"):
		return flowctlpb.FailureClass_FAILURE_CLASS_RETRYABLE_INFRASTRUCTURE
	default:
		return flowctlpb.FailureClass_FAILURE_CLASS_UNKNOWN
	}
}
