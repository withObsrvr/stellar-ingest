package main

import (
	"testing"

	extract "github.com/withObsrvr/stellar-extract"
)

func TestConvertExtractedConfigSettingMarksLedgerCloseMetaProvenance(t *testing.T) {
	got := convertExtractedConfigSetting(extract.ConfigSettingData{
		ConfigSettingID: 12,
		LedgerSequence:  3811200,
	})

	if got.ObservationSource != "ledger_close_meta" {
		t.Fatalf("observation source = %q, want ledger_close_meta", got.ObservationSource)
	}
	if got.ResolvedAtLedger == nil || *got.ResolvedAtLedger != 3811200 {
		t.Fatalf("resolved-at ledger = %v, want 3811200", got.ResolvedAtLedger)
	}
}
