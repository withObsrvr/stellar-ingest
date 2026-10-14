package main

import "testing"

func TestExtractionRequested(t *testing.T) {
	tests := []struct {
		name        string
		storageType string
		bucket      string
		want        bool
	}{
		{name: "archive bucket", storageType: "GCS", bucket: "archive/prefix", want: true},
		{name: "captive core without bucket", storageType: "CAPTIVE_CORE", want: true},
		{name: "existing output ducklake run", storageType: "FS", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := extractionRequested(test.storageType, test.bucket); got != test.want {
				t.Fatalf("extractionRequested(%q, %q) = %t, want %t", test.storageType, test.bucket, got, test.want)
			}
		})
	}
}
