package main

import (
	"errors"
	"testing"

	flowctlpb "github.com/withobsrvr/flowctl/proto"
)

func TestClassifyFlowctlFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want flowctlpb.FailureClass
	}{
		{
			name: "catalog timeout is retryable infrastructure",
			err:  errors.New("catalog postgres connection timed out"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_RETRYABLE_INFRASTRUCTURE,
		},
		{
			name: "parquet schema mismatch is non retryable schema",
			err:  errors.New("parquet column schema mismatch"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_SCHEMA,
		},
		{
			name: "utf8 encoding failure is non retryable data",
			err:  errors.New("invalid utf8 encoding in memo"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_DATA,
		},
		{
			// Precedence guard: DuckLake schema errors routinely mention the
			// catalog/postgres. A non-retryable schema failure must win over the
			// infrastructure bucket so it is never retried forever as a poison chunk.
			name: "catalog schema error is non retryable schema, not infrastructure",
			err:  errors.New("ducklake catalog: column type schema mismatch in postgres metadata"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_SCHEMA,
		},
		{
			name: "xdr decode error mentioning s3 is non retryable data, not infrastructure",
			err:  errors.New("xdr decode failed reading object from s3"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_DATA,
		},
		{
			name: "unclassified error is unknown",
			err:  errors.New("something unexpected happened"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_UNKNOWN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyFlowctlFailure(tt.err); got != tt.want {
				t.Fatalf("classifyFlowctlFailure() = %s, want %s", got, tt.want)
			}
		})
	}
}
