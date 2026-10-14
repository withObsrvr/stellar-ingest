package main

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
)

type streamTestItem struct {
	data []byte
	err  error
}

type scriptedLedgerStream struct {
	items []streamTestItem
}

func (s scriptedLedgerStream) RawLedgers(context.Context, ledgerbackend.Range, ...ledgerbackend.StreamOption) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for _, item := range s.items {
			if !yield(item.data, item.err) {
				return
			}
		}
	}
}

func TestSplitArchiveLocation(t *testing.T) {
	for _, test := range []struct {
		input, bucket, path string
	}{
		{"bucket", "bucket", ""},
		{"bucket/prefix/ledgers", "bucket", "prefix/ledgers"},
		{"s3://bucket/prefix", "bucket", "prefix"},
		{"gs://bucket/prefix", "bucket", "prefix"},
	} {
		bucket, path := splitArchiveLocation(test.input)
		if bucket != test.bucket || path != test.path {
			t.Fatalf("splitArchiveLocation(%q) = (%q, %q), want (%q, %q)", test.input, bucket, path, test.bucket, test.path)
		}
	}
}

func TestSDKStreamRangeValidatesBoundedCompleteness(t *testing.T) {
	for _, test := range []struct {
		name    string
		items   []streamTestItem
		wantSeq []uint32
		wantErr string
	}{
		{name: "complete", items: []streamTestItem{{data: streamFixtureLedger(t, 100)}, {data: streamFixtureLedger(t, 101)}}, wantSeq: []uint32{100, 101}},
		{name: "gap", items: []streamTestItem{{data: streamFixtureLedger(t, 100)}, {data: streamFixtureLedger(t, 102)}}, wantSeq: []uint32{100}, wantErr: "got 102, want 101"},
		{name: "duplicate", items: []streamTestItem{{data: streamFixtureLedger(t, 100)}, {data: streamFixtureLedger(t, 100)}}, wantSeq: []uint32{100}, wantErr: "got 100, want 101"},
		{name: "truncated", items: []streamTestItem{{data: streamFixtureLedger(t, 100)}}, wantSeq: []uint32{100}, wantErr: "incomplete bounded range"},
		{name: "source error", items: []streamTestItem{{data: streamFixtureLedger(t, 100)}, {err: errors.New("upstream unavailable")}}, wantSeq: []uint32{100}, wantErr: "upstream unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &SDKStreamSource{stream: scriptedLedgerStream{items: test.items}}
			var gotSeq []uint32
			var gotErr error
			for item := range source.StreamRange(context.Background(), 100, 101) {
				if item.err != nil {
					gotErr = item.err
					break
				}
				gotSeq = append(gotSeq, item.sequence)
			}
			if len(gotSeq) != len(test.wantSeq) {
				t.Fatalf("sequences = %v, want %v", gotSeq, test.wantSeq)
			}
			for i := range gotSeq {
				if gotSeq[i] != test.wantSeq[i] {
					t.Fatalf("sequences = %v, want %v", gotSeq, test.wantSeq)
				}
			}
			if test.wantErr == "" && gotErr != nil {
				t.Fatalf("unexpected error: %v", gotErr)
			}
			if test.wantErr != "" && (gotErr == nil || !strings.Contains(gotErr.Error(), test.wantErr)) {
				t.Fatalf("error = %v, want substring %q", gotErr, test.wantErr)
			}
		})
	}
}

func streamFixtureLedger(t *testing.T, sequence uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V:  0,
		V0: &xdr.LedgerCloseMetaV0{LedgerHeader: xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(sequence)}}},
	}
	raw, err := lcm.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal fixture ledger: %v", err)
	}
	return raw
}
