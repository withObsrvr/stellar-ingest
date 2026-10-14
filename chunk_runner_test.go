package main

import (
	"reflect"
	"testing"
)

func TestSplitLedgerChunksAlignsBoundaries(t *testing.T) {
	want := []ledgerChunk{{3, 9999}, {10000, 19999}, {20000, 20005}}
	if got := splitLedgerChunks(3, 20005, 10000); !reflect.DeepEqual(got, want) {
		t.Fatalf("chunks = %#v, want %#v", got, want)
	}
}

func TestSplitLedgerChunksRecentRange(t *testing.T) {
	want := []ledgerChunk{{64920000, 64920999}, {64921000, 64921970}}
	if got := splitLedgerChunks(64920000, 64921970, 1000); !reflect.DeepEqual(got, want) {
		t.Fatalf("chunks = %#v, want %#v", got, want)
	}
}

func TestRewriteChunkArgs(t *testing.T) {
	args := []string{"--start=3", "--end", "20005", "--output", "/old", "--chunk-size=10000", "--workers", "8", "--validate"}
	want := []string{"--workers", "8", "--validate", "--start", "10000", "--end", "19999", "--output", "/new", "--chunk-size", "0"}
	if got := rewriteChunkArgs(args, ledgerChunk{10000, 19999}, "/new"); !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}
