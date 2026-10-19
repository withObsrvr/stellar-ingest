package jobs

import (
	"reflect"
	"strings"
	"testing"
)

func TestSplitLedgerChunksAlignsBoundaries(t *testing.T) {
	want := []ledgerChunk{{3, 9999}, {10000, 19999}, {20000, 20005}}
	if got := SplitLedgerChunks(3, 20005, 10000); !reflect.DeepEqual(got, want) {
		t.Fatalf("chunks = %#v, want %#v", got, want)
	}
}

func TestSplitLedgerChunksRecentRange(t *testing.T) {
	want := []ledgerChunk{{64920000, 64920999}, {64921000, 64921970}}
	if got := SplitLedgerChunks(64920000, 64921970, 1000); !reflect.DeepEqual(got, want) {
		t.Fatalf("chunks = %#v, want %#v", got, want)
	}
}

func TestRewriteChunkArgs(t *testing.T) {
	// A parent invoked with the deprecated --output path. The child gets
	// --output-dir, which is the spelling that still means a directory once
	// --output becomes the response format.
	args := []string{"--start=3", "--end", "20005", "--output", "/old", "--chunk-size=10000", "--workers", "8", "--validate"}
	want := []string{"--workers", "8", "--validate", "--start", "10000", "--end", "19999", "--output-dir", "/new", "--chunk-size", "0"}
	if got := rewriteChunkArgs(args, ledgerChunk{10000, 19999}, "/new"); !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

// Both spellings of the directory flag must be stripped from the parent's
// arguments. A child that inherited either would write every chunk into one
// directory, and the per-chunk manifests would overwrite each other.
func TestRewriteChunkArgsStripsBothDirectorySpellings(t *testing.T) {
	for _, parentFlag := range [][]string{
		{"--output", "/old"},
		{"--output-dir", "/old"},
		{"--output-dir=/old"},
		{"--output=/old"},
	} {
		args := append([]string{"--start=3", "--end", "20005"}, parentFlag...)
		got := rewriteChunkArgs(args, ledgerChunk{10000, 19999}, "/new")

		for i, arg := range got {
			if arg == "/old" {
				t.Errorf("%v: the parent directory survived into the child args: %#v", parentFlag, got)
			}
			// The only directory flag the child may carry is --output-dir /new.
			if arg == "--output" {
				t.Errorf("%v: the child was given --output, which now means a format: %#v", parentFlag, got)
			}
			if arg == "--output-dir" && (i+1 >= len(got) || got[i+1] != "/new") {
				t.Errorf("%v: --output-dir does not point at the chunk directory: %#v", parentFlag, got)
			}
		}
	}
}

// The response format must be inherited, not stripped: a child rendering a
// different format than its parent was asked for would make the run's output
// inconsistent. --output only carries a format once the deprecated path is
// gone, so this pins the intent now.
func TestRewriteChunkArgsKeepsNonDirectoryFlags(t *testing.T) {
	args := []string{"--start=3", "--end", "20005", "--output-dir", "/old",
		"--program-status", "never", "--workers", "4"}
	got := rewriteChunkArgs(args, ledgerChunk{10000, 19999}, "/new")

	joined := ""
	for _, a := range got {
		joined += a + " "
	}
	for _, want := range []string{"--program-status never", "--workers 4"} {
		if !strings.Contains(joined, want) {
			t.Errorf("child lost %q: %#v", want, got)
		}
	}
}

// --prefetch-buffer must be inherited, not stripped. It is a per-worker
// memory budget and the child is the process that actually spends it: a
// parent told to buffer 25 ledgers per worker, whose children each buffer
// the default 200, has had its memory ceiling raised 8x by the chunking it
// asked for to stay under that ceiling.
func TestRewriteChunkArgsKeepsPrefetchBuffer(t *testing.T) {
	for _, spelling := range [][]string{
		{"--prefetch-buffer", "25"},
		{"--prefetch-buffer=25"},
	} {
		args := append([]string{"--start=3", "--end", "20005", "--output-dir", "/old",
			"--chunk-size=10000"}, spelling...)
		got := strings.Join(rewriteChunkArgs(args, ledgerChunk{10000, 19999}, "/new"), " ")

		if !strings.Contains(got, "25") || !strings.Contains(got, "--prefetch-buffer") {
			t.Errorf("%v did not survive into the child args: %s", spelling, got)
		}
	}
}
