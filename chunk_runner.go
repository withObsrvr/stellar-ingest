package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type ledgerChunk struct {
	Start uint32
	End   uint32
}

// splitLedgerChunks aligns chunk boundaries to multiples of chunkSize. This
// keeps --chunk-size 10000 aligned with the loader's range_XXXXXXXX layout,
// including when history starts at ledger 3.
func splitLedgerChunks(start, end, chunkSize uint32) []ledgerChunk {
	if chunkSize == 0 || start > end {
		return nil
	}
	var chunks []ledgerChunk
	for current := start; ; {
		boundary := (uint64(current)/uint64(chunkSize) + 1) * uint64(chunkSize)
		chunkEnd64 := boundary - 1
		if chunkEnd64 > uint64(end) {
			chunkEnd64 = uint64(end)
		}
		chunkEnd := uint32(chunkEnd64)
		chunks = append(chunks, ledgerChunk{Start: current, End: chunkEnd})
		if chunkEnd == end {
			break
		}
		current = chunkEnd + 1
	}
	return chunks
}

func runChunkedCommand(ctx context.Context, originalArgs []string, start, end, chunkSize uint32, outputDir string) error {
	chunks := splitLedgerChunks(start, end, chunkSize)
	if len(chunks) == 0 {
		return fmt.Errorf("invalid chunk range %d-%d with size %d", start, end, chunkSize)
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}

	started := time.Now()
	fmt.Printf("=== Chunked Backfill ===\nRange: %d - %d\nChunk size: %d\nChunks: %d\n\n", start, end, chunkSize, len(chunks))
	for index, chunk := range chunks {
		chunkOutput := filepath.Join(outputDir, "chunks", fmt.Sprintf("%d-%d", chunk.Start, chunk.End))
		args := rewriteChunkArgs(originalArgs, chunk, chunkOutput)
		fmt.Printf("=== Chunk %d/%d: %d - %d ===\n", index+1, len(chunks), chunk.Start, chunk.End)
		command := exec.CommandContext(ctx, executable, args...)
		command.Stdin = os.Stdin
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		command.Env = os.Environ()
		chunkStarted := time.Now()
		if err := command.Run(); err != nil {
			return fmt.Errorf("chunk %d-%d after %s: %w", chunk.Start, chunk.End, time.Since(chunkStarted).Round(time.Millisecond), err)
		}
		fmt.Printf("Chunk %d-%d completed in %s\n\n", chunk.Start, chunk.End, time.Since(chunkStarted).Round(time.Millisecond))
	}
	fmt.Printf("Chunked backfill completed in %s\n", time.Since(started).Round(time.Millisecond))
	return nil
}

func rewriteChunkArgs(args []string, chunk ledgerChunk, outputDir string) []string {
	remove := map[string]bool{"start": true, "end": true, "output": true, "chunk-size": true}
	result := make([]string, 0, len(args)+4)
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if !strings.HasPrefix(arg, "--") {
			result = append(result, arg)
			continue
		}
		name := strings.TrimPrefix(arg, "--")
		if equals := strings.IndexByte(name, '='); equals >= 0 {
			name = name[:equals]
			if remove[name] {
				continue
			}
			result = append(result, arg)
			continue
		}
		if remove[name] {
			if index+1 < len(args) && !strings.HasPrefix(args[index+1], "--") {
				index++
			}
			continue
		}
		result = append(result, arg)
	}
	return append(result,
		"--start", strconv.FormatUint(uint64(chunk.Start), 10),
		"--end", strconv.FormatUint(uint64(chunk.End), 10),
		"--output", outputDir,
		"--chunk-size", "0",
	)
}
