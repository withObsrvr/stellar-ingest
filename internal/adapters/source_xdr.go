package adapters

// The framed-XDR source: a file produced by `nebu fetch`, read into memory and
// served by ledger sequence.
//
// It lived in worker.go, which put a source inside the orchestration layer --
// the layering made that visible rather than a matter of taste, because an
// adapters test could not reach it without importing jobs and closing a
// cycle. A source belongs with the other sources.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
)

// XDRStreamSource reads a framed XDR stream file (output of `nebu fetch`)
// and indexes all ledgers into memory for random access by sequence.
type XDRStreamSource struct {
	ledgers        map[uint32][]byte
	prefetchBuffer int
}

// NewXDRStreamSource reads a nebu-style framed XDR file and indexes all ledgers.
//
// prefetchBuffer sizes the read-ahead channel StreamRange hands out; zero
// takes domain.DefaultPrefetchBuffer, which is what a caller with no opinion
// about memory passes.
func NewXDRStreamSource(filePath string, prefetchBuffer int) (*XDRStreamSource, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open xdr stream: %w", err)
	}
	defer f.Close()

	source := &XDRStreamSource{
		ledgers:        make(map[uint32][]byte),
		prefetchBuffer: prefetchBuffer,
	}

	for {
		var lcm xdr.LedgerCloseMeta
		_, err := xdr.Unmarshal(f, &lcm)
		if err != nil {
			// The XDR decoder wraps io.EOF rather than returning it bare, so a
			// clean end of stream must be detected with errors.Is. Comparing
			// with == made every complete file look like a decode failure.
			if errors.Is(err, io.EOF) {
				break // clean end of stream
			}
			return nil, fmt.Errorf("XDR stream decode error: %w", err)
		}

		seq := uint32(lcm.LedgerSequence())
		raw, err := lcm.MarshalBinary()
		if err != nil {
			log.Printf("WARN: failed to re-marshal ledger %d: %v", seq, err)
			continue
		}
		source.ledgers[seq] = raw
	}

	if len(source.ledgers) == 0 {
		return nil, fmt.Errorf("no ledgers found in %s", filePath)
	}

	log.Printf("XDRStreamSource: loaded %d ledgers from %s", len(source.ledgers), filePath)
	return source, nil
}

func (s *XDRStreamSource) GetLedger(sequence uint32) ([]byte, error) {
	data, ok := s.ledgers[sequence]
	if !ok {
		return nil, fmt.Errorf("ledger %d not found in stream", sequence)
	}
	return data, nil
}

func (s *XDRStreamSource) StreamRange(ctx context.Context, start, end uint32) <-chan domain.PrefetchedLedger {
	return StreamSequentialRange(ctx, start, end, s.GetLedger, s.prefetchBuffer)
}

func (s *XDRStreamSource) Close() error { return nil }

// Shared by both sources, which is why it sits with them rather than with the
// worker that drains the channel they fill.
// StreamSequentialRange adapts random-access local inputs to the same bounded
// stream contract used by the SDK acquisition sources.
func StreamSequentialRange(ctx context.Context, start, end uint32, read func(uint32) ([]byte, error), prefetchBuffer int) <-chan domain.PrefetchedLedger {
	ch := make(chan domain.PrefetchedLedger, domain.PrefetchBufferOrDefault(prefetchBuffer))

	go func() {
		defer close(ch)

		for seq := start; seq <= end; seq++ {
			// Check for cancellation before each fetch
			select {
			case <-ctx.Done():
				ch <- domain.PrefetchedLedger{Sequence: seq, Err: ctx.Err()}
				return
			default:
			}

			data, err := read(seq)

			select {
			case ch <- domain.PrefetchedLedger{Sequence: seq, Data: data, Err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

// Sequences returns the ledger sequences this source holds, in order.
//
// Exported so a test in another package can iterate the fixture
// deterministically. It used to reach into the map directly, which only
// worked while everything shared one package.
func (s *XDRStreamSource) Sequences() []uint32 {
	out := make([]uint32, 0, len(s.ledgers))
	for seq := range s.ledgers {
		out = append(out, seq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
