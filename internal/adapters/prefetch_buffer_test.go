package adapters

// The prefetch buffer is the loader's memory model: peak RAM is roughly
// chunk-concurrency x workers x this x ledger size. It was a package constant
// until a 100,000-ledger validation run showed the backfill is memory-bound
// before it is CPU-bound, and that the bound tightens with ledger size --
// 200 is sized for genesis, not for the Soroban tail.
//
// What these pin is that the configured number reaches the channel. A flag
// that is read but not applied is worse than no flag: it reports a memory
// budget the process does not keep.

import (
	"context"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"
)

func TestStreamSequentialRangeSizesTheChannelToTheRequest(t *testing.T) {
	read := func(uint32) ([]byte, error) { return nil, nil }

	cases := []struct {
		name string
		ask  int
		want int
	}{
		{"configured", 25, 25},
		{"zero means default", 0, domain.DefaultPrefetchBuffer},
		{"negative means default", -1, domain.DefaultPrefetchBuffer},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Cancelled up front: the capacity is the subject, not the
			// reading, and a live producer would race the assertion.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			if got := cap(StreamSequentialRange(ctx, 1, 10, read, c.ask)); got != c.want {
				t.Errorf("prefetch buffer %d produced a channel of capacity %d, want %d",
					c.ask, got, c.want)
			}
		})
	}
}

// The source must carry the number it was built with, not re-read the
// default. This is the path --prefetch-buffer actually takes for XDR input.
func TestXDRStreamSourceCarriesItsPrefetchBuffer(t *testing.T) {
	_, path := fixtures.LoadMeta(t, "pubnet-genesis-p1")

	source, err := NewXDRStreamSource(path, 25)
	if err != nil {
		t.Fatalf("NewXDRStreamSource: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if got := cap(source.StreamRange(ctx, 1, 10)); got != 25 {
		t.Errorf("StreamRange buffered %d ledgers, want the configured 25", got)
	}
}
