package ingest

import (
	"context"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/stellar-raw-ledger-origin/source/ledgerstream"
)

// SDKStreamSource delegates RPC and archive acquisition to the canonical
// stellar-raw-ledger-origin LedgerStream factory.
type SDKStreamSource struct {
	stream ledgerbackend.LedgerStream
}

func NewSDKStreamSource(config OrchestratorConfig) (*SDKStreamSource, error) {
	cfg := ledgerstream.ConfigFromEnv()
	cfg.NetworkPassphrase = config.NetworkPassphrase

	switch config.StorageType {
	case "RPC":
		cfg.Type = ledgerstream.TypeRPC
		cfg.RPCServerURL = config.Bucket
	case "GCS", "S3":
		cfg.Type = ledgerstream.TypeArchive
		cfg.ArchiveStorageType = config.StorageType
		cfg.ArchiveBucketName, cfg.ArchivePath = splitArchiveLocation(config.Bucket)
		cfg.LedgersPerFile = config.LedgersPerFile
		cfg.FilesPerPartition = config.FilesPerPartition
	case "CAPTIVE_CORE":
		cfg.Type = ledgerstream.TypeCaptiveCore
	default:
		return nil, fmt.Errorf("SDK stream does not support storage type %q", config.StorageType)
	}

	stream, err := ledgerstream.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create canonical ledger stream: %w", err)
	}
	return &SDKStreamSource{stream: stream}, nil
}

// splitArchiveLocation accepts the loader's existing bucket[/prefix] flag and
// maps it to the canonical stream's separate bucket and archive path fields.
func splitArchiveLocation(location string) (string, string) {
	trimmed := strings.Trim(strings.TrimSpace(location), "/")
	trimmed = strings.TrimPrefix(trimmed, "s3://")
	trimmed = strings.TrimPrefix(trimmed, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func (s *SDKStreamSource) StreamRange(ctx context.Context, start, end uint32) <-chan prefetchedLedger {
	ch := make(chan prefetchedLedger, defaultPrefetchBuffer)
	go func() {
		defer close(ch)
		expected := start
		for raw, streamErr := range s.stream.RawLedgers(ctx, ledgerbackend.BoundedRange(start, end)) {
			if streamErr != nil {
				sendPrefetched(ctx, ch, prefetchedLedger{sequence: expected, err: fmt.Errorf("source range [%d,%d] after ledger %d: %w", start, end, previousLedger(expected, start), streamErr)})
				return
			}

			var lcm xdr.LedgerCloseMeta
			if err := lcm.UnmarshalBinary(raw); err != nil {
				sendPrefetched(ctx, ch, prefetchedLedger{sequence: expected, err: fmt.Errorf("decode source ledger: %w", err)})
				return
			}
			sequence := uint32(lcm.LedgerSequence())
			if sequence != expected {
				sendPrefetched(ctx, ch, prefetchedLedger{sequence: sequence, err: fmt.Errorf("source ledger gap: got %d, want %d", sequence, expected)})
				return
			}
			if sequence > end {
				sendPrefetched(ctx, ch, prefetchedLedger{sequence: sequence, err: fmt.Errorf("source emitted ledger %d after bounded range end %d", sequence, end)})
				return
			}

			// LedgerStream yields borrowed memory valid only until the next step.
			data := append([]byte(nil), raw...)
			if !sendPrefetched(ctx, ch, prefetchedLedger{sequence: sequence, data: data}) {
				return
			}
			expected++
		}

		if ctx.Err() != nil {
			sendPrefetched(ctx, ch, prefetchedLedger{sequence: expected, err: ctx.Err()})
			return
		}
		if expected <= end {
			sendPrefetched(ctx, ch, prefetchedLedger{sequence: expected, err: fmt.Errorf("incomplete bounded range [%d,%d]: ended after ledger %d", start, end, previousLedger(expected, start))})
		}
	}()
	return ch
}

func sendPrefetched(ctx context.Context, ch chan<- prefetchedLedger, item prefetchedLedger) bool {
	select {
	case ch <- item:
		return true
	case <-ctx.Done():
		return false
	}
}

func previousLedger(expected, start uint32) uint32 {
	if expected == start {
		return 0
	}
	return expected - 1
}

func (s *SDKStreamSource) Close() error { return nil }
