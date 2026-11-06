package ingest

import (
	"errors"
	"fmt"
	"os"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
)

// lookupProjectionSegment resolves an allocated sequence in the actual segment
// files. A row at another coordinate cannot satisfy a pending intent.
func lookupProjectionSegment(cfg Config, sequence uint64) (history.ProjectionRow, bool, error) {
	files, err := SegmentFilesFS(cfg.FS, cfg.SegmentsDir)
	if os.IsNotExist(err) {
		return history.ProjectionRow{}, false, nil
	}
	if err != nil {
		return history.ProjectionRow{}, false, err
	}
	var found history.ProjectionRow
	visit := func(events []segment.Event) error {
		for _, ev := range events {
			if ev.Seq != sequence {
				continue
			}
			if found.SegmentSequence != 0 {
				return fmt.Errorf("duplicate durable sequence %d", sequence)
			}
			found = history.ProjectionRow{SegmentSequence: ev.Seq, Fingerprint: attribution.FingerprintSegment(ev)}
		}
		return nil
	}
	for _, file := range files {
		scanErr := segment.ScanActiveFS(cfg.FS, file.Path, visit)
		if scanErr == nil || os.IsNotExist(scanErr) {
			continue
		}
		if !errors.Is(scanErr, segment.ErrSegmentSealed) {
			return history.ProjectionRow{}, false, fmt.Errorf("scan %s: %w", file.Path, scanErr)
		}
		r, openErr := segment.Open(segment.ReaderConfig{Path: file.Path, FS: cfg.FS})
		if openErr != nil {
			return history.ProjectionRow{}, false, openErr
		}
		for i := range r.Blocks() {
			events, decodeErr := r.DecodeBlock(i)
			if decodeErr != nil {
				_ = r.Close()
				return history.ProjectionRow{}, false, decodeErr
			}
			if err := visit(events); err != nil {
				_ = r.Close()
				return history.ProjectionRow{}, false, err
			}
		}
		if err := r.Close(); err != nil {
			return history.ProjectionRow{}, false, err
		}
	}
	return found, found.SegmentSequence != 0, nil
}
