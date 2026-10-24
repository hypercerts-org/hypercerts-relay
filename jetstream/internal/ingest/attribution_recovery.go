package ingest

import (
	"errors"
	"fmt"
	"os"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
)

func RecoverAttributions(cfg Config, manager *attribution.Manager, domain string) error {
	cfg.Attribution = manager
	cfg.AttributionDomain = domain
	if manager == nil {
		return nil
	}
	if err := manager.ValidateBinding(cfg.Store, domain); err != nil {
		return err
	}
	return recoverAttributions(cfg)
}

func recoverAttributions(cfg Config) error {
	manager := cfg.Attribution
	seqs, err := manager.PreparedSeqs()
	if err != nil {
		return err
	}
	activation, err := manager.ActivationSeq()
	if err != nil {
		return err
	}
	if len(seqs) == 0 {
		return auditAttributions(cfg, activation)
	}
	wanted := make(map[uint64]struct{}, len(seqs))
	for _, seq := range seqs {
		wanted[seq] = struct{}{}
	}
	found := make(map[uint64]attribution.DurableEvent, len(seqs))
	visit := func(events []segment.Event) error {
		for _, ev := range events {
			if _, ok := wanted[ev.Seq]; !ok {
				continue
			}
			if _, dup := found[ev.Seq]; dup {
				return fmt.Errorf("duplicate durable sequence %d", ev.Seq)
			}
			found[ev.Seq] = attribution.DurableEvent{Exists: true, SegmentFingerprint: attribution.FingerprintSegment(ev)}
		}
		return nil
	}
	files, err := SegmentFilesFS(cfg.FS, cfg.SegmentsDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		for _, file := range files {
			scanErr := segment.ScanActiveFS(cfg.FS, file.Path, visit)
			if scanErr == nil || os.IsNotExist(scanErr) {
				continue
			}
			if !errors.Is(scanErr, segment.ErrSegmentSealed) {
				return fmt.Errorf("scan active %s: %w", file.Path, scanErr)
			}
			r, openErr := segment.Open(segment.ReaderConfig{Path: file.Path, FS: cfg.FS})
			if openErr != nil {
				return fmt.Errorf("open sealed %s: %w", file.Path, openErr)
			}
			for i := range r.Blocks() {
				events, decodeErr := r.DecodeBlock(i)
				if decodeErr != nil {
					_ = r.Close()
					return fmt.Errorf("decode sealed %s block %d: %w", file.Path, i, decodeErr)
				}
				if err := visit(events); err != nil {
					_ = r.Close()
					return err
				}
			}
			if err := r.Close(); err != nil {
				return err
			}
		}
	}
	if err := manager.Recover(func(seq uint64) (attribution.DurableEvent, error) {
		d, ok := found[seq]
		if !ok {
			return attribution.DurableEvent{Exists: false}, nil
		}
		return d, nil
	}, nil); err != nil {
		return err
	}
	activation, err = manager.ActivationSeq()
	if err != nil {
		return err
	}
	return auditAttributions(cfg, activation)
}

func auditAttributions(cfg Config, activation uint64) error {
	if activation == 0 {
		return nil
	}
	files, err := SegmentFilesFS(cfg.FS, cfg.SegmentsDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var missing uint64
	visit := func(events []segment.Event) error {
		for _, ev := range events {
			if ev.Seq >= activation && missing == 0 {
				if _, err := cfg.Attribution.Lookup(ev.Seq); err != nil {
					missing = ev.Seq
				}
			}
		}
		return nil
	}
	for _, file := range files {
		err := segment.ScanActiveFS(cfg.FS, file.Path, visit)
		if err == nil || os.IsNotExist(err) {
			continue
		}
		if !errors.Is(err, segment.ErrSegmentSealed) {
			return err
		}
		r, openErr := segment.Open(segment.ReaderConfig{Path: file.Path, FS: cfg.FS})
		if openErr != nil {
			return fmt.Errorf("open sealed %s: %w", file.Path, openErr)
		}
		for i := range r.Blocks() {
			events, e := r.DecodeBlock(i)
			if e != nil {
				_ = r.Close()
				return e
			}
			if e = visit(events); e != nil {
				_ = r.Close()
				return e
			}
		}
		if err := r.Close(); err != nil {
			return err
		}
	}
	finalActivation, err := cfg.Attribution.ActivationSeq()
	if err != nil {
		return err
	}
	if missing != 0 && missing >= finalActivation {
		return fmt.Errorf("durable sequence %d at or beyond attribution activation %d lacks metadata", missing, finalActivation)
	}
	return nil
}
