package ingest

import (
	"errors"
	"fmt"
	"io/fs"
	"time"

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

func recoverAttributions(cfg Config) (resultErr error) {
	if cfg.Logger != nil {
		start := time.Now()
		defer func() {
			if resultErr != nil {
				cfg.Logger.Error("attribution recovery and audit failed", "duration", time.Since(start))
			} else {
				cfg.Logger.Info("attribution recovery and audit complete", "duration", time.Since(start))
			}
		}()
	}
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
	found, visit := preparedAttributionVisitor(seqs)
	if err := scanPreparedAttributions(cfg, visit); err != nil {
		return err
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
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	missing := uint64(0)
	visit := missingAttributionVisitor(cfg, activation, &missing)
	for _, file := range files {
		err := segment.ScanActiveFS(cfg.FS, file.Path, visit)
		if err == nil || errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if !errors.Is(err, segment.ErrSegmentSealed) {
			return err
		}
		if err := visitSealedAttributionSegment(cfg, file.Path, visit, false); err != nil {
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

func visitSealedAttributionSegment(cfg Config, path string, visit func([]segment.Event) error, recovery bool) error {
	r, err := segment.Open(segment.ReaderConfig{Path: path, FS: cfg.FS})
	if err != nil {
		return fmt.Errorf("open sealed %s: %w", path, err)
	}
	for i := range r.Blocks() {
		events, err := r.DecodeBlock(i)
		if err != nil {
			_ = r.Close()
			if recovery {
				return fmt.Errorf("decode sealed %s block %d: %w", path, i, err)
			}
			return err
		}
		if err := visit(events); err != nil {
			_ = r.Close()
			return err
		}
	}
	return r.Close()
}

func preparedAttributionVisitor(seqs []uint64) (map[uint64]attribution.DurableEvent, func([]segment.Event) error) {
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
	return found, visit
}

func scanPreparedAttributions(cfg Config, visit func([]segment.Event) error) error {
	files, err := SegmentFilesFS(cfg.FS, cfg.SegmentsDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, file := range files {
		scanErr := segment.ScanActiveFS(cfg.FS, file.Path, visit)
		if scanErr == nil || errors.Is(scanErr, fs.ErrNotExist) {
			continue
		}
		if !errors.Is(scanErr, segment.ErrSegmentSealed) {
			return fmt.Errorf("scan active %s: %w", file.Path, scanErr)
		}
		if err := visitSealedAttributionSegment(cfg, file.Path, visit, true); err != nil {
			return err
		}
	}
	return nil
}

func missingAttributionVisitor(cfg Config, activation uint64, missing *uint64) func([]segment.Event) error {
	visit := func(events []segment.Event) error {
		for _, ev := range events {
			if ev.Seq >= activation && *missing == 0 {
				if _, err := cfg.Attribution.Lookup(ev.Seq); err != nil {
					*missing = ev.Seq
				}
			}
		}
		return nil
	}
	return visit
}
