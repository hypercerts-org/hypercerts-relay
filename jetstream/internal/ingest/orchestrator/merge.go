// merge.go owns the State 5 cutover step that
// drains data/backfill/live_segments/ into data/segments/. Spec:
// specs/notes/2026-05-27-merge-phase-design.md.

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
)

// runMerge is the cutover state machine's State 5: drain
// data/backfill/live_segments/ into data/segments/. Per the spec:
//
//  1. Restart guard: if data/backfill/live_segments/ is gone, the
//     prior run finished cleanup; just delete the cursor keys (they
//     may still be set if the prior run died between RemoveAll and
//     the deletes) and return.
//  2. Open the destination ingest.Writer on data/segments/ with
//     SeqKey=live.SteadySeqKey so survivors continue monotonically
//     from where backfill left off.
//  3. Build a mergeRunner and drive its drain loop. On error, best-
//     effort Close the dst writer (NOT seal — partial-merge active
//     must not be marked terminally sealed).
//  4. On success: SealActiveAndClose the dst writer, run new-DID
//     discovery (listRepos resume), RemoveAll the backfill tree,
//     delete both cursor keys.
func (o *Orchestrator) runMerge(ctx context.Context) error {
	return obs.Span(ctx, o.runMergePhases)
}

func (o *Orchestrator) runMergePhases(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	start := time.Now()
	defer func() { o.cfg.Metrics.observeState("merge", time.Since(start).Seconds()) }()

	liveSegmentsDir := filepath.Join(o.cfg.DataDir, "backfill", "live_segments")
	segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
	cleaned, err := o.prepareMergeSource(ctx, liveSegmentsDir)
	if err != nil {
		return err
	}
	if cleaned {
		return nil
	}
	dst, err := o.openMergeDestination(segmentsDir)
	if err != nil {
		return fmt.Errorf("orchestrator: merge: open dst writer: %w", err)
	}
	runner := newMergeRunner(dst, o.cfg.Store, liveSegmentsDir, o.cfg.FS, o.cfg.Logger, o.cfg.Metrics, o.cfg.CrashInjector)
	if err := o.runMergeIntoDestination(ctx, dst, runner, segmentsDir); err != nil {
		return err
	}
	if err := o.runMergeDiscovery(ctx, runner); err != nil {
		return err
	}
	if err := o.simulateCrash(ctx, crashpoint.AfterMergeDiscoveryBeforeCleanup); err != nil {
		return err
	}
	return o.cleanupMergedBackfill(ctx)
}

func (o *Orchestrator) prepareMergeSource(ctx context.Context, liveSegmentsDir string) (bool, error) {
	cleaned, err := o.mergeRestartAfterCleanup(ctx, liveSegmentsDir)
	if err != nil || cleaned {
		return cleaned, err
	}
	if err := o.sealActiveMergeSource(ctx, liveSegmentsDir); err != nil {
		return false, err
	}
	return false, nil
}

// mergeRestartAfterCleanup handles a restart after the prior process removed
// the source tree. The data directory must be synced before durable cursor
// deletion, or a power loss could make the tree reappear with its cursors gone.
func (o *Orchestrator) mergeRestartAfterCleanup(ctx context.Context, liveSegmentsDir string) (bool, error) {
	_, err := statStorageFS(o.cfg.FS, liveSegmentsDir)
	if isStorageNotExist(err) {
		if err := syncStorageDirFS(o.cfg.FS, o.cfg.DataDir); err != nil {
			return false, fmt.Errorf("orchestrator: merge: sync data dir in restart-after-cleanup guard: %w", err)
		}
		if err := deleteMergeCursor(o.cfg.Store); err != nil {
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("orchestrator: merge: stat live_segments: %w", err)
	}
	return false, nil
}

func (o *Orchestrator) openMergeDestination(segmentsDir string) (*ingest.Writer, error) {
	return ingest.Open(ingest.Config{
		SegmentsDir:            segmentsDir,
		DataDir:                o.cfg.DataDir,
		FS:                     o.cfg.FS,
		Store:                  o.cfg.Store,
		SeqKey:                 live.SteadySeqKey,
		Logger:                 o.cfg.Logger,
		Metrics:                o.cfg.IngestMetrics,
		SegmentMetrics:         o.cfg.SegmentMetrics,
		OnAfterSeal:            o.cfg.IngestOnAfterSeal,
		SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
	})
}

func (o *Orchestrator) runMergeIntoDestination(ctx context.Context, dst *ingest.Writer, runner *mergeRunner, segmentsDir string) error {
	if err := initCompactionWatermarkFloor(o.cfg.Store, dst.NextSeq()); err != nil {
		return o.closeMergeDestinationAfter(ctx, dst, "dst writer close after compaction watermark init failure", err)
	}
	if err := runner.run(ctx); err != nil {
		return o.closeMergeDestinationAfter(ctx, dst, "dst writer close after merge error", err)
	}
	if err := o.runMergePendingRepoRetry(ctx, dst); err != nil {
		return o.closeMergeDestinationAfter(ctx, dst, "dst writer close after pending retry error", err)
	}
	if err := dst.SealActiveAndClose(); err != nil {
		return fmt.Errorf("orchestrator: merge: seal dst: %w", err)
	}
	return o.finishMergedDestination(ctx, segmentsDir)
}

func (o *Orchestrator) closeMergeDestinationAfter(ctx context.Context, dst *ingest.Writer, message string, err error) error {
	if closeErr := dst.Close(); closeErr != nil {
		o.logger.WarnContext(ctx, message, "err", closeErr)
	}
	return err
}

// runMergePendingRepoRetry repairs pre-existing not_started rows only after
// the captured live tail has merged, so replacement rows sort above any stale
// account tombstones replayed from live_segments.
func (o *Orchestrator) runMergePendingRepoRetry(ctx context.Context, dst *ingest.Writer) error {
	err := backfill.RunPendingRepoRetryPass(ctx, backfill.RetryConfig{
		Store:         o.cfg.Store,
		Writer:        dst,
		HTTPClient:    o.cfg.HTTPClient,
		RelayURL:      o.cfg.RelayURL,
		Logger:        o.cfg.Logger,
		Metrics:       o.cfg.BackfillMetrics,
		DropMetrics:   o.cfg.DropMetrics,
		NewHostClient: o.cfg.BackfillNewHostClient,
		// hypercerts: Merge recovery retry shares the runtime live-verifier directory.
		Directory:   o.cfg.Directory,
		Interval:    o.cfg.FailedRepoRetryInterval,
		Workers:     o.cfg.FailedRepoRetryWorkers,
		HostWorkers: o.cfg.FailedRepoRetryHostWorkers,
		MaxDelay:    o.cfg.FailedRepoRetryMaxDelay,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: merge: pending repo retry: %w", err)
	}
	return nil
}

func (o *Orchestrator) finishMergedDestination(ctx context.Context, segmentsDir string) error {
	if err := o.runDeleteCompaction(ctx, compactionMergeTail, nil); err != nil {
		return fmt.Errorf("orchestrator: merge-tail compaction: %w", err)
	}
	// The merge-tail pass is manifest-oblivious, so reconcile every manifest
	// entry with its on-disk header before serving ungates the transition.
	if err := o.reconcileCompactionManifestFromDisk(segmentsDir); err != nil {
		return fmt.Errorf("orchestrator: merge-tail compaction manifest reconcile: %w", err)
	}
	return o.simulateCrash(ctx, crashpoint.AfterMergeDstSealBeforeDiscovery)
}

func (o *Orchestrator) runMergeDiscovery(ctx context.Context, runner *mergeRunner) error {
	if o.cfg.SkipMergeDiscovery {
		return nil
	}
	limits := discoveryLimits{
		maxHosts:       o.cfg.BackfillMaxHosts,
		maxActiveHosts: o.cfg.BackfillMaxActiveHosts,
		retryDelay:     o.cfg.MergeDiscoveryRetryBaseDelay,
	}
	return runner.runDiscoveryWithClient(ctx, o.cfg.RelayURL, o.cfg.HTTPClient, o.cfg.BackfillNewHostClient, limits)
}

func (o *Orchestrator) cleanupMergedBackfill(ctx context.Context) error {
	if err := removeAllStorageFS(o.cfg.FS, filepath.Join(o.cfg.DataDir, "backfill")); err != nil {
		return fmt.Errorf("orchestrator: merge: remove backfill dir: %w", err)
	}
	// deleteMergeCursor uses SyncWrites. Sync the source-tree removal first so a
	// power loss cannot retain the tree while its cursors have been deleted.
	if err := syncStorageDirFS(o.cfg.FS, o.cfg.DataDir); err != nil {
		return fmt.Errorf("orchestrator: merge: sync data dir after backfill removal: %w", err)
	}
	if err := deleteMergeCursor(o.cfg.Store); err != nil {
		return err
	}
	return o.simulateCrash(ctx, crashpoint.AfterMergeCleanupComplete)
}

// sealActiveMergeSource ensures the trailing source segment is sealed
// before the drain loop reads it. Normally finishBootstrap seals it at
// cutover, but a crash at crashpoint.AfterBootstrapLiveCloseBeforeSeal
// can leave it active. Only the latest segment can be unsealed: the
// bootstrap-live writer holds exactly one active segment and rotation
// seals the old file before opening the next, so checking
// files[len-1] is sufficient. Idempotent — a no-op when the trailing
// segment is already sealed.
func (o *Orchestrator) sealActiveMergeSource(ctx context.Context, liveSegmentsDir string) error {
	files, err := ingest.SegmentFilesFS(o.cfg.FS, liveSegmentsDir)
	if err != nil {
		return fmt.Errorf("orchestrator: merge: list source segments before seal guard: %w", err)
	}
	if len(files) == 0 {
		return nil
	}

	latest := files[len(files)-1]
	rd, err := segment.Open(segment.ReaderConfig{Path: latest.Path, FS: o.cfg.FS})
	if err == nil {
		return rd.Close()
	}
	if !errors.Is(err, segment.ErrActiveSegment) {
		return fmt.Errorf("orchestrator: merge: inspect source segment %s: %w", latest.Path, err)
	}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:            liveSegmentsDir,
		DataDir:                o.cfg.DataDir,
		FS:                     o.cfg.FS,
		Store:                  o.cfg.Store,
		SeqKey:                 live.BootstrapSeqKey,
		Logger:                 o.cfg.Logger,
		Metrics:                nil,
		SegmentMetrics:         o.cfg.SegmentMetrics,
		MaxSegmentBytes:        0,
		SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: merge: reopen active source for seal: %w", err)
	}
	if err := w.SealActiveAndClose(); err != nil {
		return fmt.Errorf("orchestrator: merge: seal active source: %w", err)
	}
	o.logger.InfoContext(ctx, "sealed active bootstrap-live source before merge", "segment", latest.Idx)
	return nil
}
