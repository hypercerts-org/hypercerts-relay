package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/obs"
	"golang.org/x/sync/errgroup"
)

// runBootstrap is the orchestrator's State 0. It builds:
//
//   - a shared ingest.Writer pointed at <DataDir>/segments (used by
//     the backfill engine; closed in State 4 of the cutover),
//   - the backfill engine itself,
//   - a live.Consumer pointed at <DataDir>/backfill/live_segments
//     with the throwaway "live_segments/seq/next" seq counter and
//     the shared "relay/cursor" upstream cursor.
//
// It runs the backfill engine and the live consumer as siblings
// under an internal errgroup, with the live consumer attached to a
// derived context the orchestrator can cancel independently. When
// backfill drains (returns nil), runBootstrap walks the cutover:
//
//  1. State 1: WritePhase(merging) inside the backfill goroutine.
//  2. State 2: cancel the live consumer's derived context, await
//     its Run return via g.Wait().
//  3. State 3: Close the live consumer (persists cursor + flushes),
//     then re-open the live_segments dir and SealActiveAndClose its
//     trailing active segment so the tree is fully sealed.
//  4. State 4: Close the backfill writer (flush only — steady-state
//     will reopen the same directory).
//
// On success, runBootstrap returns nil and the caller falls through
// to the merge case. On any subsystem error before backfill drains,
// the errgroup cancels both and the error is returned without
// touching the phase.
func (o *Orchestrator) runBootstrap(ctx context.Context) error {
	// hypercerts: Keep the decomposed bootstrap recovery flow explicit while
	// direct-PDS verification is enforced at the durable writer boundary.
	return obs.Span(ctx, func(ctx context.Context) error {
		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
		liveSegmentsDir := filepath.Join(o.cfg.DataDir, "backfill", "live_segments")
		bw, err := o.openBootstrapWriter(segmentsDir)
		if err != nil {
			return fmt.Errorf("orchestrator: open backfill ingest writer: %w", err)
		}
		bootstrapLive, err := o.openBootstrapLive(liveSegmentsDir)
		if err != nil {
			o.closeBootstrapWriterAfterLiveOpenFailure(ctx, bw)
			return fmt.Errorf("orchestrator: open bootstrap-live consumer: %w", err)
		}
		if err := o.runBootstrapWorkers(ctx, bw, bootstrapLive); err != nil {
			o.cleanupBootstrapAfterError(ctx, bootstrapLive, bw)
			return err
		}
		return o.finishBootstrap(ctx, bootstrapLive, bw, liveSegmentsDir)
	})
}

func (o *Orchestrator) openBootstrapWriter(segmentsDir string) (*ingest.Writer, error) {
	return ingest.Open(ingest.Config{
		// hypercerts: Gate direct-PDS bootstrap materialization before durable storage.
		CollectionPolicy:       o.cfg.CollectionPolicy,
		SegmentsDir:            segmentsDir,
		DataDir:                o.cfg.DataDir,
		FS:                     o.cfg.FS,
		Store:                  o.cfg.Store,
		Logger:                 o.cfg.Logger,
		Metrics:                o.cfg.IngestMetrics,
		SegmentMetrics:         o.cfg.SegmentMetrics,
		AsyncFlushWorkers:      o.cfg.BackfillAsyncFlushWorkers,
		OnAfterSeal:            o.cfg.IngestOnAfterSeal,
		SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
	})
}

func (o *Orchestrator) openBootstrapLive(liveSegmentsDir string) (*live.Consumer, error) {
	return live.Open(live.Config{
		// hypercerts: Bootstrap and restart share the same persisted collection policy.
		CollectionPolicy:  o.cfg.CollectionPolicy,
		DataDir:           o.cfg.DataDir,
		SegmentsDir:       liveSegmentsDir,
		FS:                o.cfg.FS,
		Store:             o.cfg.Store,
		SeqKey:            live.BootstrapSeqKey,
		CursorKey:         live.CursorKey,
		RelayURL:          o.cfg.RelayURL,
		Logger:            o.cfg.Logger,
		Metrics:           o.cfg.LiveMetrics,
		DropMetrics:       o.cfg.DropMetrics,
		Verifier:          o.cfg.Verifier,
		SyncStateStore:    o.cfg.SyncStateStore,
		MaxSegmentBytes:   o.cfg.BootstrapLiveMaxSegmentBytes,
		MaxEventsPerBlock: o.cfg.BootstrapLiveMaxEventsPerBlock,
		SegmentMetrics:    o.cfg.SegmentMetrics,
		OnEvent:           o.cfg.OnBootstrapLiveEvent,
		ReconnectBackoff:  o.cfg.LiveReconnectBackoff,
		Dial:              o.cfg.LiveDial,

		SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
	})
}

func (o *Orchestrator) closeBootstrapWriterAfterLiveOpenFailure(ctx context.Context, bw *ingest.Writer) {
	if err := bw.Close(); err != nil {
		o.logger.WarnContext(ctx, "backfill writer close after bootstrap-live open failure", "err", err)
	}
}

func (o *Orchestrator) cleanupBootstrapAfterError(ctx context.Context, bootstrapLive *live.Consumer, bw *ingest.Writer) {
	// Best-effort cleanup. Close errors are logged, not returned, because the
	// worker error is what callers need surfaced.
	if err := bootstrapLive.Close(); err != nil {
		o.logger.WarnContext(ctx, "bootstrap-live close after error", "err", err)
	}
	if err := bw.Close(); err != nil {
		o.logger.WarnContext(ctx, "backfill writer close after error", "err", err)
	}
}

func (o *Orchestrator) runBootstrapWorkers(ctx context.Context, bw *ingest.Writer, bootstrapLive *live.Consumer) error {
	g, gctx := errgroup.WithContext(ctx)
	// Wrapping gctx lets an errgroup failure cancel live processing while still
	// allowing a clean backfill drain to stop only the bootstrap-live consumer.
	liveCtx, cancelLive := context.WithCancel(gctx)
	defer cancelLive()

	// The backfill worker stores this immediately before cancellation. The live
	// worker reads it after observing liveCtx.Done(), so an atomic keeps a future
	// change to that ordering race-free.
	var drainStartUnixNano atomic.Int64
	g.Go(func() error { return o.runBootstrapBackfill(gctx, bw, cancelLive, &drainStartUnixNano) })
	g.Go(func() error { return o.runBootstrapLive(ctx, liveCtx, bootstrapLive, &drainStartUnixNano) })
	return g.Wait()
}

func (o *Orchestrator) runBootstrapBackfill(ctx context.Context, bw *ingest.Writer, cancelLive context.CancelFunc, drainStartUnixNano *atomic.Int64) error {
	err := backfill.Run(ctx, backfill.Config{
		Store:             o.cfg.Store,
		HTTPClient:        o.cfg.HTTPClient,
		Writer:            bw,
		RelayURL:          o.cfg.RelayURL,
		Logger:            o.cfg.Logger,
		Metrics:           o.cfg.BackfillMetrics,
		DropMetrics:       o.cfg.DropMetrics,
		MaxRepos:          o.cfg.MaxBackfillRepos,
		GlobalDownloads:   o.cfg.BackfillGlobalDownloads,
		HostWorkers:       o.cfg.BackfillHostWorkers,
		MaxActiveHosts:    o.cfg.BackfillMaxActiveHosts,
		MaxHosts:          o.cfg.BackfillMaxHosts,
		BackfillWorkers:   o.cfg.BackfillWorkers,
		NewHostClient:     o.cfg.BackfillNewHostClient,
		BackfillBatchSize: o.cfg.BackfillBatchSize,
		BackfillRepos:     o.cfg.BackfillRepos,
		IdentityResolver:  o.cfg.Directory.Resolver,
		// hypercerts: Bootstrap CAR verification shares the runtime live-verifier directory.
		Directory:         o.cfg.Directory,
		RetryBaseDelay:    o.cfg.BackfillRetryBaseDelay,
		AfterRepoComplete: o.cfg.AfterRepoComplete,
		CrashInjector:     o.cfg.CrashInjector,
	})
	if err != nil {
		return err
	}
	// Write phase=merging before cancelling live processing: this is the durable
	// commit point for a clean backfill drain.
	o.logger.InfoContext(ctx, "cutover begin")
	if err := o.writeMergingPhase(); err != nil {
		return err
	}
	if o.cfg.BarrierBeforeCutover != nil {
		if err := o.cfg.BarrierBeforeCutover(ctx); err != nil {
			return fmt.Errorf("orchestrator: before-cutover barrier: %w", err)
		}
	}
	drainStartUnixNano.Store(time.Now().UnixNano())
	cancelLive()
	return nil
}

func (o *Orchestrator) runBootstrapLive(parentCtx, liveCtx context.Context, bootstrapLive *live.Consumer, drainStartUnixNano *atomic.Int64) error {
	err := bootstrapLive.Run(liveCtx)
	if err != nil && errors.Is(err, context.Canceled) && liveCtx.Err() != nil && parentCtx.Err() == nil {
		if startNs := drainStartUnixNano.Load(); startNs != 0 {
			o.cfg.Metrics.observeState("drain_bootstrap", time.Since(time.Unix(0, startNs)).Seconds())
		}
		return nil
	}
	return err
}

// finishBootstrap drives States 3 and 4. Split out so the success
// path's cleanup pattern is clear and uniform: every resource gets a
// best-effort termination, the first error is reported, subsequent
// errors are logged.
//
// ctx is the parent runBootstrap context (NOT a fresh
// context.Background); a fresh ctx would orphan finishBootstrap's
// span from the runBootstrap span tree and break trace lineage.
func (o *Orchestrator) finishBootstrap(ctx context.Context, bootstrapLive *live.Consumer, bw *ingest.Writer, liveSegmentsDir string) error {
	return obs.Span(ctx, func(ctx context.Context) (retErr error) {
		// State 4 cleanup runs last (LIFO). bw.Close persists nextSeq for
		// the data/segments directory.
		defer func() {
			closeStart := time.Now()
			err := bw.Close()
			o.cfg.Metrics.observeState("close_backfill", time.Since(closeStart).Seconds())
			if err != nil {
				if retErr == nil {
					retErr = fmt.Errorf("orchestrator: close backfill ingest writer: %w", err)
				} else {
					o.logger.WarnContext(ctx, "backfill writer close failed after earlier error", "err", err)
				}
				return
			}
		}()

		// State 3: flush the bootstrap-live consumer and seal its trailing
		// active segment. Close() persists the upstream cursor and the
		// throwaway seq counter; the seal-via-reopen below finalizes the
		// active file's footer + header so the live_segments tree is
		// fully sealed once steady-state begins.
		//
		// If the bootstrap consumer's underlying writer happened to seal
		// its active file during normal rotation just before we got here,
		// ingest.Open rolls forward to a fresh empty seg_<N+1>.jss and
		// SealActiveAndClose seals that empty file. The compactor
		// reads zero events from such a file and ignores it.
		start := time.Now()
		if err := bootstrapLive.Close(); err != nil {
			return fmt.Errorf("orchestrator: close bootstrap-live consumer: %w", err)
		}
		if err := o.simulateCrash(ctx, crashpoint.AfterBootstrapLiveCloseBeforeSeal); err != nil {
			return err
		}

		sealW, err := ingest.Open(ingest.Config{
			SegmentsDir: liveSegmentsDir,
			DataDir:     o.cfg.DataDir,
			FS:          o.cfg.FS,
			Store:       o.cfg.Store,
			SeqKey:      live.BootstrapSeqKey,
			// Bare cfg.Logger; ingest.Open sets its own component.
			Logger: o.cfg.Logger,
			// Metrics nil to match the bootstrap-live consumer convention
			// (live/consumer.go Open): bootstrap-time live writes are not
			// counted in steady-state ingest counters, and the trailing
			// seal is a continuation of that lifetime.
			Metrics: nil,
			// SegmentMetrics IS shared though — the seal_duration
			// histogram is a global concern and we want every
			// segment.Writer in the process recording into the same
			// series.
			SegmentMetrics:         o.cfg.SegmentMetrics,
			SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: re-open bootstrap-live writer for seal: %w", err)
		}
		if err := sealW.SealActiveAndClose(); err != nil {
			return fmt.Errorf("orchestrator: seal bootstrap-live segment: %w", err)
		}

		o.cfg.Metrics.observeState("seal_bootstrap", time.Since(start).Seconds())
		return nil
	})
}
