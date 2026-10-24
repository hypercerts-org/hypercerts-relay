package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// seqNextKey is the legacy default value for Config.SeqKey. Kept
// as a constant to anchor the back-compat behavior of fresh data
// dirs created before SeqKey existed.
const seqNextKey = "seq/next"

// Writer owns the active segment file and the seq counter. It is
// safe for concurrent use.

type Writer struct {
	cfg Config

	// drainMu is an admission barrier for appends and explicit flushes. Acquire
	// it before mu, and hold it through async job submission so DrainDurability
	// can safely wait for all already-admitted jobs without racing future Add.
	drainMu        sync.Mutex
	mu             sync.Mutex
	active         *segment.Writer
	activeBytes    int64
	activeIdx      uint64
	nextSeq        uint64
	durableNextSeq uint64
	readLog        *ReadableLog
	closed         bool

	pendingAttributions []attribution.Prepared

	async            *asyncFlushPipeline
	asyncJobs        sync.WaitGroup
	nextAsyncFlushID uint64
}

// Open scans cfg.SegmentsDir, resumes or creates the active segment,
// and reconciles seq/next against any events in the resumed file so
// a crash between block fsync and pebble batch commit can never
// produce duplicate seq numbers.
func Open(cfg Config) (*Writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Attribution != nil {
		if err := cfg.Attribution.ValidateBinding(cfg.Store, cfg.AttributionDomain); err != nil {
			return nil, fmt.Errorf("ingest: attribution binding: %w", err)
		}
		if err := recoverAttributions(cfg); err != nil {
			return nil, fmt.Errorf("ingest: recover attribution: %w", err)
		}
	}
	cfg.applyDefaults()
	cfg.Logger = cfg.Logger.With(slog.String("component", "ingest/writer"))

	if err := mkdirAllFS(cfg.FS, cfg.SegmentsDir, 0o755); err != nil {
		return nil, cfg.wrapSegmentPersistenceError("creating segments directory", err)
	}
	idx, hasExisting, err := scanSegmentsDir(cfg.FS, cfg.SegmentsDir)
	if err != nil {
		return nil, err
	}
	w := &Writer{cfg: cfg, activeIdx: idx}
	var maxSeq uint64
	var foundEvents bool
	if hasExisting {
		path := filepath.Join(cfg.SegmentsDir, SegmentFilename(idx))
		seg, segErr := segment.New(segment.Config{Path: path, FS: cfg.FS, MaxEventsPerBlock: cfg.MaxEventsPerBlock, Metrics: cfg.SegmentMetrics, IOFaultInjector: cfg.SegmentIOFaultInjector})
		switch {
		case segErr == nil:
			w.active = seg
			info, statErr := statFS(cfg.FS, path)
			if statErr != nil {
				_ = seg.Close()
				return nil, fmt.Errorf("ingest: stat %s: %w", path, statErr)
			}
			w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)
			maxSeq, foundEvents, err = segment.ScanMaxSeqFS(cfg.FS, path)
			if err != nil {
				_ = seg.Close()
				return nil, fmt.Errorf("ingest: scan_max_seq %s: %w", path, err)
			}
		case errors.Is(segErr, segment.ErrSegmentSealed):
			r, openErr := segment.Open(segment.ReaderConfig{Path: path, FS: cfg.FS})
			if openErr != nil {
				return nil, fmt.Errorf("ingest: open sealed %s: %w", path, openErr)
			}
			hdr := r.Header()
			if closeErr := r.Close(); closeErr != nil {
				return nil, fmt.Errorf("ingest: close sealed %s: %w", path, closeErr)
			}
			if hdr.MaxSeq > 0 {
				maxSeq, foundEvents = hdr.MaxSeq, true
			}
			w.activeIdx = idx + 1
			path = filepath.Join(cfg.SegmentsDir, SegmentFilename(w.activeIdx))
			seg, segErr = segment.New(segment.Config{Path: path, FS: cfg.FS, MaxEventsPerBlock: cfg.MaxEventsPerBlock, Metrics: cfg.SegmentMetrics, IOFaultInjector: cfg.SegmentIOFaultInjector})
			if segErr != nil {
				return nil, cfg.wrapSegmentPersistenceError("opening next active segment", fmt.Errorf("ingest: open next segment %s: %w", path, segErr))
			}
			w.active, w.activeBytes = seg, 0
		default:
			return nil, cfg.wrapSegmentPersistenceError("opening existing active segment", fmt.Errorf("ingest: open existing %s: %w", path, segErr))
		}
	} else {
		path := filepath.Join(cfg.SegmentsDir, SegmentFilename(0))
		seg, segErr := segment.New(segment.Config{Path: path, FS: cfg.FS, MaxEventsPerBlock: cfg.MaxEventsPerBlock, Metrics: cfg.SegmentMetrics, IOFaultInjector: cfg.SegmentIOFaultInjector})
		if segErr != nil {
			return nil, cfg.wrapSegmentPersistenceError("creating active segment", fmt.Errorf("ingest: create %s: %w", path, segErr))
		}
		w.active, w.activeBytes = seg, 0
	}
	if !foundEvents && hasExisting {
		tailMax, tailFound, tailErr := recoverSealedTailMaxSeq(cfg.FS, cfg.SegmentsDir, idx)
		if tailErr != nil {
			_ = w.active.Close()
			return nil, tailErr
		}
		if tailFound {
			maxSeq, foundEvents = tailMax, true
		}
	}
	pebbleSeq, err := loadNextSeq(cfg.Store, cfg.SeqKey)
	if err != nil {
		_ = w.active.Close()
		return nil, err
	}
	reconciled := pebbleSeq
	if foundEvents && maxSeq+1 > reconciled {
		reconciled = maxSeq + 1
	}
	if reconciled > pebbleSeq {
		if err := saveNextSeq(cfg.Store, cfg.SeqKey, reconciled); err != nil {
			_ = w.active.Close()
			return nil, cfg.wrapSegmentPersistenceError("reconciling durable seq metadata", err)
		}
	}
	if reconciled < 1 {
		reconciled = 1
	}
	w.nextSeq, w.durableNextSeq = reconciled, reconciled
	w.readLog = newReadableLog(reconciled, cfg.ReadLogRetentionBytes, cfg.Metrics)
	w.cfg.Metrics.setActiveSegBytes(w.activeBytes)
	w.cfg.Metrics.setNextSeq(w.nextSeq)
	if cfg.AsyncFlushWorkers > 0 {
		w.async = newAsyncFlushPipeline(w, cfg.AsyncFlushWorkers)
	}
	w.cfg.Logger.Info("opened", "segments_dir", cfg.SegmentsDir, "active_index", w.activeIdx, "active_bytes", w.activeBytes, "next_seq", w.nextSeq)
	return w, nil
}

// Close flushes any pending events, persists nextSeq to pebble, and
// closes the active writer file. Idempotent. Does NOT seal — that's
// a rotation-time concern.
//
// Durability ordering matches docs/README.md §3.1.1: segment.Writer.Close
// fsyncs any buffered block before we commit the pebble batch. If
// the pebble write fails after a successful flush, Open's
// ScanMaxSeq reconciliation will recover the correct nextSeq on
// next start.
func (w *Writer) Close() error {
	if w.async != nil {
		return w.closeAsync()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.active == nil {
		return nil
	}
	if err := w.active.Close(); err != nil {
		return w.wrapSegmentPersistenceError("closing active segment", fmt.Errorf("ingest: close active segment: %w", err))
	}
	if err := w.commitTerminalDurableBatchLocked(); err != nil {
		return err
	}
	return nil
}

// SealActiveAndClose flushes any pending block, seals the active
// segment file (writes the variable-length footer and finalizes the
// 256-byte fixed header), persists nextSeq, publishes OnAfterSeal,
// and closes the writer. Idempotent.
//
// Used by the orchestrator at cutover time to finalize the
// bootstrap-phase live_segments writer's trailing active file so the
// `backfill/live_segments/` tree is fully sealed once steady-state
// begins. Steady-state callers should continue to use Close()
// instead — sealing during normal operation is a rotation-time
// concern handled inside flushAndRotateLocked.
func (w *Writer) SealActiveAndClose() error {
	if w.async != nil {
		return w.sealActiveAndCloseAsync()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.active == nil {
		return nil
	}
	// segment.Writer.Seal handles flushing any pending events itself
	// (and is a no-op on an empty pending block), so we don't have to
	// pre-flush here. Seal closes the underlying file on success;
	// failure paths leave the file open with stickyErr latched, so we
	// release the fd ourselves below.
	//
	// Durability ordering matches Close (docs/README.md §3.1.1): the segment
	// fsyncs first, then we pebble.Sync nextSeq. A crash between the
	// two leaves nextSeq lagging at most one block, which Open's
	// ScanMaxSeq reconciles on next start.
	if _, err := w.active.Seal(); err != nil {
		if cerr := w.active.Close(); cerr != nil {
			w.cfg.Logger.Warn("close after failed seal", "err", cerr)
		}
		return w.wrapSegmentPersistenceError("sealing active segment", fmt.Errorf("ingest: seal active segment: %w", err))
	}
	if err := w.commitTerminalDurableBatchLocked(); err != nil {
		return err
	}
	sealedIdx := w.activeIdx
	sealedPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(sealedIdx))
	if err := w.onAfterSealLocked(sealedIdx, sealedPath); err != nil {
		return err
	}
	return nil
}

// Append writes one event into the active segment. On success,
// mutates ev.Seq in place to the allocated value; on error ev.Seq
// is left untouched so callers can safely retry without observing
// a phantom allocation. Goroutine-safe.
// hypercerts: A policy-filtered row succeeds with Seq=0 and writes no bytes.
func (w *Writer) Append(ctx context.Context, ev *segment.Event) error {
	if w.cfg.Attribution != nil {
		return fmt.Errorf("ingest: attributed writer requires AppendAttributed metadata")
	}
	return w.append(ctx, ev, nil)
}

// AppendAttributed appends an event and its provenance in the same durable batch.
func (w *Writer) AppendAttributed(ctx context.Context, ev *segment.Event, metadata AttributionMetadata) error {
	if w.cfg.Attribution == nil {
		return fmt.Errorf("ingest: attribution manager is required")
	}
	if metadata.StreamID == "" || metadata.Policy.Revision == 0 {
		return fmt.Errorf("ingest: invalid attribution metadata: %w", attribution.ErrInvalid)
	}
	return w.append(ctx, ev, &metadata)
}

// AppendImported appends an event while retaining provenance from its source stream.
func (w *Writer) AppendImported(ctx context.Context, ev *segment.Event, source attribution.Event, domain string, destination AttributionMetadata, sourcePolicy ...attribution.Policy) error {
	if w.cfg.Attribution == nil || ev == nil || domain == "" || attribution.Validate(source) != nil || len(sourcePolicy) > 1 {
		return fmt.Errorf("ingest: invalid imported attribution: %w", attribution.ErrInvalid)
	}
	var policy *attribution.Policy
	if source.PolicyRev != nil {
		if len(sourcePolicy) != 1 || sourcePolicy[0].Revision != *source.PolicyRev {
			return fmt.Errorf("ingest: source policy required: %w", attribution.ErrInvalid)
		}
		copy := sourcePolicy[0]
		policy = &copy
	} else if len(sourcePolicy) != 0 {
		return fmt.Errorf("ingest: unexpected source policy: %w", attribution.ErrInvalid)
	}
	if attribution.ValidateSegment(*ev, destination.StreamID, destination.Policy, destination.Provenance, destination.Boundary) != nil {
		return fmt.Errorf("ingest: invalid destination attribution: %w", attribution.ErrInvalid)
	}
	metadata := destination
	imported := &attribution.ImportedSource{Domain: domain, StreamID: source.StreamID, Seq: source.Seq, PolicyRev: source.PolicyRev, Provenance: source.Provenance, Boundary: source.Boundary, Policy: policy, WitnessedAt: ev.WitnessedAt, IndexedAt: ev.IndexedAt, SegmentFingerprint: attribution.FingerprintSegment(*ev)}
	if source.Source != nil {
		imported.WitnessedAt = source.Source.WitnessedAt
		imported.IndexedAt = source.Source.IndexedAt
		imported.SegmentFingerprint = source.Source.SegmentFingerprint
	}
	metadata.Source = imported
	return w.append(ctx, ev, &metadata)
}

type AttributionMetadata struct {
	StreamID   string
	Policy     attribution.Policy
	Provenance attribution.Provenance
	Boundary   attribution.Boundary
	Source     *attribution.ImportedSource
}

type ImportedMetadata struct {
	Domain       string
	Source       attribution.Event
	SourcePolicy *attribution.Policy
	Destination  AttributionMetadata
}

func (w *Writer) append(ctx context.Context, ev *segment.Event, metadata *AttributionMetadata) error {
	if w.async != nil {
		w.drainMu.Lock()
		w.mu.Lock()
		job, err := w.appendAttributedLocked(ctx, ev, metadata)
		w.mu.Unlock()
		if err == nil {
			w.submitAsyncFlushes([]*asyncFlushJob{job})
		}
		w.drainMu.Unlock()
		if err != nil {
			return err
		}
		if err := w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job}); err != nil {
			return err
		}
		return w.rotateIfFull(ctx)
	}

	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	err := func() error {
		w.mu.Lock()
		defer w.mu.Unlock()
		_, err := w.appendAttributedLocked(ctx, ev, metadata)
		return err
	}()
	if err != nil {
		return err
	}
	return nil
}

// AppendBatch writes events after validating and preparing the complete batch.
func (w *Writer) AppendBatch(ctx context.Context, events []segment.Event) error {
	return w.appendBatch(ctx, events, nil)
}

// AppendBatchAttributed appends a batch with shared provenance and policy metadata.
func (w *Writer) AppendBatchAttributed(ctx context.Context, events []segment.Event, metadata AttributionMetadata) error {
	if w.cfg.Attribution == nil {
		return fmt.Errorf("ingest: attribution manager is required")
	}
	return w.appendBatch(ctx, events, &metadata)
}

func (w *Writer) appendBatch(ctx context.Context, events []segment.Event, metadata *AttributionMetadata) error {
	if len(events) == 0 {
		return nil
	}
	if w.async != nil {
		w.drainMu.Lock()
		w.mu.Lock()
		jobs, err := w.appendBatchCoreLocked(ctx, events, metadata)
		w.mu.Unlock()
		if err != nil {
			w.drainMu.Unlock()
			return err
		}
		w.submitAsyncFlushes(jobs)
		err = w.waitSubmittedAsyncFlushes(jobs)
		w.drainMu.Unlock()
		if err != nil {
			return err
		}
		return w.rotateIfFull(ctx)
	}
	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.appendBatchCoreLocked(ctx, events, metadata)
	return err
}

// appendBatchLocked appends while the caller owns both drainMu and mu.
func (w *Writer) appendBatchLocked(ctx context.Context, events []segment.Event) error {
	return w.appendBatchLockedWithMetadata(ctx, events, nil)
}

func (w *Writer) appendBatchAttributedLocked(ctx context.Context, events []segment.Event, metadata *AttributionMetadata) error {
	return w.appendBatchLockedWithMetadata(ctx, events, metadata)
}

func (w *Writer) appendBatchLockedWithMetadata(ctx context.Context, events []segment.Event, metadata *AttributionMetadata) error {
	jobs, err := w.appendBatchCoreLocked(ctx, events, metadata)
	if err != nil {
		return err
	}
	if w.async != nil {
		w.submitAsyncFlushes(jobs)
		return w.waitSubmittedAsyncFlushes(jobs)
	}
	return w.rotateIfFull(ctx)
}

// appendBatchCoreLocked prepares the full transaction before writing rows.
func (w *Writer) appendBatchCoreLocked(ctx context.Context, events []segment.Event, metadata *AttributionMetadata) ([]*asyncFlushJob, error) {
	if w.closed {
		return nil, ErrClosed
	}
	if w.cfg.Attribution != nil && metadata == nil {
		return nil, fmt.Errorf("ingest: attributed writer requires batch metadata")
	}
	if metadata != nil && (metadata.StreamID == "" || metadata.Policy.Revision == 0) {
		return nil, fmt.Errorf("ingest: invalid attribution metadata: %w", attribution.ErrInvalid)
	}
	type acceptedCandidate struct {
		event         segment.Event
		originalIndex int
		prepared      attribution.Prepared
	}
	candidates := make([]acceptedCandidate, 0, len(events))
	for i := range events {
		if err := segment.ValidateEvent(events[i]); err != nil {
			return nil, err
		}
		candidate := events[i]
		if !w.cfg.CollectionPolicy.Allows(&candidate) {
			continue
		}
		if superseded, err := w.snapshotSupersedes(&candidate, metadata); err != nil {
			return nil, err
		} else if superseded {
			continue
		}
		if metadata != nil && metadata.Source != nil && (metadata.Source.Seq != events[i].Seq || metadata.Source.SegmentFingerprint != attribution.FingerprintSegment(events[i])) {
			return nil, fmt.Errorf("ingest: imported source sequence mismatch: %w", attribution.ErrInvalid)
		}
		candidate.Seq = w.nextSeq + uint64(len(candidates))
		if w.cfg.TimestampStamper != nil {
			if err := w.cfg.TimestampStamper.Stamp(ctx, &candidate); err != nil {
				return nil, fmt.Errorf("ingest: timestamp stamp: %w", err)
			}
		}
		if metadata != nil {
			if err := attribution.ValidateSegment(candidate, metadata.StreamID, metadata.Policy, metadata.Provenance, metadata.Boundary); err != nil {
				return nil, fmt.Errorf("ingest: validate attribution: %w", err)
			}
		}
		candidates = append(candidates, acceptedCandidate{event: candidate, originalIndex: i})
	}
	// Run all persistence-bearing preparations before the first segment write.
	for i := range candidates {
		if metadata == nil {
			break
		}
		prepared, err := w.cfg.Attribution.PrepareSegment(candidates[i].event, metadata.StreamID, metadata.Policy, metadata.Provenance, metadata.Boundary, metadata.Source)
		if err != nil {
			return nil, w.wrapSegmentPersistenceError("preparing batch attribution", fmt.Errorf("ingest: prepare attribution: %w", err))
		}
		candidates[i].prepared = prepared
	}
	var jobs []*asyncFlushJob
	for i := range candidates {
		accepted := &candidates[i]
		full, err := w.active.Append(accepted.event)
		if err != nil {
			return jobs, w.wrapSegmentPersistenceError("appending batch event", fmt.Errorf("ingest: append: %w", err))
		}
		event := &events[accepted.originalIndex]
		event.Seq, event.IndexedAt = accepted.event.Seq, accepted.event.IndexedAt
		if metadata != nil {
			w.pendingAttributions = append(w.pendingAttributions, accepted.prepared)
		}
		w.nextSeq++
		w.cfg.Metrics.incEventsAppended()
		w.cfg.Metrics.setNextSeq(w.nextSeq)
		w.readLog.append(&accepted.event)
		if w.cfg.OnAppend != nil {
			if err := w.cfg.OnAppend(event); err != nil {
				return jobs, fmt.Errorf("ingest: on_append: %w", err)
			}
		}
		if full {
			if w.async != nil {
				job, err := w.prepareAsyncFlushLocked()
				if err != nil {
					return jobs, err
				}
				jobs = append(jobs, job)
			} else if err := w.flushAndRotateLocked(ctx); err != nil {
				return jobs, err
			}
		}
	}
	return jobs, nil
}
func (w *Writer) appendLocked(ctx context.Context, ev *segment.Event) (*asyncFlushJob, error) {
	if w.cfg.Attribution != nil {
		return nil, fmt.Errorf("ingest: attributed writer requires AppendAttributed metadata")
	}
	return w.appendAttributedLocked(ctx, ev, nil)
}

func (w *Writer) appendAttributedLocked(ctx context.Context, ev *segment.Event, metadata *AttributionMetadata) (*asyncFlushJob, error) {
	if metadata != nil && w.cfg.Attribution == nil {
		return nil, fmt.Errorf("ingest: attribution manager is required")
	}
	if w.cfg.Attribution != nil && metadata == nil {
		return nil, fmt.Errorf("ingest: attributed writer requires AppendAttributed metadata")
	}
	if metadata != nil && (metadata.StreamID == "" || metadata.Policy.Revision == 0) {
		return nil, fmt.Errorf("ingest: invalid attribution metadata: %w", attribution.ErrInvalid)
	}
	if w.closed {
		w.cfg.Metrics.incAppendErrors()
		return nil, ErrClosed
	}

	// hypercerts: Selection precedes segment bytes, timestamps, and read-log publication.
	if !w.cfg.CollectionPolicy.Allows(ev) {
		ev.Seq = 0
		return nil, nil
	}
	// hypercerts: A verified scoped snapshot can be ahead of delayed live input.
	if superseded, err := w.snapshotSupersedes(ev, metadata); err != nil {
		return nil, err
	} else if superseded {
		ev.Seq = 0
		return nil, nil
	}

	if metadata != nil && metadata.Source != nil && metadata.Source.Seq != ev.Seq {
		return nil, fmt.Errorf("ingest: imported source sequence mismatch: %w", attribution.ErrInvalid)
	}
	candidate := *ev
	candidate.Seq = w.nextSeq
	if w.cfg.TimestampStamper != nil {
		if err := w.cfg.TimestampStamper.Stamp(ctx, &candidate); err != nil {
			w.cfg.Metrics.incAppendErrors()
			return nil, fmt.Errorf("ingest: timestamp stamp: %w", err)
		}
	}
	var prepared attribution.Prepared
	if metadata != nil {
		var prepareErr error
		prepared, prepareErr = w.cfg.Attribution.PrepareSegment(candidate, metadata.StreamID, metadata.Policy, metadata.Provenance, metadata.Boundary, metadata.Source)
		if prepareErr != nil {
			return nil, fmt.Errorf("ingest: prepare attribution: %w", prepareErr)
		}
	}
	full, err := w.active.Append(candidate)
	if err != nil {
		w.cfg.Metrics.incAppendErrors()
		return nil, fmt.Errorf("ingest: append: %w", err)
	}
	ev.Seq = candidate.Seq
	ev.IndexedAt = candidate.IndexedAt
	if metadata != nil {
		w.pendingAttributions = append(w.pendingAttributions, prepared)
	}
	w.nextSeq++
	w.cfg.Metrics.incEventsAppended()
	w.cfg.Metrics.setNextSeq(w.nextSeq)
	w.readLog.append(&candidate)

	if w.cfg.OnAppend != nil {
		if err := w.cfg.OnAppend(ev); err != nil {
			return nil, fmt.Errorf("ingest: on_append: %w", err)
		}
	}

	if full {
		if w.async != nil {
			job, err := w.prepareAsyncFlushLocked()
			if err != nil {
				return nil, err
			}
			return job, nil
		}
		if err := w.flushAndRotateLocked(ctx); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Flush forces any pending block to fsync. Goroutine-safe. Used by
// the merge phase to order destination-segment durability before
// its cursor commit (docs/README.md §3.1.1 / merge spec §5.2). Steady-
// state callers do not need to call this — the per-block-fill path
// inside Append handles ordinary flush + rotation.
//
// Flush deliberately does not rotate: size-based rotation is the
// concern of Append/AppendBatch (sync: flushAndRotateLocked; async:
// rotateIfFull). A segment pushed over MaxSegmentBytes by an explicit
// Flush alone stays active until the next append, which then rotates
// it — bounded overshoot, since appends dominate in every caller.
//
// A no-op when nothing is buffered.
func (w *Writer) Flush(ctx context.Context) error {
	if w.async != nil {
		w.drainMu.Lock()
		job, err := w.prepareAsyncFlushForFlush()
		if err == nil {
			w.submitAsyncFlushes([]*asyncFlushJob{job})
		}
		w.drainMu.Unlock()
		if err != nil {
			return err
		}
		return w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job})
	}

	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	return w.flushSync(ctx)
}

func (w *Writer) prepareAsyncFlushForFlush() (*asyncFlushJob, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrClosed
	}
	if w.active == nil {
		return nil, nil
	}
	return w.prepareAsyncFlushLocked()
}

func (w *Writer) flushSync(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.active == nil {
		return nil
	}
	return w.flushAndRotateLocked(ctx)
}

func (w *Writer) drainAsync(ctx context.Context) error {
	job, err := w.prepareAsyncFlushForFlush()
	if err != nil {
		return err
	}
	w.submitAsyncFlushes([]*asyncFlushJob{job})
	if err := w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job}); err != nil {
		return err
	}
	w.asyncJobs.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true, w.sampleDurableBatchPrepareValueLocked())
}

func (w *Writer) drainSync(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.active == nil {
		return nil
	}
	if w.active.Pending() > 0 {
		if err := w.flushAndRotateLocked(ctx); err != nil {
			return err
		}
	} else if len(w.pendingAttributions) > 0 {
		pending := w.pendingAttributions
		if err := w.commitDurableBatchWithAttributionsLocked(ctx, w.nextSeq, true, w.sampleDurableBatchPrepareValueLocked(), pending); err != nil {
			return err
		}
		w.pruneCommittedAttributions(pending)
		return nil
	}
	return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true, w.sampleDurableBatchPrepareValueLocked())
}

// DrainDurability forces pending event-backed metadata to its block durability
// point and commits metadata-only durable hooks even when no events are pending.
func (w *Writer) DrainDurability(ctx context.Context) error {
	w.drainMu.Lock()
	defer w.drainMu.Unlock()

	if w.async != nil {
		return w.drainAsync(ctx)
	}
	return w.drainSync(ctx)
}

// SetDurableBatchHook installs or replaces the metadata hook invoked when the
// writer commits durable block metadata. Backfill Run owns this hook for the
// backfill writer and intentionally replaces any prior hook. Callers should
// wire this before starting producers.
func (w *Writer) SetDurableBatchHook(h DurableBatchHook) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cfg.OnDurableBatch = h
}

func (w *Writer) commitTerminalDurableBatchLocked() error {
	return w.commitDurableBatchLocked(context.Background(), w.nextSeq, true, w.sampleDurableBatchPrepareValueLocked())
}

// flushAndRotateLocked is the post-Append durability commit. The
// caller MUST hold w.mu.
//
// Order matters per docs/README.md §3.1.1:
//  1. segment.Writer.Flush fsyncs the block.
//  2. We pebble.Sync the new seq/next.
//  3. If activeBytes >= MaxSegmentBytes, seal the current segment
//     and open the next index. segment.Writer.Seal handles its own
//     torn-tail recovery on partial-seal failure.
//
// Step 1 first ensures a crash between (1) and (2) leaves seq/next
// lagging at most one block; Open's ScanMaxSeq reconciles it.
//
// Spans are emitted at the block-flush and rotation granularity
// (~one per 4096 events / one per ~256MB respectively); per-Append
// spans would balloon to ~1B/day at full network scale.
func (w *Writer) flushAndRotateLocked(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		if err := w.flushBlockLocked(ctx); err != nil {
			return err
		}

		path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(w.activeIdx))
		info, statErr := statFS(w.cfg.FS, path)
		if statErr != nil {
			return fmt.Errorf("ingest: stat active segment: %w", statErr)
		}
		w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)
		w.cfg.Metrics.setActiveSegBytes(w.activeBytes)

		if w.activeBytes < w.cfg.MaxSegmentBytes {
			return nil
		}

		return w.rotateLocked(ctx)
	})
}

// flushBlockLocked is the shared flush-side of the durability commit:
// fsync the pending block, then pebble.Sync seq/next and durable-batch
// metadata.
// The caller MUST hold w.mu.
func (w *Writer) pruneCommittedAttributions(committed []attribution.Prepared) {
	if len(committed) == 0 {
		return
	}
	committedSeqs := make(map[uint64]struct{}, len(committed))
	for _, prepared := range committed {
		committedSeqs[prepared.Event.Seq] = struct{}{}
	}
	remaining := w.pendingAttributions[:0]
	for _, prepared := range w.pendingAttributions {
		if _, ok := committedSeqs[prepared.Event.Seq]; !ok {
			remaining = append(remaining, prepared)
		}
	}
	w.pendingAttributions = remaining
}

func (w *Writer) flushBlockLocked(ctx context.Context) error {
	prepareValue := w.sampleDurableBatchPrepareValueLocked()
	if err := w.active.Flush(); err != nil {
		return w.wrapSegmentPersistenceError("flushing active segment block", fmt.Errorf("ingest: flush block: %w", err))
	}
	w.cfg.Metrics.incBlocksFlushed()

	var attributions []attribution.Prepared
	if w.cfg.Attribution != nil && len(w.pendingAttributions) > 0 {
		// Commit only metadata whose rows have reached durability, independent of
		// unrelated legacy rows in the active segment block.
		for _, pending := range w.pendingAttributions {
			if pending.Event.Seq >= w.activeDurableTailSeqLocked() && pending.Event.Seq < w.nextSeq {
				attributions = append(attributions, pending)
			}
		}
	}
	if err := w.commitDurableBatchWithAttributionsLocked(ctx, w.nextSeq, false, prepareValue, attributions); err != nil {
		return err
	}
	if len(attributions) > 0 {
		w.pruneCommittedAttributions(attributions)
	}
	return nil
}

// ForceRotate flushes any pending block and rotates the active segment
// regardless of its size, so the just-sealed file becomes visible to
// the delete compactor's sealed-segment sweep. A no-op when the active
// segment holds zero events — rotating an empty file would churn out
// empty sealed segments (e.g. every compaction interval while the
// upstream relay is down) for no compliance benefit. Goroutine-safe;
// concurrent Appends serialize against the rotation on w.mu.
func (w *Writer) ForceRotate(ctx context.Context) error {
	w.drainMu.Lock()
	defer w.drainMu.Unlock()

	if w.async != nil {
		w.asyncJobs.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.active.Pending() == 0 && len(w.active.Blocks()) == 0 {
		return nil
	}
	if err := w.flushBlockLocked(ctx); err != nil {
		return err
	}
	return w.rotateLocked(ctx)
}

// rotateLocked seals the active segment and opens the next one. The
// caller MUST hold w.mu. Split out from flushAndRotateLocked so its
// span (rotateLocked) is a clear child rather than buried inside the
// parent.
func (w *Writer) rotateLocked(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		sealedIdx := w.activeIdx
		sealedPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(sealedIdx))
		if _, err := w.active.Seal(); err != nil {
			return w.wrapSegmentPersistenceError("sealing active segment", fmt.Errorf("ingest: seal segment %d: %w", sealedIdx, err))
		}

		if err := w.onAfterSealLocked(sealedIdx, sealedPath); err != nil {
			return err
		}

		w.activeIdx++
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("active_idx", int64(w.activeIdx)))

		nextPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(w.activeIdx))
		next, err := segment.New(segment.Config{
			Path:              nextPath,
			FS:                w.cfg.FS,
			MaxEventsPerBlock: w.cfg.MaxEventsPerBlock,
			Metrics:           w.cfg.SegmentMetrics,
			IOFaultInjector:   w.cfg.SegmentIOFaultInjector,
		})
		if err != nil {
			return w.wrapSegmentPersistenceError("opening new active segment", fmt.Errorf("ingest: open new active segment %s: %w", nextPath, err))
		}
		w.active = next
		w.activeBytes = 0
		w.cfg.Metrics.setActiveSegBytes(0)
		w.cfg.Metrics.incSegmentsRotated()

		w.cfg.Logger.InfoContext(ctx, "rotated segment", "new_index", w.activeIdx)
		return nil
	})
}

// onAfterSealLocked publishes a newly sealed segment. The caller MUST
// hold w.mu; hooks must not call back into Writer.
func (w *Writer) onAfterSealLocked(idx uint64, path string) error {
	if w.cfg.OnAfterSeal == nil {
		return nil
	}
	if err := w.cfg.OnAfterSeal(idx, path); err != nil {
		return fmt.Errorf("ingest: on_after_seal: %w", err)
	}
	return nil
}

// NextSeq returns the next seq value the writer will allocate.
// Exposed for tests and observability; production callers should
// not rely on this value being stable across goroutines.
func (w *Writer) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

// ReadLog returns the writer-owned readable log.
func (w *Writer) ReadLog() *ReadableLog {
	return w.readLog
}

// ActiveIndex returns the numeric index of the current active segment.
func (w *Writer) ActiveIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeIdx
}

// ActiveFlushedRange is a coherent snapshot of one active segment generation's
// frame-aligned flushed byte range. It excludes the pending in-memory block.
type ActiveFlushedRange struct {
	Index       uint64
	StartOffset uint64
	EndOffset   uint64
}

// ActiveFlushedRange returns the active flushed range containing startSeq and
// every later flushed block in the same active generation. Segment index and
// offsets are captured together under the writer lock; disk I/O must happen
// after this method returns.
func (w *Writer) ActiveFlushedRange(startSeq uint64) (ActiveFlushedRange, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.active == nil {
		return ActiveFlushedRange{}, false
	}
	start, end, ok := w.active.FlushedRangeFromSeq(startSeq)
	if !ok {
		return ActiveFlushedRange{}, false
	}
	return ActiveFlushedRange{Index: w.activeIdx, StartOffset: start, EndOffset: end}, true
}

// SegmentsDir returns the configured segments directory. The cfg is
// read-only after Open, so no mutex is needed.
func (w *Writer) SegmentsDir() string {
	return w.cfg.SegmentsDir
}

// SegmentFile is one entry in a SegmentFiles result.
type SegmentFile struct {
	Idx  uint64
	Path string
}

// SegmentFiles returns every seg_*.jss file under dir, sorted by
// numeric index ascending. Non-segment files and subdirectories are
// silently skipped — the directory may legitimately contain other
// operator-placed files.
//
// Used by every consumer that needs the full segment manifest in
// creation order: the merge phase draining live_segments/, the
// delete/update compactor, and inspect tooling.
func SegmentFiles(dir string) ([]SegmentFile, error) {
	return SegmentFilesFS(nil, dir)
}

// SegmentFilesFS is SegmentFiles against fs. Nil fs uses the host OS filesystem.
func SegmentFilesFS(fs vfs.FS, dir string) ([]SegmentFile, error) {
	sfs := ingestFS(fs)
	entries, err := sfs.List(dir)
	if err != nil {
		return nil, fmt.Errorf("ingest: readdir %s: %w", dir, err)
	}
	out := make([]SegmentFile, 0, len(entries))
	for _, name := range entries {
		idx, ok := ParseSegmentIndex(name)
		if !ok {
			continue
		}
		path := sfs.PathJoin(dir, name)
		info, statErr := sfs.Stat(path)
		if statErr != nil {
			return nil, fmt.Errorf("ingest: stat %s: %w", path, statErr)
		}
		if info.IsDir() {
			continue
		}
		out = append(out, SegmentFile{Idx: idx, Path: path})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Idx < out[j].Idx })
	return out, nil
}

// recoverSealedTailMaxSeq finds the highest seq envelope among the sealed
// segments STRICTLY BELOW highestIdx, scanning newest-first and returning the
// first non-empty one (segments are seq-monotonic in creation order, so the
// highest-index non-empty segment holds the global max). It is the fallback for
// Open's recovery floor when the highest segment carries no envelope of its own
// (a truly empty active segment, or an empty/compacted-empty sealed segment):
// without it a missing/illegal-0 seq/next would reuse seqs a lower segment owns.
//
// Each candidate is opened with the checksum-verifying segment.Open (not
// ReadSealedHeader): like the highest-segment branch, this is a corruption
// safety net and must not floor nextSeq off an unverified header. A still-active
// (unsealed) lower segment is impossible here — only the highest index is ever
// the active file — but ErrActiveSegment is treated as "no usable envelope" and
// skipped rather than failing the open, so a partially-rotated dir still
// recovers. Any other open error is returned: a corrupt lower segment must
// crash Open, not silently lower the floor.
func recoverSealedTailMaxSeq(fs vfs.FS, dir string, highestIdx uint64) (maxSeq uint64, found bool, err error) {
	files, err := SegmentFilesFS(fs, dir)
	if err != nil {
		return 0, false, err
	}
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].Idx >= highestIdx {
			continue
		}
		r, openErr := segment.Open(segment.ReaderConfig{Path: files[i].Path, FS: fs})
		if openErr != nil {
			if errors.Is(openErr, segment.ErrActiveSegment) {
				continue // an unsealed file carries no committed envelope; skip it
			}
			return 0, false, fmt.Errorf("ingest: open sealed tail %s: %w", files[i].Path, openErr)
		}
		hdr := r.Header()
		if closeErr := r.Close(); closeErr != nil {
			return 0, false, fmt.Errorf("ingest: close sealed tail %s: %w", files[i].Path, closeErr)
		}
		if hdr.MaxSeq > 0 {
			return hdr.MaxSeq, true, nil
		}
	}
	return 0, false, nil
}

// scanSegmentsDir lists cfg.SegmentsDir and returns the highest seg_*
// index seen and whether any matching files exist. Thin wrapper over
// SegmentFiles preserved for the writer-open path.
func scanSegmentsDir(fs vfs.FS, dir string) (idx uint64, has bool, err error) {
	files, err := SegmentFilesFS(fs, dir)
	if err != nil {
		return 0, false, err
	}
	if len(files) == 0 {
		return 0, false, nil
	}
	last := files[len(files)-1]
	return last.Idx, true, nil
}

// loadNextSeq reads the persisted seq/next counter for key, returning 0 for a
// fresh data dir (absent key). The caller floors nextSeq to 1 either way, so the
// first-ever event is seq 1 and seq 0 stays a pure "nothing yet" sentinel
// (design §R8); the absent-vs-zero distinction is therefore irrelevant here.
func loadNextSeq(st *store.Store, key string) (val uint64, err error) {
	v, _, err := st.GetUint64LE(key)
	return v, err
}

// saveNextSeq durably persists the seq counter for key via pebble.Sync.
func saveNextSeq(st *store.Store, key string, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if err := st.Set([]byte(key), buf[:], store.SyncWrites); err != nil {
		return fmt.Errorf("ingest: save %s: %w", key, err)
	}
	return nil
}

func stageNextSeq(b *pebble.Batch, key string, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if err := b.Set([]byte(key), buf[:], nil); err != nil {
		return fmt.Errorf("ingest: stage %s: %w", key, err)
	}
	return nil
}

func (w *Writer) activeDurableTailSeqLocked() uint64 {
	if w.readLog != nil {
		return w.readLog.DurableSeq()
	}
	return w.durableNextSeq
}

func (w *Writer) sampleDurableBatchPrepareValueLocked() any {
	if w.cfg.DurableBatchPrepareValue == nil {
		return nil
	}
	return w.cfg.DurableBatchPrepareValue()
}

func (w *Writer) commitDurableBatchLocked(ctx context.Context, nextSeq uint64, force bool, prepareValue any) error {
	return w.commitDurableBatchWithAttributionsLocked(ctx, nextSeq, force, prepareValue, w.pendingAttributions)
}

func (w *Writer) commitDurableBatchWithAttributionsLocked(ctx context.Context, nextSeq uint64, force bool, prepareValue any, pending []attribution.Prepared) error {
	// The block this commit describes is already fsynced by the time we get
	// here (flushBlockLocked / commitAsyncFlush / the drain paths all flush
	// first). The durable metadata commit must therefore run to completion
	// regardless of caller cancellation: aborting now would leave seq/next and
	// any OnDurableBatch metadata (e.g. queued repo completions) behind durable
	// segment data, and would surface a benign run-cancel (backfill MaxRepos,
	// graceful shutdown) as a spurious fatal "on_durable_batch: context
	// canceled". WithoutCancel keeps trace/span lineage while detaching
	// cancellation, matching the async path's context.Background() intent.
	ctx = context.WithoutCancel(ctx)

	var afterCommit func()
	var afterDone func(error)
	var commitErr error
	commit := func(b *pebble.Batch) error {
		if err := stageNextSeq(b, w.cfg.SeqKey, nextSeq); err != nil {
			return err
		}
		if w.cfg.OnDurableBatch != nil {
			cb, done, err := w.cfg.OnDurableBatch(ctx, b, nextSeq, force, prepareValue)
			afterDone = done
			if err != nil {
				return fmt.Errorf("ingest: on_durable_batch: %w", err)
			}
			afterCommit = cb
		}
		return nil
	}
	defer func() {
		if afterDone != nil {
			afterDone(commitErr)
		}
	}()
	previousDurableSeq := w.activeDurableTailSeqLocked()
	filtered := make([]attribution.Prepared, 0, len(pending))
	for _, prepared := range pending {
		if prepared.Event.Seq >= previousDurableSeq && prepared.Event.Seq < nextSeq {
			filtered = append(filtered, prepared)
		}
	}
	if w.cfg.Attribution != nil && len(filtered) > 0 {
		commitErr = w.cfg.Attribution.CommitBatch(filtered, commit)
	} else {
		b := w.cfg.Store.NewBatch()
		defer func() { _ = b.Close() }()
		if err := commit(b); err != nil {
			commitErr = err
			return err
		}
		commitErr = w.cfg.Store.Commit(b, store.SyncWrites)
	}
	if commitErr != nil {
		return w.wrapSegmentPersistenceError("committing durable metadata batch", fmt.Errorf("ingest: commit durable batch: %w", commitErr))
	}
	if len(filtered) > 0 {
		w.pruneCommittedAttributions(filtered)
	}
	w.durableNextSeq = nextSeq
	w.readLog.advanceDurable(nextSeq)
	if afterCommit != nil {
		afterCommit()
	}
	return nil
}
