package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
)

func sameProjectedSource(planned, supplied segment.Event) bool {
	return planned.Kind == supplied.Kind && planned.DID == supplied.DID && planned.Collection == supplied.Collection && planned.Rkey == supplied.Rkey && planned.Rev == supplied.Rev && string(planned.Payload) == string(supplied.Payload)
}

func (w *Writer) addPendingProjection(sequence uint64) {
	for _, pending := range w.pendingProjectionSequences {
		if pending == sequence {
			return
		}
	}
	w.pendingProjectionSequences = append(w.pendingProjectionSequences, sequence)
}

func (w *Writer) AppendProjected(ctx context.Context, event segment.Event, journalSeq uint64, metadata AttributionMetadata) error {
	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.drainHistoryBeforeLocked(ctx, journalSeq); err != nil {
		return err
	}
	return w.appendProjectedLockedWithDrain(ctx, event, journalSeq, metadata, false)
}

func (w *Writer) appendProjectedLocked(ctx context.Context, event segment.Event, journalSeq uint64, metadata AttributionMetadata) error {
	if err := w.drainHistoryBeforeLocked(ctx, journalSeq); err != nil {
		return err
	}
	return w.appendProjectedLockedWithDrain(ctx, event, journalSeq, metadata, false)
}

func (w *Writer) appendProjectedLockedWithDrain(ctx context.Context, event segment.Event, journalSeq uint64, metadata AttributionMetadata, drain bool) error {
	if w.cfg.History == nil || w.cfg.Attribution == nil {
		return errors.New("ingest: projected append requires history and attribution managers")
	}
	if drain {
		if err := w.drainHistoryLocked(ctx); err != nil {
			return err
		}
	}
	return w.appendProjectedCoreLocked(ctx, event, journalSeq, metadata)
}

func (w *Writer) appendProjectedCoreLocked(ctx context.Context, event segment.Event, journalSeq uint64, metadata AttributionMetadata) error {
	if err := w.cfg.Attribution.ValidateBinding(w.cfg.Store, w.cfg.AttributionDomain); err != nil {
		return fmt.Errorf("ingest: projected attribution binding: %w", err)
	}
	if journalSeq == 0 {
		return fmt.Errorf("ingest: invalid journal sequence: %w", history.ErrInvalid)
	}
	status, err := w.cfg.History.ProjectionStatusFor(w.cfg.HistoryProjectionTarget, journalSeq)
	if err != nil {
		return fmt.Errorf("ingest: read projection status: %w", err)
	}
	receipt, found, err := w.cfg.History.Event(journalSeq)
	if err != nil {
		return fmt.Errorf("ingest: read projection journal event: %w", err)
	}
	if !found {
		return fmt.Errorf("ingest: missing projection journal event %d", journalSeq)
	}
	if !sameProjectedSource(receipt.InputEvent, event) {
		return fmt.Errorf("ingest: projected event differs from admitted input: %w", history.ErrProjectionConflict)
	}
	if history.OriginalHash(receipt.Original) != receipt.Source.ContentHash {
		return fmt.Errorf("ingest: journal receipt content hash mismatch: %w", history.ErrInvalid)
	}
	if status == history.RecoveryComplete {
		return nil
	}
	if w.closed {
		return ErrClosed
	}
	if w.async != nil {
		return errors.New("ingest: projected append requires synchronous writer")
	}
	metadata = attributionFromHistory(receipt.Attribution)
	if metadata.StreamID == "" || metadata.Policy.Revision == 0 || metadata.Provenance == "" {
		return fmt.Errorf("ingest: missing saved projection attribution: %w", history.ErrInvalid)
	}
	pending, err := w.cfg.History.PendingProjectionsFor(w.cfg.HistoryProjectionTarget)
	if err != nil {
		return fmt.Errorf("ingest: read pending projections: %w", err)
	}
	var reserved *history.ProjectionIntent
	for i := range pending {
		if pending[i].Intent.JournalSequence == journalSeq {
			reserved = &pending[i].Intent
			break
		}
	}
	if status == "pending" && reserved == nil {
		return errors.New("ingest: projection reservation is unavailable")
	}
	if status != "pending" && reserved != nil {
		return errors.New("ingest: inconsistent pending projection status")
	}
	eventCopy := event
	if err := segment.ValidateEvent(eventCopy); err != nil {
		return err
	}
	disposition := receipt.Disposition
	segmentSeq := w.nextSeq
	appliedThrough, err := w.cfg.History.AppliedThroughFor(w.cfg.HistoryProjectionTarget)
	if err != nil {
		return fmt.Errorf("ingest: read applied projection prefix: %w", err)
	}
	if journalSeq <= appliedThrough {
		return fmt.Errorf("ingest: journal sequence %d is ahead of applied prefix %d: %w", journalSeq, appliedThrough, history.ErrReservationPending)
	}
	for _, p := range pending {
		if p.Intent.JournalSequence < journalSeq {
			return fmt.Errorf("ingest: earlier journal sequence %d is pending: %w", p.Intent.JournalSequence, history.ErrReservationPending)
		}
	}
	intent := history.ProjectionIntent{Target: w.cfg.HistoryProjectionTarget, JournalSequence: journalSeq, SegmentSequence: segmentSeq, Fingerprint: receipt.Source.ContentHash, Disposition: disposition}
	if disposition == history.Apply {
		if reserved != nil {
			if reserved.PlannedEvent == nil || !sameProjectedSource(*reserved.PlannedEvent, eventCopy) || reserved.PlannedEvent.Seq != reserved.SegmentSequence || reserved.Fingerprint != attribution.FingerprintSegment(*reserved.PlannedEvent) {
				return fmt.Errorf("ingest: projected event differs from reserved source: %w", history.ErrProjectionConflict)
			}
			eventCopy = *reserved.PlannedEvent
			intent = *reserved
			if eventCopy.Seq > w.nextSeq {
				return fmt.Errorf("ingest: reserved segment sequence %d is ahead of allocator %d: %w", eventCopy.Seq, w.nextSeq, history.ErrProjectionConflict)
			}
			if eventCopy.Seq < w.nextSeq {
				if eventCopy.Seq >= w.readLog.DurableSeq() {
					return nil
				}
				row, exists, err := lookupProjectionSegment(w.cfg, eventCopy.Seq)
				if err != nil {
					return err
				}
				if !exists || row.Fingerprint != attribution.FingerprintSegment(eventCopy) {
					return fmt.Errorf("ingest: reserved segment row is missing or differs: %w", history.ErrProjectionConflict)
				}
				w.addPendingProjection(journalSeq)
				prepareValue := w.sampleDurableBatchPrepareValueLocked()
				if prepareValue == nil {
					prepareValue = int64(0)
				}
				return w.commitDurableBatchLocked(ctx, w.nextSeq, true, prepareValue)
			}
		} else {
			eventCopy.Seq = segmentSeq
			if w.cfg.TimestampStamper != nil {
				if err := w.cfg.TimestampStamper.Stamp(ctx, &eventCopy); err != nil {
					return err
				}
			}
			intent.Fingerprint = attribution.FingerprintSegment(eventCopy)
			intent.PlannedEvent = &eventCopy
		}
	} else {
		intent.SegmentSequence = 0
	}
	if err := w.cfg.History.PrepareProjection(intent); err != nil {
		return fmt.Errorf("ingest: prepare durable projection intent: %w", err)
	}
	if disposition != history.Apply {
		if w.active.Pending() > 0 {
			if err := w.flushAndRotateLocked(ctx); err != nil {
				return fmt.Errorf("ingest: flush pending segment before historical projection: %w", err)
			}
		}
		w.addPendingProjection(journalSeq)
		prepareValue := w.sampleDurableBatchPrepareValueLocked()
		if prepareValue == nil {
			prepareValue = int64(0)
		}
		return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true, prepareValue)
	}
	prepared, err := w.cfg.Attribution.PrepareSegment(eventCopy, metadata.StreamID, metadata.Policy, metadata.Provenance, metadata.Boundary, metadata.Source)
	if err != nil {
		return fmt.Errorf("ingest: prepare projection attribution: %w", err)
	}
	full, err := w.active.Append(eventCopy)
	if err != nil {
		return fmt.Errorf("ingest: append projected event: %w", err)
	}
	event.Seq, event.IndexedAt = eventCopy.Seq, eventCopy.IndexedAt
	w.pendingAttributions = append(w.pendingAttributions, prepared)
	w.addPendingProjection(journalSeq)
	w.nextSeq++
	w.cfg.Metrics.incEventsAppended()
	w.cfg.Metrics.setNextSeq(w.nextSeq)
	w.readLog.append(&eventCopy)
	if w.cfg.OnAppend != nil {
		if err := w.cfg.OnAppend(&event); err != nil {
			return fmt.Errorf("ingest: on_append: %w", err)
		}
	}
	if full {
		return w.flushAndRotateLocked(ctx)
	}
	return nil
}
