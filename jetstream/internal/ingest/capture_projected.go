package ingest

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
)

// CaptureProjectedReceipt is the durable result of recording an upstream
// original and, when appropriate, applying it to the current-state projection.
type CaptureProjectedReceipt struct {
	JournalSeq    uint64
	SegmentSeq    uint64
	Disposition   history.Disposition
	NewProjection bool
}

// CaptureAndProject commits the canonical input and admission-time decision
// before applying its projection. A replay uses the complete saved input.
func (w *Writer) CaptureAndProject(ctx context.Context, source history.Source, event segment.Event, original []byte, policy history.Policy, provenance string, metadata AttributionMetadata) (CaptureProjectedReceipt, error) {
	if w.cfg.History == nil || w.cfg.Attribution == nil {
		return CaptureProjectedReceipt{}, errors.New("ingest: capture-and-project requires history and attribution managers")
	}
	if err := w.cfg.Attribution.ValidateBinding(w.cfg.Store, w.cfg.AttributionDomain); err != nil {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: projected attribution binding: %w", err)
	}
	if err := w.cfg.History.ValidateBinding(w.cfg.Store); err != nil {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: history binding: %w", err)
	}
	if source.Cursor == 0 || len(original) == 0 || policy.Revision == 0 || provenance == "" {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: invalid capture metadata: %w", history.ErrInvalid)
	}
	if err := segment.ValidateEvent(event); err != nil {
		return CaptureProjectedReceipt{}, err
	}
	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return CaptureProjectedReceipt{}, ErrClosed
	}
	if w.async != nil {
		return CaptureProjectedReceipt{}, errors.New("ingest: capture-and-project requires synchronous writer")
	}

	if metadata.StreamID == "" {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: attribution stream is required")
	}
	receipt, found, err := w.cfg.History.LookupSource(source)
	if err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if found {
		if history.OriginalHash(original) != receipt.Source.ContentHash {
			return CaptureProjectedReceipt{}, history.ErrConflict
		}
		if !sameProjectedSource(receipt.InputEvent, event) {
			return CaptureProjectedReceipt{}, history.ErrConflict
		}
		if receipt.Owner != w.cfg.HistoryProjectionTarget {
			return CaptureProjectedReceipt{JournalSeq: receipt.Sequence, Disposition: receipt.Disposition}, nil
		}
		if err := w.drainHistoryLocked(ctx); err != nil {
			return CaptureProjectedReceipt{}, err
		}
		return w.projectCapturedReceiptLocked(ctx, receipt, receipt.InputEvent, metadata, false)
	}

	if err := w.drainHistoryLocked(ctx); err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if metadata.Policy.Revision != policy.Revision || !slices.Equal(metadata.Policy.Collections, policy.Collections) {
		return CaptureProjectedReceipt{}, errors.New("ingest: projection policy differs from admitted policy")
	}
	if metadata.Provenance == attribution.ProvenanceSnapshot || metadata.Provenance == attribution.ProvenanceReconciliation {
		sequence := receipt.Sequence
		metadata.Boundary.JournalSequence = &sequence
	}
	if metadata.Provenance != attribution.ProvenanceSnapshot && metadata.Provenance != attribution.ProvenanceReconciliation && metadata.Provenance != attribution.ProvenanceLive {
		metadata.Boundary = attribution.Boundary{}
	}
	if metadata.StreamID == "" || metadata.Policy.Revision == 0 || metadata.Provenance == "" || metadata.Provenance != attribution.Provenance(provenance) && !(event.Kind == segment.KindSync && metadata.Provenance == attribution.ProvenanceReconciliation) {
		return CaptureProjectedReceipt{}, errors.New("ingest: invalid projection attribution metadata")
	}
	if event.Kind != segment.KindSync && !w.cfg.CollectionPolicy.Allows(&event) {
		return CaptureProjectedReceipt{}, errors.New("ingest: captured event denied by current materialization policy")
	}
	superseded := false
	if event.Kind != segment.KindSync {
		var err error
		superseded, err = w.snapshotSupersedes(&event, &metadata)
		if err != nil {
			return CaptureProjectedReceipt{}, err
		}
	}
	disposition := history.Apply
	boundary := uint64(0)
	if event.Kind == segment.KindSync {
		disposition = history.Noop
	} else if superseded {
		disposition = history.HistoricalOnly
		if metadata.Boundary.JournalSequence != nil {
			boundary = *metadata.Boundary.JournalSequence
		}
	}
	input := event
	input.Payload = append([]byte(nil), event.Payload...)
	journalEvent := history.Append{Owner: w.cfg.HistoryProjectionTarget, Source: source, Kind: fmt.Sprintf("%d", event.Kind), DID: event.DID, Collection: event.Collection, Revision: event.Rev, Time: event.WitnessedAt, Original: original, InputEvent: input, Policy: policy, Provenance: provenance, Disposition: disposition, Boundary: boundary, Attribution: historyAttribution(metadata)}
	appended, err := w.cfg.History.Append(journalEvent)
	if err != nil {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: capture original: %w", err)
	}
	page, err := w.cfg.History.Read(appended.Sequence-1, appended.Sequence, 1)
	if err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if len(page.Events) != 1 {
		return CaptureProjectedReceipt{}, errors.New("ingest: captured journal receipt missing")
	}
	return w.projectCapturedReceiptLocked(ctx, page.Events[0], page.Events[0].InputEvent, metadata, true)
}

func (w *Writer) projectCapturedReceiptLocked(ctx context.Context, receipt history.Event, event segment.Event, metadata AttributionMetadata, newlyAppended bool) (CaptureProjectedReceipt, error) {
	result := CaptureProjectedReceipt{JournalSeq: receipt.Sequence, Disposition: receipt.Disposition, NewProjection: newlyAppended}
	metadata = attributionFromHistory(receipt.Attribution)
	if metadata.StreamID == "" || metadata.Policy.Revision == 0 || metadata.Provenance == "" {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: missing saved projection attribution: %w", history.ErrInvalid)
	}
	if err := w.appendProjectedLockedWithDrain(ctx, event, receipt.Sequence, metadata, false); err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if receipt.Disposition == history.Apply {
		intent, found, err := w.cfg.History.ProjectionIntentFor(w.cfg.HistoryProjectionTarget, receipt.Sequence)
		if err != nil {
			return CaptureProjectedReceipt{}, err
		}
		if found {
			result.SegmentSeq = intent.SegmentSequence
		}
	}
	return result, nil
}
