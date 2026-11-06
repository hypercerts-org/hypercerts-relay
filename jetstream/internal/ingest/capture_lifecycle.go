package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
)

// CaptureLifecycle classifies lifecycle history while holding the writer's
// admission and state locks, so projection ordering cannot race classification.
func lifecyclePayloadSequence(event segment.Event) int64 {
	if event.Kind == segment.KindAccount {
		var account comatproto.SyncSubscribeRepos_Account
		if account.UnmarshalCBOR(event.Payload) == nil {
			return account.Seq
		}
	} else {
		var identity comatproto.SyncSubscribeRepos_Identity
		if identity.UnmarshalCBOR(event.Payload) == nil {
			return identity.Seq
		}
	}
	return 0
}

func (w *Writer) CaptureLifecycle(ctx context.Context, source history.Source, event segment.Event, original []byte, policy history.Policy, provenance string, metadata AttributionMetadata) (CaptureProjectedReceipt, error) {
	if w.cfg.History == nil || w.cfg.Attribution == nil {
		return CaptureProjectedReceipt{}, errors.New("ingest: lifecycle capture requires history and attribution managers")
	}
	if err := w.cfg.History.ValidateBinding(w.cfg.Store); err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if err := w.cfg.Attribution.ValidateBinding(w.cfg.Store, w.cfg.AttributionDomain); err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if source.Cursor == 0 || len(original) == 0 || policy.Revision == 0 || provenance != string(attribution.ProvenanceLive) {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: invalid lifecycle capture metadata: %w", history.ErrInvalid)
	}
	if event.Kind != segment.KindAccount && event.Kind != segment.KindIdentity {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: lifecycle event required: %w", history.ErrInvalid)
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
		return CaptureProjectedReceipt{}, errors.New("ingest: lifecycle capture requires synchronous writer")
	}
	if source.Domain == "" || metadata.StreamID == "" || metadata.Policy.Revision != policy.Revision {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: invalid lifecycle attribution: %w", attribution.ErrInvalid)
	}
	if err := w.drainHistoryLocked(ctx); err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if prior, found, err := w.cfg.History.LookupSource(source); err != nil {
		return CaptureProjectedReceipt{}, err
	} else if found {
		if history.OriginalHash(original) != prior.Source.ContentHash || prior.InputEvent.Kind != event.Kind || prior.InputEvent.DID != event.DID || prior.InputEvent.Rev != event.Rev || string(prior.InputEvent.Payload) != string(event.Payload) {
			return CaptureProjectedReceipt{}, history.ErrConflict
		}
		return w.projectCapturedReceiptLocked(ctx, prior, prior.InputEvent, metadata, false)
	}
	boundary := uint64(0)
	disposition := history.Apply
	var appliedSeq int64
	var journalSequence uint64
	var err error
	if w.cfg.LifecycleOrdering != nil {
		appliedSeq, err = w.cfg.LifecycleOrdering(ctx, event.DID, event.Kind)
	} else {
		appliedSeq, journalSequence, err = w.cfg.History.LatestAppliedLifecycle(event.DID, event.Kind, w.cfg.HistoryProjectionTarget)
		boundary = journalSequence
	}
	if err != nil {
		return CaptureProjectedReceipt{}, fmt.Errorf("ingest: load durable lifecycle ordering: %w", err)
	}
	if appliedSeq > 0 && lifecyclePayloadSequence(event) <= appliedSeq {
		disposition = history.HistoricalOnly
		if w.cfg.LifecycleOrdering != nil && w.cfg.LifecycleJournalBoundary != nil {
			boundary, err = w.cfg.LifecycleJournalBoundary(ctx, event.DID, event.Kind)
		}
		if err != nil {
			return CaptureProjectedReceipt{}, fmt.Errorf("ingest: load lifecycle journal boundary: %w", err)
		}
	}
	metadata.Boundary = attribution.Boundary{}
	appended, err := w.cfg.History.Append(history.Append{Owner: w.cfg.HistoryProjectionTarget, Source: source, Kind: fmt.Sprintf("%d", event.Kind), DID: event.DID, Revision: event.Rev, Time: event.WitnessedAt, Original: original, InputEvent: event, Policy: policy, Provenance: provenance, Disposition: disposition, Boundary: boundary, Attribution: historyAttribution(metadata)})
	if err != nil {
		return CaptureProjectedReceipt{}, err
	}
	page, err := w.cfg.History.Read(appended.Sequence-1, appended.Sequence, 1)
	if err != nil {
		return CaptureProjectedReceipt{}, err
	}
	if len(page.Events) != 1 {
		return CaptureProjectedReceipt{}, errors.New("ingest: lifecycle history receipt missing")
	}
	return w.projectCapturedReceiptLocked(ctx, page.Events[0], page.Events[0].InputEvent, metadata, true)
}
