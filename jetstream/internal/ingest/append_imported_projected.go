package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
)

func (w *Writer) AppendImportedProjected(ctx context.Context, event *segment.Event, source attribution.Event, domain string, destination AttributionMetadata, sourcePolicy ...attribution.Policy) error {
	if event == nil || w.cfg.History == nil || w.cfg.Attribution == nil || domain == "" || attribution.Validate(source) != nil || len(sourcePolicy) > 1 {
		return fmt.Errorf("ingest: invalid imported projection: %w", attribution.ErrInvalid)
	}
	var importedPolicy *attribution.Policy
	if source.PolicyRev != nil {
		if len(sourcePolicy) != 1 || sourcePolicy[0].Revision != *source.PolicyRev {
			return fmt.Errorf("ingest: source policy required: %w", attribution.ErrInvalid)
		}
		copy := sourcePolicy[0]
		importedPolicy = &copy
	} else if len(sourcePolicy) != 0 {
		return fmt.Errorf("ingest: unexpected source policy: %w", attribution.ErrInvalid)
	}
	if err := attribution.ValidateSegment(*event, destination.StreamID, destination.Policy, destination.Provenance, destination.Boundary); err != nil {
		return fmt.Errorf("ingest: invalid destination attribution: %w", attribution.ErrInvalid)
	}
	original := *event
	identity := original
	if source.Source != nil {
		identity.Seq = source.Seq
		identity.WitnessedAt = source.Source.WitnessedAt
		identity.IndexedAt = source.Source.IndexedAt
		if attribution.FingerprintSegment(identity) != source.Source.SegmentFingerprint {
			return history.ErrImportConflict
		}
	}
	imported := &attribution.ImportedSource{Domain: domain, StreamID: source.StreamID, Seq: source.Seq, PolicyRev: source.PolicyRev, Provenance: source.Provenance, Boundary: source.Boundary, Policy: importedPolicy, WitnessedAt: identity.WitnessedAt, IndexedAt: identity.IndexedAt, SegmentFingerprint: attribution.FingerprintSegment(identity)}
	destination.Source = imported
	fingerprintBytes, err := json.Marshal(struct {
		Event segment.Event `json:"event"`
		Source attribution.Event `json:"source"`
	}{identity, source})
	if err != nil { return err }
	policyBytes, err := json.Marshal(destination.Policy)
	if err != nil { return err }
	sourceBytes, err := json.Marshal(source)
	if err != nil { return err }
	destinationBytes, err := json.Marshal(destination)
	if err != nil { return err }
	receiptSource := history.ImportSource{Domain: domain, StreamID: source.StreamID, Sequence: source.Seq, Operation: 0}
	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.async != nil { return errors.New("ingest: imported history projection requires an open synchronous writer") }
	if err := w.cfg.Attribution.ValidateBinding(w.cfg.Store, w.cfg.AttributionDomain); err != nil { return err }
	fingerprint := history.OriginalHash(fingerprintBytes)
	receipt, duplicate, err := w.cfg.History.LookupImport(receiptSource, fingerprint)
	if err != nil { return err }
	if duplicate {
		if receipt.Target != string(w.cfg.HistoryProjectionTarget) { return history.ErrImportConflict }
		*event = receipt.PlannedEvent
		if receipt.State == history.ImportPending { return w.replayPendingImportedReceiptLocked(ctx, receipt, destination) }
		return nil
	}
	planned := original
	planned.Seq = w.nextSeq
	if w.cfg.TimestampStamper != nil {
		if err := w.cfg.TimestampStamper.Stamp(ctx, &planned); err != nil { return err }
	}
	_, err = w.cfg.Attribution.PrepareSegment(planned, destination.StreamID, destination.Policy, destination.Provenance, destination.Boundary, imported)
	if err != nil { return err }
	receipt, duplicate, err = w.cfg.History.PrepareImport(history.ImportReceipt{Source: receiptSource, Fingerprint: fingerprint, Target: string(w.cfg.HistoryProjectionTarget), SegmentSequence: planned.Seq, PlannedEvent: planned, SourceAttribution: sourceBytes, DestinationAttribution: destinationBytes, Policy: policyBytes})
	if err != nil { return err }
	if duplicate { return history.ErrImportConflict }
	prepared, err := w.cfg.Attribution.PrepareSegment(receipt.PlannedEvent, destination.StreamID, destination.Policy, destination.Provenance, destination.Boundary, imported)
	if err != nil { return err }
	full, err := w.active.Append(receipt.PlannedEvent)
	if err != nil { return err }
	event.Seq, event.IndexedAt = receipt.PlannedEvent.Seq, receipt.PlannedEvent.IndexedAt
	w.pendingAttributions = append(w.pendingAttributions, prepared)
	w.pendingImportCompletions = append(w.pendingImportCompletions, receipt.Source)
	w.nextSeq++
	w.cfg.Metrics.incEventsAppended()
	w.cfg.Metrics.setNextSeq(w.nextSeq)
	w.readLog.append(&receipt.PlannedEvent)
	if full { return w.flushBlockLocked(ctx) }
	return w.flushAndRotateLocked(ctx)
}

func (w *Writer) replayPendingImportedReceiptLocked(ctx context.Context, receipt history.ImportReceipt, destination AttributionMetadata) error {
	if receipt.SegmentSequence != w.nextSeq || receipt.PlannedEvent.Seq != w.nextSeq { return history.ErrImportConflict }
	var saved AttributionMetadata
	if err := json.Unmarshal(receipt.DestinationAttribution, &saved); err != nil { return history.ErrImportConflict }
	var savedSource attribution.Event
	if err := json.Unmarshal(receipt.SourceAttribution, &savedSource); err != nil { return history.ErrImportConflict }
	if saved.StreamID != destination.StreamID { return history.ErrImportConflict }
	var policy attribution.Policy
	if err := json.Unmarshal(receipt.Policy, &policy); err != nil { return history.ErrImportConflict }
	if policy.Revision != saved.Policy.Revision { return history.ErrImportConflict }
	var imported attribution.ImportedSource
	if saved.Source != nil { imported = *saved.Source }
	prepared, err := w.cfg.Attribution.PrepareSegment(receipt.PlannedEvent, saved.StreamID, saved.Policy, saved.Provenance, saved.Boundary, &imported)
	if err != nil { return err }
	full, err := w.active.Append(receipt.PlannedEvent)
	if err != nil { return err }
	w.pendingAttributions = append(w.pendingAttributions, prepared)
	w.pendingImportCompletions = append(w.pendingImportCompletions, receipt.Source)
	w.nextSeq++
	w.cfg.Metrics.incEventsAppended()
	w.cfg.Metrics.setNextSeq(w.nextSeq)
	w.readLog.append(&receipt.PlannedEvent)
	if full { return w.flushBlockLocked(ctx) }
	return w.flushAndRotateLocked(ctx)
}
