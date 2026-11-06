package ingest

import (
	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
)

func historyAttribution(metadata AttributionMetadata) history.Attribution {
	return history.Attribution{
		StreamID:   metadata.StreamID,
		Policy:     history.EffectivePolicy{Revision: metadata.Policy.Revision, Collections: append([]string(nil), metadata.Policy.Collections...), EffectiveAfterSeq: cloneUint64(metadata.Policy.EffectiveAfterSeq)},
		Provenance: string(metadata.Provenance),
		Boundary:   historyBoundary(metadata.Boundary),
		Source:     historySource(metadata.Source),
	}
}

func historyBoundary(boundary attribution.Boundary) history.AttributionBoundary {
	return history.AttributionBoundary{Seq: cloneUint64(boundary.Seq), SnapshotID: cloneString(boundary.SnapshotID), JournalSequence: cloneUint64(boundary.JournalSequence)}
}

func historySource(source *attribution.ImportedSource) *history.ImportedSource {
	if source == nil {
		return nil
	}
	var policy *history.EffectivePolicy
	if source.Policy != nil {
		policy = &history.EffectivePolicy{Revision: source.Policy.Revision, Collections: append([]string(nil), source.Policy.Collections...), EffectiveAfterSeq: cloneUint64(source.Policy.EffectiveAfterSeq)}
	}
	return &history.ImportedSource{Domain: source.Domain, StreamID: source.StreamID, Seq: source.Seq, PolicyRev: cloneUint64(source.PolicyRev), Provenance: string(source.Provenance), Boundary: historyBoundary(source.Boundary), Policy: policy, WitnessedAt: source.WitnessedAt, IndexedAt: source.IndexedAt, SegmentFingerprint: source.SegmentFingerprint}
}

func attributionFromHistory(saved history.Attribution) AttributionMetadata {
	metadata := AttributionMetadata{StreamID: saved.StreamID, Policy: attribution.Policy{Revision: saved.Policy.Revision, Collections: append([]string(nil), saved.Policy.Collections...), EffectiveAfterSeq: cloneUint64(saved.Policy.EffectiveAfterSeq)}, Provenance: attribution.Provenance(saved.Provenance), Boundary: attribution.Boundary{Seq: cloneUint64(saved.Boundary.Seq), SnapshotID: cloneString(saved.Boundary.SnapshotID), JournalSequence: cloneUint64(saved.Boundary.JournalSequence)}}
	if saved.Source != nil {
		source := saved.Source
		var policy *attribution.Policy
		if source.Policy != nil {
			policy = &attribution.Policy{Revision: source.Policy.Revision, Collections: append([]string(nil), source.Policy.Collections...), EffectiveAfterSeq: cloneUint64(source.Policy.EffectiveAfterSeq)}
		}
		metadata.Source = &attribution.ImportedSource{Domain: source.Domain, StreamID: source.StreamID, Seq: source.Seq, PolicyRev: cloneUint64(source.PolicyRev), Provenance: attribution.Provenance(source.Provenance), Boundary: attribution.Boundary{Seq: cloneUint64(source.Boundary.Seq), SnapshotID: cloneString(source.Boundary.SnapshotID), JournalSequence: cloneUint64(source.Boundary.JournalSequence)}, Policy: policy, WitnessedAt: source.WitnessedAt, IndexedAt: source.IndexedAt, SegmentFingerprint: source.SegmentFingerprint}
	}
	return metadata
}

func cloneUint64(v *uint64) *uint64 {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}
func cloneString(v *string) *string {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}
