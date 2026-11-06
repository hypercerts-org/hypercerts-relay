package ingest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestOpenRecoversImportPreparedWithoutBytes(t *testing.T) {
	w := newTestWriter(t, Config{})
	journal, err := history.Open(w.cfg.Store)
	require.NoError(t, err)
	manager, err := attribution.Open(w.cfg.Store, "destination-stream", "import-recovery-no-bytes")
	require.NoError(t, err)
	w.cfg.History, w.cfg.Attribution, w.cfg.AttributionDomain = journal, manager, "import-recovery-no-bytes"
	policy := attribution.Policy{Revision: 1, Collections: []string{"app.example.record"}}
	require.NoError(t, manager.EnsureInitialPolicyHistory(policy, nil))
	sourceRevision := uint64(1)
	sourcePolicy := attribution.Policy{Revision: 1, Collections: []string{"app.example.record"}}
	sourceEvent := segment.Event{Seq: 17, Kind: segment.KindCreate, Collection: "app.example.record", DID: "did:plc:imported", Rkey: "record", Payload: []byte(`{"record":true}`), WitnessedAt: 10}
	source := attribution.Event{StreamID: "source-stream", Seq: 17, PolicyRev: &sourceRevision, Provenance: attribution.ProvenanceLive, Payload: []byte(`{"source":true}`)}
	planned := sourceEvent
	event := planned
	event.Seq = 1
	planned = event
	destination := AttributionMetadata{StreamID: "destination-stream", Policy: policy, Provenance: attribution.ProvenanceReconciliation, Boundary: attribution.Boundary{Seq: new(uint64(1)), SnapshotID: new("import")}, Source: &attribution.ImportedSource{Domain: "source-domain", StreamID: source.StreamID, Seq: source.Seq, PolicyRev: source.PolicyRev, Provenance: source.Provenance, Boundary: source.Boundary, Policy: &sourcePolicy, WitnessedAt: sourceEvent.WitnessedAt, IndexedAt: sourceEvent.IndexedAt, SegmentFingerprint: attribution.FingerprintSegment(sourceEvent)}}
	_, err = manager.PrepareSegment(event, destination.StreamID, destination.Policy, destination.Provenance, destination.Boundary, destination.Source)
	require.NoError(t, err)
	receipt := history.ImportReceipt{Source: history.ImportSource{Domain: "source-domain", StreamID: source.StreamID, Sequence: source.Seq}, Fingerprint: history.OriginalHash([]byte("original")), Target: string(history.TargetMain), SegmentSequence: 1, PlannedEvent: event, SourceAttribution: marshalTestJSON(t, source), DestinationAttribution: marshalTestJSON(t, destination), Policy: marshalTestJSON(t, policy)}
	_, duplicate, err := journal.PrepareImport(receipt)
	require.NoError(t, err)
	require.False(t, duplicate)
	require.NoError(t, w.Close())

	reopened, err := Open(w.cfg)
	require.NoError(t, err)
	defer reopened.Close()
	receipt, found, err := journal.LookupImport(receipt.Source, receipt.Fingerprint)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, history.ImportCompleted, receipt.State)
	require.EqualValues(t, 2, reopened.NextSeq())
	readLog := reopened.ReadLog()
	require.EqualValues(t, 2, readLog.DurableRange())
	require.EqualValues(t, 2, readLog.TipSeq())
	tip, err := journal.Tip()
	require.NoError(t, err)
	require.Zero(t, tip)
}

func TestOpenRecoversImportBytesBeforeMetadata(t *testing.T) {
	w := newTestWriter(t, Config{})
	journal, err := history.Open(w.cfg.Store)
	require.NoError(t, err)
	manager, err := attribution.Open(w.cfg.Store, "destination-stream", "import-recovery-bytes")
	require.NoError(t, err)
	w.cfg.History, w.cfg.Attribution, w.cfg.AttributionDomain = journal, manager, "import-recovery-bytes"
	policy := attribution.Policy{Revision: 1, Collections: []string{"app.example.record"}}
	require.NoError(t, manager.EnsureInitialPolicyHistory(policy, nil))
	planned := segment.Event{Seq: 1, Kind: segment.KindCreate, Collection: "app.example.record", DID: "did:plc:imported", Rkey: "record", Payload: []byte(`{"record":true}`), WitnessedAt: 10, IndexedAt: 20}
	_, err = w.active.Append(planned)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	sourceRevision := uint64(1)
	sourcePolicy := attribution.Policy{Revision: 1, Collections: []string{"app.example.record"}}
	sourceEvent := planned
	sourceEvent.Seq = 17
	source := attribution.Event{StreamID: "source-stream", Seq: 17, PolicyRev: &sourceRevision, Provenance: attribution.ProvenanceLive, Payload: []byte(`{"source":true}`)}
	destination := AttributionMetadata{StreamID: "destination-stream", Policy: policy, Provenance: attribution.ProvenanceReconciliation, Boundary: attribution.Boundary{Seq: new(uint64(1)), SnapshotID: new("import")}, Source: &attribution.ImportedSource{Domain: "source-domain", StreamID: source.StreamID, Seq: source.Seq, PolicyRev: source.PolicyRev, Provenance: source.Provenance, Boundary: source.Boundary, Policy: &sourcePolicy, WitnessedAt: sourceEvent.WitnessedAt, IndexedAt: sourceEvent.IndexedAt, SegmentFingerprint: attribution.FingerprintSegment(sourceEvent)}}
	_, err = manager.PrepareSegment(planned, destination.StreamID, destination.Policy, destination.Provenance, destination.Boundary, destination.Source)
	w.readLog = newReadableLog(2, w.cfg.ReadLogRetentionBytes, w.cfg.Metrics)
	w.durableNextSeq = 2
	require.NoError(t, err)
	receipt := history.ImportReceipt{Source: history.ImportSource{Domain: "source-domain", StreamID: source.StreamID, Sequence: source.Seq}, Fingerprint: history.OriginalHash([]byte("original")), Target: string(history.TargetMain), SegmentSequence: 1, PlannedEvent: planned, SourceAttribution: marshalTestJSON(t, source), DestinationAttribution: marshalTestJSON(t, destination), Policy: marshalTestJSON(t, policy)}
	_, _, err = journal.PrepareImport(receipt)
	require.NoError(t, err)

	reopened, err := Open(w.cfg)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
	again, err := Open(w.cfg)
	require.NoError(t, err)
	defer again.Close()
	receipt, found, err := journal.LookupImport(receipt.Source, receipt.Fingerprint)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, history.ImportCompleted, receipt.State)
	require.EqualValues(t, 2, again.NextSeq())
	require.NoError(t, again.Flush(context.Background()))
	row, found, err := lookupProjectionSegment(w.cfg, 1)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 1, row.SegmentSequence)
}

func marshalTestJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return encoded
}
