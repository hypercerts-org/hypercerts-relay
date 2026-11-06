package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestAppendProjectedRejectsSequenceGapBehindReservation(t *testing.T) {
	db := newTestStore(t)
	h, err := history.Open(db)
	require.NoError(t, err)
	attr, err := attribution.Open(db, h.StreamID(), "history-reservation-test")
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"org.example.record"})
	require.NoError(t, err)
	w, err := Open(Config{SegmentsDir: filepath.Join(t.TempDir(), "segments"), Store: db, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: NewMetrics(prometheus.NewRegistry()), MaxEventsPerBlock: 1, History: h, Attribution: attr, AttributionDomain: "history-reservation-test", CollectionPolicy: policy})
	require.NoError(t, err)
	defer w.Close()
	appendEvent := func(cursor uint64, event segment.Event) history.Receipt {
		rkey := event.Rkey
		body := []byte(rkey)
		canonical := event
		canonical.Payload = append([]byte(nil), event.Payload...)
		r, appendErr := h.Append(history.Append{Source: history.Source{Domain: "relay", Epoch: "epoch", Cursor: cursor, Path: rkey, ContentHash: history.OriginalHash(body)}, Kind: "create", DID: "did:plc:history", Collection: "org.example.record", Revision: "rev-1", Original: body, InputEvent: canonical, Policy: history.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: "live", Disposition: history.Apply, Attribution: history.Attribution{StreamID: h.StreamID(), Policy: history.EffectivePolicy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: "live"}})
		require.NoError(t, appendErr)
		return r
	}
	makeEvent := func(rkey string) segment.Event {
		return segment.Event{Kind: segment.KindCreate, DID: "did:plc:history", Collection: "org.example.record", Rkey: rkey, Rev: "rev-1", Payload: []byte(`{"value":1}`)}
	}
	first, second := appendEvent(1, makeEvent("r1")), appendEvent(2, makeEvent("r2"))
	meta := AttributionMetadata{StreamID: h.StreamID(), Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceLive}
	w.cfg.OnDurableBatch = func(_ context.Context, _ *pebble.Batch, _ uint64, _ bool, _ any) (func(), func(error), error) {
		return nil, nil, errors.New("injected commit failure")
	}
	require.ErrorContains(t, w.AppendProjected(t.Context(), makeEvent("r1"), first.Sequence, meta), "injected commit failure")
	w.cfg.OnDurableBatch = nil
	require.NoError(t, w.AppendProjected(t.Context(), makeEvent("r2"), second.Sequence, meta))
	require.NoError(t, w.AppendProjected(t.Context(), makeEvent("r1"), first.Sequence, meta))
	require.Empty(t, w.pendingProjectionSequences)
	appliedThrough, err := h.AppliedThrough()
	require.NoError(t, err)
	require.Equal(t, second.Sequence, appliedThrough)
	secondEvent := makeEvent("r2")
	secondPage, readErr := h.Read(second.Sequence-1, second.Sequence, 1)
	require.NoError(t, readErr)
	secondEvent = secondPage.Events[0].InputEvent
	require.NoError(t, w.AppendProjected(t.Context(), secondEvent, second.Sequence, meta))
	third := makeEvent("r3")
	thirdReceipt := appendEvent(3, third)
	require.NoError(t, w.AppendProjected(t.Context(), third, thirdReceipt.Sequence, meta))
	intent, found, err := h.ProjectionIntentFor(history.TargetMain, thirdReceipt.Sequence)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), intent.SegmentSequence)

	future := appendEvent(4, makeEvent("r4"))
	planned := makeEvent("r4")
	planned.Seq = 5
	intent = history.ProjectionIntent{Target: history.TargetMain, JournalSequence: future.Sequence, SegmentSequence: 5, Fingerprint: attribution.FingerprintSegment(planned), Disposition: history.Apply, PlannedEvent: &planned}
	require.NoError(t, h.PrepareProjection(intent))
	require.ErrorIs(t, w.AppendProjected(t.Context(), makeEvent("r4"), future.Sequence, meta), history.ErrProjectionConflict)
	require.EqualValues(t, 4, w.NextSeq())
}
