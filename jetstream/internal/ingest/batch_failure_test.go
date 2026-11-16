package ingest

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestSingleAppendFailureDiscardsPreparedCoordinate(t *testing.T) {
	w := newTestWriter(t, Config{})
	manager, err := attribution.Open(w.cfg.Store, "stream", "single-failure")
	require.NoError(t, err)
	w.cfg.Attribution, w.cfg.AttributionDomain = manager, "single-failure"
	metadata := AttributionMetadata{StreamID: "stream", Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceLive}
	event := segment.Event{Kind: segment.KindCreate, DID: strings.Repeat("x", 65536), Collection: "org.example.record", Rkey: "key"}
	require.ErrorIs(t, w.AppendAttributed(t.Context(), &event, metadata), segment.ErrFieldTooLong)
	event.DID = "did:plc:test"
	require.NoError(t, w.AppendAttributed(t.Context(), &event, metadata))
}

func TestBatchPreparationFailureDiscardsEarlierCoordinates(t *testing.T) {
	w := newTestWriter(t, Config{})
	manager, err := attribution.Open(w.cfg.Store, "stream", "prepare-failure")
	require.NoError(t, err)
	w.cfg.Attribution, w.cfg.AttributionDomain = manager, "prepare-failure"
	metadata := AttributionMetadata{StreamID: "stream", Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceLive}
	conflict := segment.Event{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "existing"}
	prepared, err := manager.PrepareSegment(conflict, metadata.StreamID, metadata.Policy, metadata.Provenance, metadata.Boundary)
	require.NoError(t, err)
	candidates := []acceptedCandidate{
		{event: segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "first"}},
		{event: segment.Event{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "conflict"}},
	}
	require.ErrorIs(t, w.prepareCandidateAttributions(candidates, &metadata), attribution.ErrConflict)
	replacement := candidates[0].event
	replacement.Rkey = "replacement"
	_, err = manager.PrepareSegment(replacement, metadata.StreamID, metadata.Policy, metadata.Provenance, metadata.Boundary)
	require.NoError(t, err, "earlier unappended coordinate must be removed")
	require.NoError(t, manager.DiscardPrepared(prepared))
}

func TestAsyncBatchFailureDrainsPreparedJobs(t *testing.T) {
	for _, locked := range []bool{false, true} {
		t.Run(map[bool]string{false: "public", true: "locked"}[locked], func(t *testing.T) {
			w := newTestWriter(t, Config{AsyncFlushWorkers: 1, MaxEventsPerBlock: 1, ReadLogRetentionBytes: 1 << 20})
			manager, err := attribution.Open(w.cfg.Store, "stream", "batch-failure")
			require.NoError(t, err)
			w.cfg.Attribution, w.cfg.AttributionDomain = manager, "batch-failure"
			injected := errors.New("injected append hook failure")
			w.cfg.OnAppend = func(event *segment.Event) error {
				if event.Rkey == "second" {
					return injected
				}
				return nil
			}
			metadata := AttributionMetadata{StreamID: "stream", Policy: attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}}, Provenance: attribution.ProvenanceLive}
			rows := []segment.Event{
				{Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "first", Payload: []byte(`{"ok":true}`)},
				{Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "second", Payload: []byte(`{"ok":true}`)},
				{Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "unappended", Payload: []byte(`{"ok":true}`)},
			}
			done := make(chan error, 1)
			go func() {
				if locked {
					w.drainMu.Lock()
					w.mu.Lock()
					err := w.appendBatchLockedWithMetadata(t.Context(), rows, &metadata)
					w.mu.Unlock()
					w.drainMu.Unlock()
					done <- err
				} else {
					done <- w.AppendBatchAttributed(t.Context(), rows, metadata)
				}
			}()
			select {
			case err := <-done:
				require.ErrorIs(t, err, injected)
			case <-time.After(2 * time.Second):
				t.Fatal("batch did not drain")
			}
			_, err = manager.Lookup(1)
			require.NoError(t, err, "prepared first row must be durable before returning the batch error")
			w.cfg.OnAppend = nil
			require.NoError(t, w.Flush(t.Context()))
			replacement := segment.Event{Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "replacement", Payload: []byte(`{"ok":true}`)}
			require.NoError(t, w.AppendAttributed(t.Context(), &replacement, metadata))
			closed := make(chan error, 1)
			go func() { closed <- w.Close() }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("close hung after batch failure")
			}
			for _, row := range rows[:2] {
				_, err := manager.Lookup(row.Seq)
				require.NoError(t, err)
			}
			_, err = manager.Lookup(replacement.Seq)
			require.NoError(t, err)
		})
	}
}
