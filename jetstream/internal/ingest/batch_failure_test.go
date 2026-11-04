package ingest

import (
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

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
			closed := make(chan error, 1)
			go func() { closed <- w.Close() }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("close hung after batch failure")
			}
			for _, row := range rows {
				_, err := manager.Lookup(row.Seq)
				require.NoError(t, err)
			}
		})
	}
}
