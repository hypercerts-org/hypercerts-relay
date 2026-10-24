package subscribe

import (
	"encoding/json"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestPolicyGapsRemovalReaddAndOpenIntervals(t *testing.T) {
	b3, b8 := uint64(3), uint64(8)
	history := []attribution.Policy{
		{Revision: 1, Collections: []string{"app.example.a", "app.example.b"}},
		{Revision: 2, Collections: []string{"app.example.b"}, EffectiveAfterSeq: &b3},
		{Revision: 3, Collections: []string{"app.example.a", "app.example.b"}, EffectiveAfterSeq: &b8},
		{Revision: 4, Collections: []string{"app.example.a"}, EffectiveAfterSeq: &b8},
	}
	gaps := PolicyGaps(history)
	require.Len(t, gaps, 2)
	require.Equal(t, uint64(4), *gaps[0].FromSeq)
	require.Equal(t, uint64(8), *gaps[0].ThroughSeq)
	require.Equal(t, "app.example.a", *gaps[0].Collection)
	require.Equal(t, uint64(9), *gaps[1].FromSeq)
	require.Nil(t, gaps[1].ThroughSeq)
	require.Equal(t, "app.example.b", *gaps[1].Collection)
}

func TestParseSelectedRequest(t *testing.T) {
	floor := uint64(3)
	config := SelectedConfig{StreamID: "stream-a", CurrentSeq: 9, ReplayFloor: &floor, ReplayEnabled: true}
	for _, tc := range []struct {
		name, contract, stream, cursor string
		wantErr                        error
	}{
		{"unsupported", "other", "", "", ErrUnsupportedContract}, {"empty cursor", SelectedContract, "stream-a", "", ErrInvalidCursor}, {"empty stream", SelectedContract, "", "4", ErrInvalidCursor}, {"strict uint64", SelectedContract, "stream-a", "cursor=4", ErrInvalidCursor}, {"missing cursor", SelectedContract, "stream-a", "", ErrInvalidCursor}, {"missing stream", SelectedContract, "", "4", ErrInvalidCursor}, {"non-integer", SelectedContract, "stream-a", "1.5", ErrInvalidCursor}, {"wrong stream", SelectedContract, "stream-b", "4", ErrStreamMismatch}, {"future", SelectedContract, "stream-a", "10", ErrCursorOutOfRange}, {"replay disabled", SelectedContract, "stream-a", "4", ErrReplayUnavailable}, {"below floor", SelectedContract, "stream-a", "2", ErrReplayUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config
			if tc.name == "replay disabled" {
				c.ReplayEnabled = false
			}
			_, err := ParseSelectedRequest(tc.contract, tc.stream, tc.cursor, c)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
	r, err := ParseSelectedRequest(SelectedContract, "stream-a", "3", config)
	require.NoError(t, err)
	require.Equal(t, uint64(3), *r.Cursor)
	r, err = ParseSelectedRequest("", "", "", config)
	require.NoError(t, err)
	require.Empty(t, r.Contract)
}

func TestEncodeV2SelectedAddsMetadataInsideTypedCommit(t *testing.T) {
	policyRev := uint64(12)
	event := &segment.Event{Seq: 123, WitnessedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.record", Rkey: "r", Payload: []byte{0xa0}}
	body, err := EncodeV2Selected(event, SelectedEventMetadata{StreamID: "stream", PolicyRev: &policyRev, Provenance: attribution.ProvenanceLive})
	require.NoError(t, err)
	var frame struct {
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(body, &frame))
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(frame.Payload, &payload))
	require.Contains(t, string(frame.Payload), `"$type":"network.bsky.jetstream.subscribeEvents#commit"`)
	var meta SelectedEventMetadata
	require.NoError(t, json.Unmarshal(payload["hypercerts"], &meta))
	require.Equal(t, "stream", meta.StreamID)
	require.Equal(t, policyRev, *meta.PolicyRev)
	require.Equal(t, attribution.ProvenanceLive, meta.Provenance)
	require.Contains(t, string(frame.Payload), `"seq":123`)
	require.Contains(t, string(frame.Payload), `"record":{}`)
}

func TestSelectedSyncMetadataOmitsCARAndDisclosesScope(t *testing.T) {
	policyRev := uint64(7)
	secretCAR := []byte("secret-excluded-record-payload")
	event := &segment.Event{Seq: 9, WitnessedAt: 1_700_000_000_000_000, Kind: segment.KindSync, DID: "did:plc:scope", Rev: "rev", Payload: secretCAR}
	metadata := SelectedEventMetadata{StreamID: "stream", PolicyRev: &policyRev, Provenance: attribution.ProvenanceReconciliation, ReconciliationRequired: true, Scope: &SelectedSyncScope{DID: event.DID, Collections: []string{"app.example.admitted"}}}
	body, err := EncodeV2Selected(event, metadata)
	require.NoError(t, err)
	frame := string(body)
	require.NotContains(t, frame, string(secretCAR))
	require.Contains(t, frame, `"did":"did:plc:scope"`)
	require.Contains(t, frame, `"reconciliationRequired":true`)
	require.Contains(t, frame, `"app.example.admitted"`)
	require.NotContains(t, frame, `"blocks":"`)
}

func TestSelectedMetadataLegacyUnknownAndPolicyInfo(t *testing.T) {
	event := attribution.Event{StreamID: "stream", Seq: 1, Provenance: attribution.ProvenanceLegacyUnknown}
	metadata := SelectedMetadata(event)
	require.Nil(t, metadata.PolicyRev)
	body, err := EncodeV2SelectedInfo("PolicyAdopted", "Selected collection policy adopted", map[string]any{"contract": SelectedContract, "streamId": "stream", "policyRevision": uint64(1), "collections": []string{}, "effectiveAfterSeq": nil})
	require.NoError(t, err)
	var frame struct {
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(body, &frame))
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(frame.Payload, &payload))
	require.NotContains(t, string(frame.Payload), `"seq"`)
	require.Contains(t, string(frame.Payload), `"name":"PolicyAdopted"`)
	require.Contains(t, string(frame.Payload), `"contract":"hypercerts-selected-v1"`)
}
