package subscribe

import (
	"encoding/json"
	"testing"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestSelectedEventMetadataTypedDecode(t *testing.T) {
	identityPayload, err := (&comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:test", Seq: 99}).MarshalCBOR()
	require.NoError(t, err)
	accountPayload, err := (&comatproto.SyncSubscribeRepos_Account{DID: "did:plc:test", Active: true, Seq: 100}).MarshalCBOR()
	require.NoError(t, err)
	syncPayload, err := (&comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:test", Rev: "rev", Blocks: []byte{1}}).MarshalCBOR()
	require.NoError(t, err)
	policyRev, boundarySeq, snapshotID := uint64(12), uint64(8), "snapshot-a"
	for _, tc := range []struct {
		name     string
		kind     segment.Kind
		payload  []byte
		metadata func(*jetstream.JetstreamSubscribeEvents_Message) gt.Option[jetstream.JetstreamSubscribeEvents_SelectedMetadata]
	}{
		{"commit", segment.KindCreate, []byte{0xa0}, func(msg *jetstream.JetstreamSubscribeEvents_Message) gt.Option[jetstream.JetstreamSubscribeEvents_SelectedMetadata] {
			return msg.JetstreamSubscribeEvents_Commit.Val().Hypercerts
		}},
		{"identity", segment.KindIdentity, identityPayload, func(msg *jetstream.JetstreamSubscribeEvents_Message) gt.Option[jetstream.JetstreamSubscribeEvents_SelectedMetadata] {
			return msg.JetstreamSubscribeEvents_Identity.Val().Hypercerts
		}},
		{"account", segment.KindAccount, accountPayload, func(msg *jetstream.JetstreamSubscribeEvents_Message) gt.Option[jetstream.JetstreamSubscribeEvents_SelectedMetadata] {
			return msg.JetstreamSubscribeEvents_Account.Val().Hypercerts
		}},
		{"sync", segment.KindSync, syncPayload, func(msg *jetstream.JetstreamSubscribeEvents_Message) gt.Option[jetstream.JetstreamSubscribeEvents_SelectedMetadata] {
			return msg.JetstreamSubscribeEvents_Sync.Val().Hypercerts
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := &segment.Event{Seq: 123, WitnessedAt: 1_700_000_000_000_000, Kind: tc.kind, DID: "did:plc:test", Collection: "app.example.record", Rkey: "r", Rev: "rev", Payload: tc.payload}
			metadata := SelectedMetadata(attribution.Event{StreamID: "stream-a", PolicyRev: &policyRev, Provenance: attribution.ProvenanceReconciliation, Boundary: attribution.Boundary{Seq: &boundarySeq, SnapshotID: &snapshotID}})
			if tc.kind == segment.KindSync {
				metadata = selectedSyncMetadata(attribution.Event{StreamID: "stream-a", PolicyRev: &policyRev, Provenance: attribution.ProvenanceReconciliation, Boundary: attribution.Boundary{Seq: &boundarySeq, SnapshotID: &snapshotID}}, event.DID, attribution.Policy{Collections: []string{"app.example.record"}})
			}
			body, encodeErr := EncodeV2Selected(event, metadata)
			require.NoError(t, encodeErr)
			requireSelectedTypedRoundTrip(t, body)
			decoded := tc.metadata(decodeV2Union(t, body))
			require.True(t, decoded.HasVal())
			got := decoded.Val()
			require.Equal(t, gt.Some("stream-a"), got.StreamId)
			require.Equal(t, gt.Some(int64(12)), got.PolicyRev)
			require.Equal(t, gt.Some("reconciliation"), got.Provenance)
			require.NotNil(t, got.ReconciliationBoundary)
			var decodedBoundary jetstream.JetstreamSubscribeEvents_SelectedReconciliationBoundary
			require.NoError(t, decodedBoundary.UnmarshalJSON(got.ReconciliationBoundary))
			require.Equal(t, gt.Some(int64(8)), decodedBoundary.Seq)
			require.Equal(t, gt.Some(snapshotID), decodedBoundary.SnapshotId)
			if tc.kind == segment.KindSync {
				require.Equal(t, gt.Some(true), got.ReconciliationRequired)
				require.True(t, got.Scope.HasVal())
				require.Equal(t, gt.Some(event.DID), got.Scope.Val().DID)
				require.JSONEq(t, `["app.example.record"]`, string(got.Scope.Val().Collections))
			} else {
				require.False(t, got.ReconciliationRequired.HasVal())
				require.False(t, got.Scope.HasVal())
			}
			ordinary, ordinaryErr := EncodeV2(event)
			require.NoError(t, ordinaryErr)
			require.NotContains(t, unwrapV2Frame(t, ordinary), "hypercerts")
			require.False(t, tc.metadata(decodeV2Union(t, ordinary)).HasVal())
			requireSelectedTypedRoundTrip(t, ordinary)
			metadata.PolicyRev = nil
			metadata.Boundary = nil
			metadata.Provenance = attribution.ProvenanceLegacyUnknown
			body, encodeErr = EncodeV2Selected(event, metadata)
			require.NoError(t, encodeErr)
			requireSelectedTypedRoundTrip(t, body)
			legacy := tc.metadata(decodeV2Union(t, body)).Val()
			require.False(t, legacy.PolicyRev.HasVal())
			require.Nil(t, legacy.ReconciliationBoundary)
			require.Equal(t, gt.Some("legacy-unknown"), legacy.Provenance)
		})
	}
}

func TestSelectedInfoMetadataTypedDecode(t *testing.T) {
	boundary, floor, original, from, through := uint64(8), uint64(9), uint64(1), uint64(2), uint64(7)
	collection := "app.example.record"
	for _, tc := range []struct {
		name     string
		metadata map[string]any
	}{
		{"SelectedFeed", map[string]any{"contract": SelectedContract, "streamId": "stream-a", "policyRev": uint64(12), "collections": []string{collection}, "effectiveAfterSeq": &boundary, "replayFloor": &floor, "historyMode": "materialized", "originalHistoryFromSeq": &original, "coverage": "unknown", "gaps": []SelectedGap{{FromSeq: &from, ThroughSeq: &through, Reason: "policy-disabled", Collection: &collection}, {FromSeq: &floor, Reason: "legacy-scope-unknown"}}}},
		{"PolicyAdopted", map[string]any{"contract": SelectedContract, "streamId": "stream-a", "policyRevision": uint64(12), "collections": []string{collection}, "effectiveAfterSeq": &boundary}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := EncodeV2SelectedInfo(tc.name, "Selected stream notice", tc.metadata)
			require.NoError(t, err)
			requireSelectedTypedRoundTrip(t, body)
			info := decodeV2Union(t, body).JetstreamSubscribeEvents_Info.Val()
			require.Equal(t, tc.name, info.Name)
			require.True(t, info.Hypercerts.HasVal())
			got := info.Hypercerts.Val()
			require.Equal(t, gt.Some(SelectedContract), got.Contract)
			require.Equal(t, gt.Some("stream-a"), got.StreamId)
			require.JSONEq(t, `["app.example.record"]`, string(got.Collections))
			require.JSONEq(t, `8`, string(got.EffectiveAfterSeq))
			if tc.name == "SelectedFeed" {
				require.Equal(t, gt.Some(int64(12)), got.PolicyRev)
				require.False(t, got.PolicyRevision.HasVal())
				require.JSONEq(t, `9`, string(got.ReplayFloor))
				require.JSONEq(t, `1`, string(got.OriginalHistoryFromSeq))
				require.Equal(t, gt.Some("materialized"), got.HistoryMode)
				require.Equal(t, gt.Some("unknown"), got.Coverage)
				requireSelectedPolicyGaps(t, got, tc.metadata["gaps"].([]SelectedGap), collection)
			} else {
				require.Equal(t, gt.Some(int64(12)), got.PolicyRevision)
				require.False(t, got.PolicyRev.HasVal())
				require.Nil(t, got.ReplayFloor)
				require.Empty(t, got.Gaps)
			}
			tc.metadata["effectiveAfterSeq"] = nil
			if tc.name == "SelectedFeed" {
				tc.metadata["replayFloor"] = nil
				tc.metadata["originalHistoryFromSeq"] = nil
				tc.metadata["gaps"] = []SelectedGap{}
			}
			tc.metadata["collections"] = []string{}
			body, err = EncodeV2SelectedInfo(tc.name, "Selected stream notice", tc.metadata)
			require.NoError(t, err)
			requireSelectedTypedRoundTrip(t, body)
			nullable := decodeV2Union(t, body).JetstreamSubscribeEvents_Info.Val().Hypercerts.Val()
			require.JSONEq(t, `null`, string(nullable.EffectiveAfterSeq))
			if tc.name == "SelectedFeed" {
				require.JSONEq(t, `null`, string(nullable.ReplayFloor))
				require.JSONEq(t, `null`, string(nullable.OriginalHistoryFromSeq))
				require.JSONEq(t, `[]`, string(nullable.Gaps))
			} else {
				require.Nil(t, nullable.ReplayFloor)
				require.Nil(t, nullable.OriginalHistoryFromSeq)
				require.Nil(t, nullable.Gaps)
			}
			require.JSONEq(t, `[]`, string(nullable.Collections))
		})
	}
	ordinary, err := EncodeV2Info("OutdatedCursor", "Resume at retention floor")
	require.NoError(t, err)
	require.NotContains(t, unwrapV2Frame(t, ordinary), "hypercerts")
	require.False(t, decodeV2Union(t, ordinary).JetstreamSubscribeEvents_Info.Val().Hypercerts.HasVal())
}

func requireSelectedTypedRoundTrip(t *testing.T, frame []byte) {
	t.Helper()
	payload, err := json.Marshal(unwrapV2Frame(t, frame))
	require.NoError(t, err)
	encoded, err := decodeV2Union(t, frame).MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, string(payload), string(encoded))
}

func TestSelectedMetadataTypedRoundTrip(t *testing.T) {
	boundarySeq, snapshotID := uint64(8), "snapshot-a"
	for _, boundary := range []attribution.Boundary{{}, {Seq: &boundarySeq}, {SnapshotID: &snapshotID}} {
		t.Run("event", func(t *testing.T) {
			event := &segment.Event{Seq: 123, WitnessedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "app.example.record", Rkey: "r", Rev: "rev", Payload: []byte{0xa0}}
			metadata := SelectedMetadata(attribution.Event{StreamID: "stream-a", Provenance: attribution.ProvenanceLegacyUnknown, Boundary: boundary})
			frame, err := EncodeV2Selected(event, metadata)
			require.NoError(t, err)
			requireSelectedTypedRoundTrip(t, frame)
			ordinary, err := EncodeV2(event)
			require.NoError(t, err)
			requireSelectedTypedRoundTrip(t, ordinary)
		})
	}
	for _, collections := range [][]string{nil, {}} {
		t.Run("empty-sync-scope", func(t *testing.T) {
			event := &segment.Event{Seq: 123, WitnessedAt: 1_700_000_000_000_000, Kind: segment.KindSync, DID: "did:plc:test", Rev: "rev"}
			metadata := selectedSyncMetadata(attribution.Event{StreamID: "stream-a", Provenance: attribution.ProvenanceReconciliation}, event.DID, attribution.Policy{})
			metadata.Scope.Collections = collections
			frame, err := EncodeV2Selected(event, metadata)
			require.NoError(t, err)
			requireSelectedTypedRoundTrip(t, frame)
		})
	}
	for _, tc := range []struct {
		name     string
		metadata map[string]any
	}{
		{"SelectedFeed", map[string]any{"contract": SelectedContract, "streamId": "stream-a", "policyRev": uint64(12), "collections": []string{}, "effectiveAfterSeq": nil, "replayFloor": nil, "historyMode": "materialized", "originalHistoryFromSeq": nil, "coverage": "unknown", "gaps": []SelectedGap{}}},
		{"SelectedFeed", map[string]any{"contract": SelectedContract, "streamId": "stream-a", "policyRev": uint64(12), "collections": []string{"app.example.record"}, "effectiveAfterSeq": nil, "replayFloor": nil, "historyMode": "materialized", "originalHistoryFromSeq": nil, "coverage": "unknown", "gaps": []SelectedGap{{Reason: "legacy-scope-unknown"}}}},
		{"PolicyAdopted", map[string]any{"contract": SelectedContract, "streamId": "stream-a", "policyRevision": uint64(12), "collections": []string{}, "effectiveAfterSeq": nil}},
		{"Reconciliation", map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := EncodeV2SelectedInfo(tc.name, "Selected stream notice", tc.metadata)
			require.NoError(t, err)
			requireSelectedTypedRoundTrip(t, frame)
		})
	}
	ordinary, err := EncodeV2Info("OutdatedCursor", "Resume at retention floor")
	require.NoError(t, err)
	requireSelectedTypedRoundTrip(t, ordinary)
}

func TestSelectedBoundaryIntegerAndObjectTypedRoundTrip(t *testing.T) {
	for _, boundary := range []string{`8`, `{"seq":8,"snapshotId":null}`, `{"seq":null,"snapshotId":"snapshot-a"}`} {
		payload := []byte(`{"streamId":"stream-a","policyRev":1,"provenance":"snapshot","reconciliationBoundary":` + boundary + `}`)
		var metadata jetstream.JetstreamSubscribeEvents_SelectedMetadata
		require.NoError(t, metadata.UnmarshalJSON(payload))
		encoded, err := metadata.MarshalJSON()
		require.NoError(t, err)
		require.JSONEq(t, string(payload), string(encoded))
	}
}

func requireSelectedPolicyGaps(t *testing.T, got jetstream.JetstreamSubscribeEvents_SelectedInfoMetadata, expected []SelectedGap, collection string) {
	t.Helper()
	var gaps []jetstream.JetstreamSubscribeEvents_SelectedGap
	require.NoError(t, json.Unmarshal(got.Gaps, &gaps))
	require.Len(t, gaps, 2)
	for gapIndex := range gaps {
		encodedGap, gapErr := gaps[gapIndex].MarshalJSON()
		require.NoError(t, gapErr)
		expectedGap, gapErr := json.Marshal(expected[gapIndex])
		require.NoError(t, gapErr)
		require.JSONEq(t, string(expectedGap), string(encodedGap))
	}
	require.Equal(t, gt.Some(int64(2)), gaps[0].FromSeq)
	require.Equal(t, gt.Some(int64(7)), gaps[0].ThroughSeq)
	require.Equal(t, gt.Some("policy-disabled"), gaps[0].Reason)
	require.Equal(t, gt.Some(collection), gaps[0].Collection)
	require.Equal(t, gt.Some(int64(9)), gaps[1].FromSeq)
	require.Equal(t, gt.Some("legacy-scope-unknown"), gaps[1].Reason)
	require.False(t, gaps[1].ThroughSeq.HasVal())
	require.False(t, gaps[1].Collection.HasVal())
}
