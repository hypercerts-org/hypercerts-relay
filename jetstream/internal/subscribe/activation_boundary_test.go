package subscribe

import (
	"math"
	"strconv"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestReplayBoundaryUsesDurableActivationNotInitialPolicy(t *testing.T) {
	for _, boundary := range []*uint64{nil, func() *uint64 { v := uint64(1); return &v }()} {
		st, err := store.Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer st.Close()
		manager, err := attribution.Open(st, "stream", "activation-test")
		require.NoError(t, err)
		policy := attribution.Policy{Revision: 1, Collections: []string{"org.example.record"}, EffectiveAfterSeq: boundary}
		require.NoError(t, manager.EnsureInitialPolicyHistory(policy, boundary))
		activation, err := manager.ActivationSeq()
		require.NoError(t, err)
		require.Zero(t, activation)
		config := ApplySelectedReplayBoundary(SelectedConfig{StreamID: "stream", CurrentSeq: 4, ReplayEnabled: true}, activation)
		require.Nil(t, config.ReplayFloor)
		_, err = ParseSelectedRequest(SelectedContract, "stream", "1", config)
		require.ErrorIs(t, err, ErrReplayUnavailable)
		row := segment.Event{Seq: 5, Kind: segment.KindCreate, DID: "did:plc:test", Collection: "org.example.record", Rkey: "r", Payload: []byte{0xa0}}
		prepared, err := manager.PrepareSegment(row, "stream", policy, attribution.ProvenanceLive, attribution.Boundary{})
		require.NoError(t, err)
		require.NoError(t, manager.CommitBatch([]attribution.Prepared{prepared}, nil))
		activation, err = manager.ActivationSeq()
		require.NoError(t, err)
		require.EqualValues(t, 5, activation)
		history, err := manager.ReadPolicyHistory()
		require.NoError(t, err)
		require.Equal(t, boundary, history[0].EffectiveAfterSeq)
		config = ApplySelectedReplayBoundary(SelectedConfig{StreamID: "stream", CurrentSeq: 5, ReplayEnabled: true}, activation)
		require.EqualValues(t, 5, *config.ReplayFloor)
		_, err = ParseSelectedRequest(SelectedContract, "stream", "4", config)
		require.ErrorIs(t, err, ErrReplayUnavailable)
		_, err = ParseSelectedRequest(SelectedContract, "stream", "5", config)
		require.NoError(t, err)
		_, err = ParseSelectedRequest(SelectedContract, "", "", config)
		require.NoError(t, err)
	}
}

func TestReplayBoundaryWithoutActivationAndAtUintLimit(t *testing.T) {
	for _, tip := range []uint64{0, 8, math.MaxUint64} {
		config := ApplySelectedReplayBoundary(SelectedConfig{StreamID: "stream", CurrentSeq: tip, ReplayEnabled: true}, 0)
		require.Nil(t, config.ReplayFloor)
		require.False(t, config.ReplayEnabled)
		if tip == 0 {
			require.Empty(t, config.Gaps)
		} else {
			require.Equal(t, tip, *config.Gaps[0].ThroughSeq)
		}
		_, err := ParseSelectedRequest(SelectedContract, "stream", strconv.FormatUint(tip, 10), config)
		require.ErrorIs(t, err, ErrReplayUnavailable)
		_, err = ParseSelectedRequest(SelectedContract, "", "", config)
		require.NoError(t, err)
	}
	pending := ApplySelectedReplayBoundary(SelectedConfig{StreamID: "stream", CurrentSeq: 4, ReplayEnabled: true}, 5)
	require.Nil(t, pending.ReplayFloor)
	require.False(t, pending.ReplayEnabled)
	require.EqualValues(t, 4, *pending.Gaps[0].ThroughSeq)
	config := ApplySelectedReplayBoundary(SelectedConfig{StreamID: "stream", CurrentSeq: math.MaxUint64, ReplayEnabled: true}, math.MaxUint64)
	require.Equal(t, uint64(math.MaxUint64), *config.ReplayFloor)
	require.Equal(t, uint64(math.MaxUint64-1), *config.Gaps[0].ThroughSeq)
}
