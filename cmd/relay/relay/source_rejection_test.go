package relay

import (
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/stretchr/testify/require"
)

func TestRejectedSourcePositionReusesDurableDecision(t *testing.T) {
	ident, commit := loadIngestFixture(t)
	r, _, output := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, nil)
	invalid := commit
	invalid.Blocks = []byte{0xff}
	require.NoError(t, r.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &invalid}, "source.example", ingestTestHostID))
	require.NoError(t, r.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID))
	require.Empty(t, output.events)

	r.Config.LenientSyncValidation = true
	require.NoError(t, r.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID))
	require.Len(t, output.events, 1)
}

func TestMalformedMetadataEventsAreDurablyRejected(t *testing.T) {
	for _, kind := range []string{"identity", "account"} {
		t.Run(kind, func(t *testing.T) {
			ident, _ := loadIngestFixture(t)
			r, _, output := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, nil)
			evt := &stream.XRPCStreamEvent{}
			if kind == "identity" {
				evt.RepoIdentity = &comatproto.SyncSubscribeRepos_Identity{Did: "invalid", Seq: 17}
			} else {
				evt.RepoAccount = &comatproto.SyncSubscribeRepos_Account{Did: "invalid", Seq: 17}
			}
			require.NoError(t, r.processRepoEvent(t.Context(), evt, "source.example", ingestTestHostID))
			rejections, err := r.ListRejectedEvents(t.Context(), 0, 100)
			require.NoError(t, err)
			require.Len(t, rejections, 1)
			require.Equal(t, kind, rejections[0].EventKind)
			require.Empty(t, rejections[0].DID)
			require.Empty(t, output.events)
		})
	}
}

func TestInitialCommitStillValidatesTimestampAndRoot(t *testing.T) {
	for _, field := range []string{"time", "commit"} {
		t.Run(field, func(t *testing.T) {
			ident, commit := loadIngestFixture(t)
			r, _, output := newIngestTestRelay(t, newFixtureDirectory(ident), ident.DID, true, nil)
			if field == "time" {
				commit.Time = "invalid"
			} else {
				commit.Commit = testSourceCommit(t, commit.Repo, commit.Seq).Commit
			}
			require.NoError(t, r.processRepoEvent(t.Context(), &stream.XRPCStreamEvent{RepoCommit: &commit}, "source.example", ingestTestHostID))
			rejections, err := r.ListRejectedEvents(t.Context(), 0, 100)
			require.NoError(t, err)
			require.Len(t, rejections, 1)
			require.Empty(t, output.events)
		})
	}
}
