package jetstreamd

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestSelectedPolicyGapsFromAdoptedHistory(t *testing.T) {
	tests := []struct {
		name         string
		adoptions    [][]string
		appends      []history.Append
		appendsAfter map[int][]history.Append
		want         []wantGap
		checkZero    bool
	}{
		{name: "never disabled", adoptions: [][]string{{"app.example.a"}, {"app.example.a"}}},
		{name: "removed remains open", adoptions: [][]string{{"app.example.a"}, {}}, appends: []history.Append{historyAppend(1), historyAppend(2)}, want: []wantGap{{"app.example.a", 3, nil}}},
		{name: "removed then readded", adoptions: [][]string{{"app.example.a"}, {}, {"app.example.a"}}, appendsAfter: map[int][]history.Append{0: {historyAppend(1)}, 1: {historyAppend(2)}}, want: []wantGap{{"app.example.a", 2, ptrSeq(2)}}},
		{name: "zero event removal and readd", adoptions: [][]string{{"app.example.a"}, {}, {"app.example.a"}}, checkZero: true},
		{name: "removed future interval open", adoptions: [][]string{{"app.example.a"}, {}, {"app.example.a"}, {}}, appends: []history.Append{historyAppend(1)}, want: []wantGap{{"app.example.a", 2, nil}}},
		{name: "multiple collections", adoptions: [][]string{{"app.example.a", "app.example.b"}, {"app.example.b"}, {"app.example.a", "app.example.b"}}},
		{name: "other collection event during exclusion", adoptions: [][]string{{"app.example.a", "app.example.b"}, {"app.example.b"}}, appends: []history.Append{historyAppendCollection(1, "app.example.b")}, appendsAfter: map[int][]history.Append{1: {historyAppendCollection(2, "app.example.b")}}, want: []wantGap{{"app.example.a", 2, nil}}},
		{name: "readd after one-event gap", adoptions: [][]string{{"app.example.a"}, {"app.example.a"}, {}}, appends: []history.Append{historyAppend(1)}, want: []wantGap{{"app.example.a", 2, nil}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := store.Open(filepath.Join(t.TempDir(), "db"), nil)
			require.NoError(t, err)
			defer db.Close()
			journal, err := history.Open(db)
			require.NoError(t, err)
			for i, collections := range tt.adoptions {
				_, err := journal.AdoptPolicy(history.Policy{Revision: uint64(i + 1), Collections: collections}, int64(i+1))
				require.NoError(t, err)
				if i == 0 {
					for _, appendRow := range tt.appends {
						_, err := journal.Append(appendRow)
						require.NoError(t, err)
					}
				}
				for _, appendRow := range tt.appendsAfter[i] {
					_, err := journal.Append(appendRow)
					require.NoError(t, err)
				}
				state, ok, err := journal.ReadPolicyState()
				require.NoError(t, err)
				require.True(t, ok)
				fresh, err := history.Open(db)
				require.NoError(t, err)
				decoded, decodedOK, err := fresh.ReadPolicyState()
				require.NoError(t, err)
				require.True(t, decodedOK)
				require.Equal(t, state, decoded)
				state = decoded
				if tt.checkZero && i == len(tt.adoptions)-1 {
					require.NotNil(t, state.Intervals[0].ThroughJournalSequence)
					require.Zero(t, *state.Intervals[0].ThroughJournalSequence)
					require.Len(t, selectedPolicyGaps(state, 0), 0)
					require.Equal(t, []history.PolicyTransition{{Collection: "app.example.a", RemovalRevision: 2, ReaddRevision: 3, EffectiveAfterSeq: 0}}, state.Transitions)
				}
			}
			state, ok, err := journal.ReadPolicyState()
			require.NoError(t, err)
			require.True(t, ok)
			tip, err := journal.Tip()
			require.NoError(t, err)
			got := selectedPolicyGaps(state, tip)
			require.Len(t, got, len(tt.want))
			for i, want := range tt.want {
				require.Equal(t, want.collection, *got[i].Collection)
				require.Equal(t, want.from, *got[i].FromSeq)
				require.Equal(t, "policy-disabled", got[i].Reason)
				if want.through == nil {
					require.Nil(t, got[i].ThroughSeq)
				} else {
					require.NotNil(t, got[i].ThroughSeq)
					require.Equal(t, *want.through, *got[i].ThroughSeq)
				}
			}
		})
	}
}

type wantGap struct {
	collection string
	from       uint64
	through    *uint64
}

func ptrSeq(v uint64) *uint64 { return &v }

func historyAppend(cursor uint64) history.Append {
	return historyAppendCollection(cursor, "app.example.a")
}

func historyAppendCollection(cursor uint64, collection string) history.Append {
	return history.Append{Source: history.Source{Domain: "test", Epoch: "epoch", Cursor: cursor, Operation: 1, Path: collection + "/r", ContentHash: history.OriginalHash([]byte("payload"))}, Kind: "put", DID: "did:plc:test", Collection: collection, Revision: "r", Original: []byte("payload"), Attribution: history.Attribution{StreamID: "stream", Provenance: "live", Policy: history.EffectivePolicy{Revision: 1, Collections: []string{collection}}}, InputEvent: segment.Event{Kind: segment.KindCreate, DID: "did:plc:test", Collection: collection, Rkey: "r", Rev: "r", Payload: []byte("payload")}, Policy: history.Policy{Revision: 1, Collections: []string{collection}}, Provenance: "live", Disposition: history.Apply}
}
