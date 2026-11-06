package live

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestConvertSourceEventPreservesOriginalOperationOrdinals(t *testing.T) {
	evt, _ := buildCommit(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"org.example.excluded", "skip"},
		struct{ Coll, Rkey string }{"org.example.selected", "keep"},
	)
	got, err := ConvertSourceEvent(evt, testWitnessedAt, []string{"org.example.selected"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.EqualValues(t, 1, got[0].Ordinal)
	require.Equal(t, "keep", got[0].Event.Rkey)
	require.NotEmpty(t, got[0].CBOR)
	var decoded comatproto.SyncSubscribeRepos_RepoOp
	require.NoError(t, decoded.UnmarshalCBOR(got[0].CBOR))
	require.Equal(t, evt.Commit.Ops[1].Path, decoded.Path)
}

func TestConvertSourceEventDeleteRetainsPriorCIDAndSelectedBlockIsExact(t *testing.T) {
	evt, payloads := buildCommit(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"org.example.selected", "one"},
	)
	prior := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("prior record"))
	evt.Commit.Ops = append(evt.Commit.Ops, comatproto.SyncSubscribeRepos_RepoOp{
		Action: "delete", Path: "org.example.selected/gone", CID: gt.Some(lextypes.LexCIDLink{Link: prior.String()}),
	})
	got, err := ConvertSourceEvent(evt, testWitnessedAt, []string{"org.example.selected"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.EqualValues(t, 0, got[0].Ordinal)
	require.EqualValues(t, 1, got[1].Ordinal)
	require.Equal(t, payloads[0], got[0].Event.Payload)
	var create comatproto.SyncSubscribeRepos_RepoOp
	require.NoError(t, create.UnmarshalCBOR(got[0].CBOR))
	require.Equal(t, evt.Commit.Ops[0].Path, create.Path)
	var deletion comatproto.SyncSubscribeRepos_RepoOp
	require.NoError(t, deletion.UnmarshalCBOR(got[1].CBOR))
	require.True(t, deletion.CID.HasVal())
	require.Equal(t, prior.String(), deletion.CID.Val().Link)
}

func TestSourceEncodingIgnoresLocalClocksButConflictsOnSourcePayload(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	journal, err := history.Open(db)
	require.NoError(t, err)
	capture := OriginalCapture{Journal: journal, Domain: "relay", Epoch: "epoch"}
	row := OriginalOperation{Ordinal: 3, Path: "org.example.selected/r", DID: "did:plc:a", Collection: "org.example.selected", Revision: "rev", Kind: "create", ObservedAt: 100, Payload: []byte("source")}
	src := SourceEvent{Cursor: 7, Raw: []byte("operation cbor"), Rows: []OriginalOperation{row}}
	capture.Policy = func() selection.Policy {
		return selection.Policy{Revision: 1, Collections: []string{"org.example.selected"}}
	}
	first, err := capture.CaptureOriginal(src)
	require.NoError(t, err)
	row.ObservedAt = 999
	src.Rows[0] = row
	second, err := capture.CaptureOriginal(src)
	require.NoError(t, err)
	require.True(t, second[0].Duplicate)
	require.Equal(t, first[0].Sequence, second[0].Sequence)
	row.Payload = []byte("changed")
	src.Rows[0] = row
	_, err = capture.CaptureOriginal(src)
	require.ErrorIs(t, err, history.ErrConflict)
	page, err := journal.Read(0, 10, 10)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.JSONEq(t, `{"version":1,"ordinal":3,"path":"org.example.selected/r","did":"did:plc:a","collection":"org.example.selected","revision":"rev","kind":"create","operation":"c291cmNl","payload":"c291cmNl","sourceEvidence":"b3BlcmF0aW9uIGNib3I="}`, string(page.Events[0].Original))
}
