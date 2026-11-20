package tombstone

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestEncodeScopedSyncRoundTripsAndClassifiesBadInput(t *testing.T) {
	payload, err := cbor.Marshal(map[string]any{"did": "did:plc:scoped", "rev": "3mmoojp7vgo2g"})
	require.NoError(t, err)
	scope := SyncScope{Collections: []string{"app.bsky.feed.post"}, Paths: []string{"app.bsky.feed.post/a"}}
	encoded, err := EncodeScopedSync(payload, scope)
	require.NoError(t, err)
	decoded, err := DecodeScopedSync(&segment.Event{Kind: segment.KindSync, Payload: encoded})
	require.NoError(t, err)
	require.Equal(t, &scope, decoded)

	for _, bad := range [][]byte{nil, {0xff}, {0x01}} {
		_, err := EncodeScopedSync(bad, scope)
		require.ErrorIs(t, err, ErrScopedSyncEncode)
	}
}
