package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/eventmgr"
	"github.com/bluesky-social/indigo/cmd/relay/stream/persist/diskpersist"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestTwoSourcesRetainRawReplayAfterDisconnect(t *testing.T) {
	ctx := context.Background()
	fixtures := []*sourceFixture{newSourceFixture(t), newSourceFixture(t)}
	dids := []syntax.DID{"did:plc:abcdefghijklmnopqrstuvwx", "did:plc:bcdefghijklmnopqrstuvwxy2"}
	dir := identity.NewMockDirectory()
	for i, f := range fixtures {
		dir.Insert(identity.Identity{
			DID: dids[i], Handle: syntax.HandleInvalid,
			Services: map[string]identity.ServiceEndpoint{
				"atproto_pds": {Type: "AtprotoPersonalDataServer", URL: "http://" + f.host},
			},
		})
	}
	root := t.TempDir()
	db, err := gorm.Open(sqlite.Open(filepath.Join(root, "relay.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	persistence, err := diskpersist.NewDiskPersistence(filepath.Join(root, "events"), "", db, nil)
	require.NoError(t, err)
	events := eventmgr.NewEventManager(persistence)
	r, err := NewRelay(db, events, dir, nil)
	require.NoError(t, err)
	persistence.SetUidSource(r)
	r.HostChecker = &HostClient{Client: &http.Client{Timeout: time.Second}}
	t.Cleanup(func() {
		require.NoError(t, r.Slurper.Shutdown())
		require.NoError(t, events.Shutdown(ctx))
	})
	for i, f := range fixtures {
		require.NoError(t, r.SubscribeToHost(ctx, f.host, true, true))
		peer := f.next(t)
		peer.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
			Seq: 1, Did: dids[i].String(), Time: "2026-09-07T12:00:00Z",
		}})
	}
	require.Eventually(t, func() bool {
		if err := r.Slurper.persistCursors(ctx); err != nil {
			return false
		}
		for _, f := range fixtures {
			host, err := r.GetHost(ctx, f.host)
			if err != nil || host.LastSeq != 1 {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Millisecond)
	for _, f := range fixtures {
		require.NoError(t, r.Slurper.StopSource(ctx, f.host))
	}
	since := int64(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = r.HandleSubscribeRepos(w, req, &since, "127.0.0.1")
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/xrpc/com.atproto.sync.subscribeRepos", nil)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	seen := map[string]bool{}
	for range dids {
		kind, reader, err := conn.NextReader()
		require.NoError(t, err)
		require.Equal(t, websocket.BinaryMessage, kind)
		var header stream.EventHeader
		require.NoError(t, header.UnmarshalCBOR(reader))
		require.Equal(t, "#identity", header.MsgType)
		var evt comatproto.SyncSubscribeRepos_Identity
		require.NoError(t, evt.UnmarshalCBOR(reader))
		seen[evt.Did] = true
	}
	for _, did := range dids {
		require.True(t, seen[did.String()])
	}
}
