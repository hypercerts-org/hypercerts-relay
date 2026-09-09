package relay

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RussellLuo/slidingwindow"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

type sourceConnection struct {
	conn   *websocket.Conn
	cursor string
	closed chan struct{}
}

type sourceFixture struct {
	host        string
	connections chan *sourceConnection
}

func newSourceFixture(t *testing.T) *sourceFixture {
	t.Helper()
	f := &sourceFixture{connections: make(chan *sourceConnection, 16)}
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xrpc/com.atproto.server.describeServer" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"did":"did:web:fixture.example","availableUserDomains":[]}`))
			return
		}
		if r.URL.Path != "/xrpc/com.atproto.sync.subscribeRepos" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		c := &sourceConnection{conn: conn, cursor: r.URL.Query().Get("cursor"), closed: make(chan struct{})}
		f.connections <- c
		defer close(c.closed)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	f.host = "localhost:" + u.Port()
	return f
}

func (f *sourceFixture) next(t *testing.T) *sourceConnection {
	t.Helper()
	select {
	case c := <-f.connections:
		t.Cleanup(func() { _ = c.conn.Close() })
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("source connection did not arrive")
		return nil
	}
}

func (c *sourceConnection) emit(t *testing.T, evt *stream.XRPCStreamEvent) {
	t.Helper()
	w, err := c.conn.NextWriter(websocket.BinaryMessage)
	require.NoError(t, err)
	require.NoError(t, evt.Serialize(w))
	require.NoError(t, w.Close())
}

func testStreamLimiters(counts StreamLimiterCounts) *StreamLimiters {
	second, _ := slidingwindow.NewLimiter(time.Second, counts.PerSecond, windowFunc)
	hour, _ := slidingwindow.NewLimiter(time.Hour, counts.PerHour, windowFunc)
	day, _ := slidingwindow.NewLimiter(24*time.Hour, counts.PerDay, windowFunc)
	return &StreamLimiters{PerSecond: second, PerHour: hour, PerDay: day}
}

func testSourceCommit(t *testing.T, did string, seq int64) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()
	c, err := (cid.V1Builder{Codec: cid.DagCBOR, MhType: 0x12}).Sum([]byte("fixture"))
	require.NoError(t, err)
	return &comatproto.SyncSubscribeRepos_Commit{
		Seq: seq, Repo: did, Commit: lexutil.LexLink(c), Blocks: []byte{},
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{}, Blobs: []lexutil.LexLink{},
		Rev: "3m3b4fywvy22a", Time: "2026-09-07T12:00:00Z",
	}
}

func TestSourceHandlerFailurePreservesCursor(t *testing.T) {
	for _, kind := range []string{"identity", "account", "commit", "sync"} {
		t.Run(kind, func(t *testing.T) {
			f := newSourceFixture(t)
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+f.host+"/xrpc/com.atproto.sync.subscribeRepos", nil)
			require.NoError(t, err)
			defer conn.Close()
			peer := f.next(t)
			failure := errors.New("injected processing failure")
			s := &Slurper{
				Config: DefaultSlurperConfig(), logger: slog.Default(),
				processCallback: func(context.Context, *stream.XRPCStreamEvent, string, uint64) error { return failure },
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			counts := s.ComputeLimiterCounts(100, false)
			sub := &Subscription{Hostname: f.host, HostID: 1, ctx: ctx, cancel: cancel, Limiters: testStreamLimiters(counts)}
			sub.LastSeq.Store(10)
			result := make(chan error, 1)
			go func() { result <- s.handleConnection(ctx, conn, sub) }()
			did := "did:plc:abcdefghijklmnopqrstuvwx"
			evt := &stream.XRPCStreamEvent{}
			switch kind {
			case "identity":
				evt.RepoIdentity = &comatproto.SyncSubscribeRepos_Identity{Seq: 11, Did: did, Time: "2026-09-07T12:00:00Z"}
			case "account":
				evt.RepoAccount = &comatproto.SyncSubscribeRepos_Account{Seq: 11, Did: did, Active: true, Time: "2026-09-07T12:00:00Z"}
			case "commit":
				evt.RepoCommit = testSourceCommit(t, did, 11)
			case "sync":
				evt.RepoSync = &comatproto.SyncSubscribeRepos_Sync{Seq: 11, Did: did, Rev: "3m3b4fywvy22a", Blocks: []byte{}, Time: "2026-09-07T12:00:00Z"}
			}
			peer.emit(t, evt)
			select {
			case err := <-result:
				require.ErrorIs(t, err, failure)
			case <-time.After(5 * time.Second):
				t.Fatal("handler failure did not close the source connection")
			}
			require.Equal(t, int64(10), sub.LastSeq.Load())
		})
	}
}

func TestSourceReconnectUsesSuccessfulCursor(t *testing.T) {
	f := newSourceFixture(t)
	r, db := testRelayWithHostDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	host := models.Host{Hostname: f.host, NoSSL: true, LastSeq: 10, AccountLimit: 100}
	require.NoError(t, db.Create(&host).Error)
	failed := false
	processed := make(chan int64, 8)
	config := DefaultSlurperConfig()
	config.ConcurrencyPerHost = 1
	config.PersistCursorCallback = r.PersistHostCursors
	config.PersistHostStatusCallback = r.UpdateHostStatus
	s, err := NewSlurper(func(_ context.Context, evt *stream.XRPCStreamEvent, _ string, _ uint64) error {
		if evt.Sequence() == 12 && !failed {
			failed = true
			return errors.New("injected processing failure")
		}
		processed <- evt.Sequence()
		return nil
	}, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = s.KillUpstreamConnection(context.Background(), host.Hostname, false)
		require.NoError(t, s.Shutdown())
	})
	require.NoError(t, s.Subscribe(&host))
	first := f.next(t)
	require.Equal(t, "10", first.cursor)
	first.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{Seq: 11, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z"}})
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not processed")
	}
	require.Eventually(t, func() bool {
		if err := s.persistCursors(context.Background()); err != nil {
			return false
		}
		got, err := r.GetHostByID(context.Background(), host.ID)
		return err == nil && got.LastSeq == 11
	}, 5*time.Second, time.Millisecond)
	first.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{Seq: 12, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z"}})
	second := f.next(t)
	require.Equal(t, "11", second.cursor)
	second.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{Seq: 12, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z"}})
	select {
	case seq := <-processed:
		require.Equal(t, int64(12), seq)
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not retried")
	}
}

func TestSourceShutdownFlushesCursorWithoutCancelledWrite(t *testing.T) {
	f := newSourceFixture(t)
	r, db := testRelayWithHostDB(t)
	host := models.Host{Hostname: f.host, NoSSL: true, LastSeq: 10, AccountLimit: 100}
	require.NoError(t, db.Create(&host).Error)

	processed := make(chan struct{}, 1)
	var cancelledWrites atomic.Int32
	config := DefaultSlurperConfig()
	config.ConcurrencyPerHost = 1
	config.PersistCursorPeriod = time.Hour
	config.PersistCursorCallback = func(ctx context.Context, cursors *[]HostCursor) error {
		if ctx.Err() != nil {
			cancelledWrites.Add(1)
		}
		return r.PersistHostCursors(ctx, cursors)
	}
	s, err := NewSlurper(func(context.Context, *stream.XRPCStreamEvent, string, uint64) error {
		processed <- struct{}{}
		return nil
	}, config)
	require.NoError(t, err)
	require.NoError(t, s.Subscribe(&host))
	connection := f.next(t)
	connection.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
		Seq: 11, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z",
	}})
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not processed")
	}
	require.Eventually(t, func() bool {
		s.subsLk.Lock()
		sub := s.subs[host.Hostname]
		s.subsLk.Unlock()
		if sub == nil {
			return false
		}
		sub.lk.RLock()
		defer sub.lk.RUnlock()
		return sub.scheduler != nil && sub.scheduler.LastSeq() == 11
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, s.Shutdown())
	require.Zero(t, cancelledWrites.Load())
	saved, err := r.GetHostByID(context.Background(), host.ID)
	require.NoError(t, err)
	require.Equal(t, int64(11), saved.LastSeq)
}

func TestSourceRestartAfterCursorStorageFailure(t *testing.T) {
	f := newSourceFixture(t)
	r, db := testRelayWithHostDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	host := models.Host{Hostname: f.host, NoSSL: true, LastSeq: 10, AccountLimit: 100}
	require.NoError(t, db.Create(&host).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_cursor BEFORE UPDATE OF last_seq ON host
		BEGIN SELECT RAISE(ABORT, 'injected cursor failure'); END`).Error)
	processed := make(chan struct{}, 2)
	config := DefaultSlurperConfig()
	config.PersistCursorPeriod = time.Hour
	config.PersistCursorCallback = r.PersistHostCursors
	config.PersistHostStatusCallback = r.UpdateHostStatus
	callback := func(context.Context, *stream.XRPCStreamEvent, string, uint64) error {
		processed <- struct{}{}
		return nil
	}
	s, err := NewSlurper(callback, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Shutdown() })
	require.NoError(t, s.Subscribe(&host))
	first := f.next(t)
	first.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
		Seq: 11, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z",
	}})
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not processed")
	}
	require.Eventually(t, func() bool {
		return s.persistCursors(context.Background()) != nil
	}, 5*time.Second, time.Millisecond)
	require.ErrorContains(t, s.StopSource(context.Background(), host.Hostname), "injected cursor failure")
	require.NoError(t, s.Shutdown())
	saved, err := r.GetHostByID(context.Background(), host.ID)
	require.NoError(t, err)
	require.Equal(t, int64(10), saved.LastSeq)
	require.NoError(t, db.Exec("DROP TRIGGER reject_cursor").Error)
	restarted, err := NewSlurper(callback, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Shutdown()) })
	require.NoError(t, restarted.Subscribe(saved))
	second := f.next(t)
	require.Equal(t, "10", second.cursor)
	second.emit(t, &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
		Seq: 11, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z",
	}})
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not replayed after restart")
	}
}
