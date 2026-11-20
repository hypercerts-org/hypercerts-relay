package relay

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func handlerPanicCount(t *testing.T, host string) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, handlerPanics.WithLabelValues(host).Write(&m))
	return m.GetCounter().GetValue()
}

// hypercerts: a handler panic is counted, the failed position is replayed
// rather than acknowledged, and the redial waits for the backoff.
func TestSourceHandlerPanicBacksOffAndRetries(t *testing.T) {
	f := newSourceFixture(t)
	r, db := testRelayWithHostDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	host := models.Host{Hostname: f.host, NoSSL: true, LastSeq: 10, AccountLimit: 100}
	require.NoError(t, db.Create(&host).Error)

	var panickedAt atomic.Int64
	processed := make(chan int64, 1)
	config := DefaultSlurperConfig()
	config.ConcurrencyPerHost = 1
	config.PersistCursorCallback = r.PersistHostCursors
	config.PersistHostStatusCallback = r.UpdateHostStatus
	config.TestDialContext = (&net.Dialer{}).DialContext
	s, err := NewSlurper(func(_ context.Context, evt *stream.XRPCStreamEvent, _ string, _ uint64) error {
		if panickedAt.CompareAndSwap(0, time.Now().UnixNano()) {
			panic("injected handler panic")
		}
		processed <- evt.Sequence()
		return nil
	}, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = s.KillUpstreamConnection(context.Background(), host.Hostname, false)
		require.NoError(t, s.Shutdown())
	})

	before := handlerPanicCount(t, host.Hostname)
	require.NoError(t, s.Subscribe(&host))
	evt := &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{Seq: 11, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z"}}

	first := f.next(t)
	require.Equal(t, "10", first.cursor)
	first.emit(t, evt)

	second := f.next(t)
	require.Equal(t, "10", second.cursor)
	require.GreaterOrEqual(t, time.Since(time.Unix(0, panickedAt.Load())), 2*time.Second)
	require.Equal(t, before+1, handlerPanicCount(t, host.Hostname))

	second.emit(t, evt)
	select {
	case seq := <-processed:
		require.Equal(t, int64(11), seq)
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not retried")
	}
}
