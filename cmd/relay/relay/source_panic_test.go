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
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/parallel"
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
// rather than acknowledged, and each redial waits for a growing backoff.
func TestSourceHandlerPanicBacksOffAndRetries(t *testing.T) {
	f := newSourceFixture(t)
	r, db := testRelayWithHostDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	host := models.Host{Hostname: f.host, NoSSL: true, LastSeq: 10, AccountLimit: 100}
	require.NoError(t, db.Create(&host).Error)

	var calls atomic.Int64
	processed := make(chan int64, 1)
	config := DefaultSlurperConfig()
	config.ConcurrencyPerHost = 1
	config.PersistCursorCallback = r.PersistHostCursors
	config.PersistHostStatusCallback = r.UpdateHostStatus
	config.TestDialContext = (&net.Dialer{}).DialContext
	s, err := NewSlurper(func(_ context.Context, evt *stream.XRPCStreamEvent, _ string, _ uint64) error {
		if calls.Add(1) <= 2 {
			panic("injected handler panic")
		}
		processed <- evt.Sequence()
		return nil
	}, config)
	require.NoError(t, err)
	waiting := make(chan int)
	release := make(chan struct{})
	s.waitAfterHandlerPanic = func(ctx context.Context, attempt int) bool {
		select {
		case waiting <- attempt:
		case <-ctx.Done():
			return false
		}
		select {
		case <-release:
			return true
		case <-ctx.Done():
			return false
		}
	}
	t.Cleanup(func() {
		_ = s.KillUpstreamConnection(context.Background(), host.Hostname, false)
		require.NoError(t, s.Shutdown())
	})

	before := handlerPanicCount(t, host.Hostname)
	require.NoError(t, s.Subscribe(&host))
	evt := &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{Seq: 11, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z"}}

	conn := f.next(t)
	for attempt := 1; attempt <= 2; attempt++ {
		require.Equal(t, "10", conn.cursor)
		conn.emit(t, evt)
		select {
		case got := <-waiting:
			require.Equal(t, attempt, got)
		case <-time.After(5 * time.Second):
			t.Fatal("source did not wait after the handler panic")
		}
		select {
		case <-f.connections:
			t.Fatal("source redialed before the backoff was released")
		case <-time.After(100 * time.Millisecond):
		}
		require.Equal(t, before+float64(attempt), handlerPanicCount(t, host.Hostname))
		release <- struct{}{}
		conn = f.next(t)
	}

	require.Equal(t, "10", conn.cursor)
	conn.emit(t, evt)
	select {
	case seq := <-processed:
		require.Equal(t, int64(11), seq)
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not retried")
	}
}

func TestBackoffGrowsWithAttempts(t *testing.T) {
	require.Greater(t, sleepForBackoff(2), sleepForBackoff(1))
	require.Greater(t, sleepForBackoff(1), time.Duration(0))
}

// hypercerts: a panic before any acknowledged cursor must not let the redial
// resume at the live tail; it replays from the first observed sequence.
func TestSourceHandlerPanicWithoutCursorRetainsReplayBoundary(t *testing.T) {
	f := newSourceFixture(t)
	r, db := testRelayWithHostDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	host := models.Host{Hostname: f.host, NoSSL: true, LastSeq: -1, AccountLimit: 100}
	require.NoError(t, db.Create(&host).Error)

	var calls atomic.Int64
	processed := make(chan int64, 1)
	config := DefaultSlurperConfig()
	config.ConcurrencyPerHost = 1
	config.PersistCursorCallback = r.PersistHostCursors
	config.PersistHostStatusCallback = r.UpdateHostStatus
	config.TestDialContext = (&net.Dialer{}).DialContext
	s, err := NewSlurper(func(_ context.Context, evt *stream.XRPCStreamEvent, _ string, _ uint64) error {
		if calls.Add(1) <= 1 {
			panic("injected handler panic")
		}
		processed <- evt.Sequence()
		return nil
	}, config)
	require.NoError(t, err)
	waiting := make(chan int)
	release := make(chan struct{})
	s.waitAfterHandlerPanic = func(ctx context.Context, attempt int) bool {
		select {
		case waiting <- attempt:
		case <-ctx.Done():
			return false
		}
		select {
		case <-release:
			return true
		case <-ctx.Done():
			return false
		}
	}
	t.Cleanup(func() {
		_ = s.KillUpstreamConnection(context.Background(), host.Hostname, false)
		require.NoError(t, s.Shutdown())
	})

	require.NoError(t, s.Subscribe(&host))
	evt := &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{Seq: 11, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z"}}

	conn := f.next(t)
	require.Empty(t, conn.cursor, "a source without an acknowledged cursor subscribes cursorless")
	conn.emit(t, evt)
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("source did not wait after the handler panic")
	}
	select {
	case <-f.connections:
		t.Fatal("source redialed before the backoff was released")
	case <-time.After(100 * time.Millisecond):
	}
	release <- struct{}{}

	retry := f.next(t)
	require.Equal(t, "10", retry.cursor, "the redial replays the failed event and the events emitted during backoff")
	retry.emit(t, evt)
	select {
	case seq := <-processed:
		require.Equal(t, int64(11), seq)
	case <-time.After(5 * time.Second):
		t.Fatal("source event was not retried")
	}
}

// hypercerts: the replay boundary must cover events handed to the scheduler
// even when their callbacks never run. With two workers, seq 12 panics and
// stops the scheduler while seq 11 — handed earlier but still queued behind
// the occupied workers — never reaches a callback; a boundary recorded in the
// callback would be 12 and the redial cursor would skip event 11.
func TestRateSchedulerRecordsHandoffBeforeDispatch(t *testing.T) {
	var firstSeen atomic.Int64
	noteSeen := func(seq int64) {
		for {
			cur := firstSeen.Load()
			if seq <= 0 || (cur != 0 && seq >= cur) {
				return
			}
			if firstSeen.CompareAndSwap(cur, seq) {
				return
			}
		}
	}

	startedPanic := make(chan struct{}, 1)
	startedBlocker := make(chan struct{}, 1)
	releasePanic := make(chan struct{})
	dispatched := make(chan int64, 4)
	scheduler := parallel.NewScheduler(2, 8, "handoff-test", func(ctx context.Context, evt *stream.XRPCStreamEvent) error {
		switch evt.Sequence() {
		case 12: // occupies the first worker, then panics on release
			startedPanic <- struct{}{}
			select {
			case <-releasePanic:
				panic("injected handler panic")
			case <-ctx.Done():
				return nil
			}
		case 13: // occupies the second worker until the scheduler stops
			startedBlocker <- struct{}{}
			<-ctx.Done()
			return nil
		default:
			dispatched <- evt.Sequence()
			return nil
		}
	})
	t.Cleanup(scheduler.Shutdown)

	rs := rateScheduler{Scheduler: scheduler, noteSeq: noteSeen}
	identity := func(seq int64) *stream.XRPCStreamEvent {
		return &stream.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{Seq: seq, Did: "did:plc:abcdefghijklmnopqrstuvwx", Time: "2026-09-07T12:00:00Z"}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, rs.AddWork(ctx, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", identity(12)))
	<-startedPanic
	require.NoError(t, rs.AddWork(ctx, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", identity(13)))
	<-startedBlocker
	// seq 11 is handed while both workers are occupied, so it stays queued
	// and its callback never runs once the panic stops the scheduler.
	require.NoError(t, rs.AddWork(ctx, "did:plc:cccccccccccccccccccccccc", identity(11)))
	require.Equal(t, int64(11), firstSeen.Load(), "the hand-off, not the callback, records the replay boundary")

	close(releasePanic)
	<-scheduler.Done()
	require.ErrorIs(t, scheduler.Err(), parallel.ErrHandlerPanic)
	require.Equal(t, int64(11), firstSeen.Load(), "the replay boundary must include the undispatched event")
	select {
	case seq := <-dispatched:
		t.Fatalf("event %d was dispatched despite the stopped scheduler", seq)
	default:
	}
}
