package eventmgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/stretchr/testify/require"
)

type persistenceStub struct {
	err         error
	broadcaster func(*stream.XRPCStreamEvent)
}

func (p *persistenceStub) Persist(context.Context, *stream.XRPCStreamEvent) error {
	return p.err
}

func (*persistenceStub) Playback(context.Context, int64, func(*stream.XRPCStreamEvent) error) error {
	return nil
}

func (*persistenceStub) TakeDownRepo(context.Context, uint64) error {
	return nil
}

func (*persistenceStub) Flush(context.Context) error {
	return nil
}

func (*persistenceStub) Shutdown(context.Context) error {
	return nil
}

func (p *persistenceStub) SetEventBroadcaster(broadcaster func(*stream.XRPCStreamEvent)) {
	p.broadcaster = broadcaster
}

func TestAddEventReturnsPersistenceError(t *testing.T) {
	persistErr := errors.New("persist failed")
	em := NewEventManager(&persistenceStub{err: persistErr})

	err := em.AddEvent(t.Context(), &stream.XRPCStreamEvent{})
	require.ErrorIs(t, err, persistErr)
}

type playbackHookStub struct {
	persistenceStub
	playback func(call int) error
	calls    int
}

func (p *playbackHookStub) Playback(context.Context, int64, func(*stream.XRPCStreamEvent) error) error {
	p.calls++
	return p.playback(p.calls)
}

func TestSubscribeDoesNotRegisterSubscriberCleanedUpDuringPlayback(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	registeredAfterCleanup := false

	stub := &playbackHookStub{}
	em := NewEventManager(stub)
	stub.playback = func(call int) error {
		if call == 1 {
			close(started)
			<-release
			return nil
		}
		// later playbacks only run once the subscriber is registered for live events
		em.subsLk.Lock()
		registeredAfterCleanup = len(em.subs) > 0
		em.subsLk.Unlock()
		return nil
	}

	since := int64(0)
	out, cleanup, err := em.Subscribe(t.Context(), "test", nil, &since)
	require.NoError(t, err)

	// the client disconnects while the first cursor playback is still running
	<-started
	cleanup()
	close(release)

	select {
	case _, ok := <-out:
		require.False(t, ok, "no events expected")
	case <-time.After(5 * time.Second):
		t.Fatal("playback goroutine did not finish")
	}

	require.Equal(t, 1, stub.calls)
	require.False(t, registeredAfterCleanup)
	em.subsLk.Lock()
	defer em.subsLk.Unlock()
	require.Empty(t, em.subs)
}
