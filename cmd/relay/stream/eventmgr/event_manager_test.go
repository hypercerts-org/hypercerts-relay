package eventmgr

import (
	"context"
	"errors"
	"testing"

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
