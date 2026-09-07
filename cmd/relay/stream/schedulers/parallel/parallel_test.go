package parallel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/stretchr/testify/require"
)

func schedulerEvent(seq int64) *stream.XRPCStreamEvent {
	return &stream.XRPCStreamEvent{RepoCommit: &atproto.SyncSubscribeRepos_Commit{Seq: seq}}
}

func requireReceive(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduler")
	}
}

func TestSchedulerFailureStopsAcknowledgementAfterLaterCompletion(t *testing.T) {
	handlerErr := errors.New("handler failed")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondFinished := make(chan struct{})

	scheduler := NewScheduler(2, 0, t.Name(), func(_ context.Context, event *stream.XRPCStreamEvent) error {
		switch event.Sequence() {
		case 1:
			close(firstStarted)
			<-releaseFirst
			return handlerErr
		case 2:
			close(secondFinished)
		}
		return nil
	})
	defer scheduler.Shutdown()

	require.NoError(t, scheduler.AddWork(t.Context(), "repo-one", schedulerEvent(1)))
	requireReceive(t, firstStarted)
	require.NoError(t, scheduler.AddWork(t.Context(), "repo-two", schedulerEvent(2)))
	requireReceive(t, secondFinished)
	require.Equal(t, int64(0), scheduler.LastSeq())

	close(releaseFirst)
	requireReceive(t, scheduler.Done())
	require.ErrorIs(t, scheduler.Err(), handlerErr)
	require.Equal(t, int64(0), scheduler.LastSeq())
	require.ErrorIs(t, scheduler.AddWork(t.Context(), "repo-three", schedulerEvent(3)), handlerErr)
}

func TestSchedulerFailureDropsQueuedWorkForSameRepository(t *testing.T) {
	handlerErr := errors.New("handler failed")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})

	scheduler := NewScheduler(1, 0, t.Name(), func(_ context.Context, event *stream.XRPCStreamEvent) error {
		if event.Sequence() == 1 {
			close(firstStarted)
			<-releaseFirst
			return handlerErr
		}
		close(secondStarted)
		return nil
	})
	defer scheduler.Shutdown()

	require.NoError(t, scheduler.AddWork(t.Context(), "repo", schedulerEvent(1)))
	requireReceive(t, firstStarted)
	require.NoError(t, scheduler.AddWork(t.Context(), "repo", schedulerEvent(2)))

	close(releaseFirst)
	requireReceive(t, scheduler.Done())
	require.ErrorIs(t, scheduler.Err(), handlerErr)
	select {
	case <-secondStarted:
		t.Fatal("queued work ran after a handler failure")
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, int64(0), scheduler.LastSeq())
}

func TestSchedulerCancellationStopsQueuedWork(t *testing.T) {
	firstStarted := make(chan struct{})
	cancelled := make(chan error, 1)
	secondStarted := make(chan struct{})

	scheduler := NewScheduler(1, 0, t.Name(), func(ctx context.Context, event *stream.XRPCStreamEvent) error {
		if event.Sequence() == 1 {
			close(firstStarted)
			<-ctx.Done()
			return ctx.Err()
		}
		close(secondStarted)
		return nil
	})
	defer scheduler.Shutdown()

	require.NoError(t, scheduler.AddWork(t.Context(), "repo-one", schedulerEvent(1)))
	requireReceive(t, firstStarted)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		cancelled <- scheduler.AddWork(ctx, "repo-two", schedulerEvent(2))
	}()
	require.Eventually(t, func() bool {
		scheduler.lk.Lock()
		defer scheduler.lk.Unlock()
		_, queued := scheduler.active["repo-two"]
		return queued
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-cancelled, context.Canceled)
	requireReceive(t, scheduler.Done())
	require.ErrorIs(t, scheduler.Err(), context.Canceled)
	require.Equal(t, int64(0), scheduler.LastSeq())
	select {
	case <-secondStarted:
		t.Fatal("cancelled work ran")
	default:
	}
}

func TestSchedulerQueuedEventBlocksLaterAcknowledgement(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	handlerErr := errors.New("queued event failed")
	scheduler := NewScheduler(2, 4, t.Name(), func(ctx context.Context, evt *stream.XRPCStreamEvent) error {
		switch evt.Sequence() {
		case 1:
			close(firstStarted)
			select {
			case <-releaseFirst:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case 2:
			close(secondStarted)
			select {
			case <-releaseSecond:
				return handlerErr
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	defer scheduler.Shutdown()
	require.NoError(t, scheduler.AddWork(t.Context(), "first", schedulerEvent(1)))
	requireReceive(t, firstStarted)
	require.NoError(t, scheduler.AddWork(t.Context(), "first", schedulerEvent(2)))
	require.NoError(t, scheduler.AddWork(t.Context(), "other", schedulerEvent(3)))
	require.Eventually(t, func() bool {
		scheduler.lk.Lock()
		defer scheduler.lk.Unlock()
		for work := range scheduler.completed {
			if work.val.Sequence() == 3 {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	close(releaseFirst)
	requireReceive(t, secondStarted)
	require.Equal(t, int64(1), scheduler.LastSeq())
	close(releaseSecond)
	requireReceive(t, scheduler.Done())
	require.ErrorIs(t, scheduler.Err(), handlerErr)
	require.Equal(t, int64(1), scheduler.LastSeq())
}
