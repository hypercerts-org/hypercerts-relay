package parallel

// hypercerts: a panic in the consumer callback must surface as a scheduler
// error.

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/bluesky-social/indigo/cmd/relay/stream"
)

func TestDoSafelyConvertsPanicToError(t *testing.T) {
	boom := errors.New("boom")
	p := &Scheduler{ident: "test", log: slog.Default()}

	called := false
	p.do = func(ctx context.Context, val *stream.XRPCStreamEvent) error {
		called = true
		panic(boom)
	}

	err := p.doSafely(context.Background(), &stream.XRPCStreamEvent{})
	if !called {
		t.Fatal("callback was not invoked")
	}
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped panic error, got: %v", err)
	}
	if want := "event handler panicked: boom"; err.Error() != want {
		t.Fatalf("got %q, want %q", err.Error(), want)
	}
}

// A handler panic must stop the scheduler through the worker, leave the failed
// position unacknowledged, and be counted.
func TestWorkerStopsSchedulerOnHandlerPanic(t *testing.T) {
	boom := errors.New("boom")
	s := NewScheduler(1, 0, t.Name(), func(context.Context, *stream.XRPCStreamEvent) error {
		panic(boom)
	})
	defer s.Shutdown()

	if err := s.AddWork(context.Background(), "did:example:a", schedulerEvent(7)); err != nil {
		t.Fatal(err)
	}
	requireReceive(t, s.Done())

	err := s.Err()
	if !errors.Is(err, boom) || !errors.Is(err, ErrHandlerPanic) {
		t.Fatalf("got %v, want the recovered panic", err)
	}
	if got := s.LastSeq(); got != 0 {
		t.Fatalf("failed position was acknowledged: %d", got)
	}
	if got := s.HandlerPanics(); got != 1 {
		t.Fatalf("got %d recovered panics, want 1", got)
	}
}
