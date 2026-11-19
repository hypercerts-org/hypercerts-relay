package parallel

// hypercerts: a panic in the consumer callback (e.g. from malformed upstream
// event data reaching an unchecked path) must surface as a scheduler error,
// not kill the relay process (TECH-932 defense-in-depth for F2/F3/F4).

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/bluesky-social/indigo/cmd/relay/stream"
)

func TestDoSafelyConvertsPanicToError(t *testing.T) {
	boom := errors.New("boom from malformed event")
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
	if want := "event handler panicked: boom from malformed event"; err.Error() != want {
		t.Fatalf("got %q, want %q", err.Error(), want)
	}
}

func TestDoSafelyPassesThroughValue(t *testing.T) {
	sentinel := errors.New("ordinary error")
	p := &Scheduler{ident: "test", log: slog.Default()}
	p.do = func(ctx context.Context, val *stream.XRPCStreamEvent) error { return sentinel }

	if err := p.doSafely(context.Background(), &stream.XRPCStreamEvent{}); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want sentinel", err)
	}
}
