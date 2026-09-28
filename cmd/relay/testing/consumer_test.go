package testing

import (
	"errors"
	"strings"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
)

func TestConsumerWaitsForDeliveryAndReturnsSnapshot(t *testing.T) {
	c := NewConsumer("ws://example.test")
	c.Timeout = time.Second

	type consumeResult struct {
		count int
		err   error
	}
	resultCh := make(chan consumeResult, 1)
	go func() {
		events, err := c.ConsumeEvents(1)
		resultCh <- consumeResult{count: len(events), err: err}
	}()

	if err := c.eventCallbacks().RepoIdentity(&comatproto.SyncSubscribeRepos_Identity{Seq: 42}); err != nil {
		t.Fatal(err)
	}
	got := <-resultCh
	if got.err != nil || got.count != 1 {
		t.Fatalf("ConsumeEvents() = (%d events, %v), want one event", got.count, got.err)
	}

	events, err := c.ConsumeEvents(1)
	if err != nil {
		t.Fatal(err)
	}
	c.Clear()
	if len(events) != 1 || c.Count() != 0 {
		t.Fatalf("returned snapshot changed after Clear: snapshot=%d current=%d", len(events), c.Count())
	}
}

func TestConsumerReportsStreamFailureWithoutWaitingForTimeout(t *testing.T) {
	c := NewConsumer("ws://example.test")
	c.Timeout = time.Second
	c.done = make(chan struct{})
	c.streamErr = errors.New("fixture disconnected")
	close(c.done)

	_, err := c.ConsumeEvents(1)
	if err == nil || !strings.Contains(err.Error(), "fixture disconnected") || !strings.Contains(err.Error(), "0/1 events") {
		t.Fatalf("ConsumeEvents() error = %v, want disconnect and event count", err)
	}
}

func TestConsumerTimeoutReportsProgress(t *testing.T) {
	c := NewConsumer("ws://example.test")
	c.Timeout = 0
	if err := c.eventCallbacks().RepoIdentity(&comatproto.SyncSubscribeRepos_Identity{Seq: 42}); err != nil {
		t.Fatal(err)
	}

	_, err := c.ConsumeEvents(2)
	if err == nil || !strings.Contains(err.Error(), "received 1, last seq 42") {
		t.Fatalf("ConsumeEvents() error = %v, want observed count and last sequence", err)
	}
}
