package testing

import (
	"context"
	"fmt"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/sequential"

	"github.com/gorilla/websocket"
)

// testing helper which receives a set of firehose events
type Consumer struct {
	Host      string
	Events    []*stream.XRPCStreamEvent
	LastSeq   int64
	Timeout   time.Duration
	eventsLk  sync.Mutex
	changed   chan struct{}
	done      chan struct{}
	streamErr error
	cancel    func()
}

func NewConsumer(host string) *Consumer {
	c := Consumer{
		Host:    host,
		Timeout: time.Second * 10,
		changed: make(chan struct{}),
	}
	return &c
}

func (c *Consumer) eventCallbacks() *stream.RepoStreamCallbacks {
	rsc := &stream.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			c.appendEvent(&stream.XRPCStreamEvent{RepoCommit: evt}, evt.Seq)
			return nil
		},
		RepoSync: func(evt *comatproto.SyncSubscribeRepos_Sync) error {
			c.appendEvent(&stream.XRPCStreamEvent{RepoSync: evt}, evt.Seq)
			return nil
		},
		RepoIdentity: func(evt *comatproto.SyncSubscribeRepos_Identity) error {
			c.appendEvent(&stream.XRPCStreamEvent{RepoIdentity: evt}, evt.Seq)
			return nil
		},
		RepoAccount: func(evt *comatproto.SyncSubscribeRepos_Account) error {
			c.appendEvent(&stream.XRPCStreamEvent{RepoAccount: evt}, evt.Seq)
			return nil
		},
	}
	return rsc
}

func (c *Consumer) appendEvent(evt *stream.XRPCStreamEvent, seq int64) {
	c.eventsLk.Lock()
	defer c.eventsLk.Unlock()
	c.Events = append(c.Events, evt)
	c.LastSeq = seq
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *Consumer) Connect(ctx context.Context, cursor int) error {

	u := c.Host + "/xrpc/com.atproto.sync.subscribeRepos"
	if cursor >= 0 {
		u = u + fmt.Sprintf("?cursor=%d", cursor)
	}

	dialer := websocket.Dialer{}
	conn, _, err := dialer.DialContext(ctx, u, nil)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan struct{})

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	seqScheduler := sequential.NewScheduler("test", c.eventCallbacks().EventHandler)
	go func() {
		err := stream.HandleRepoStream(ctx, conn, seqScheduler, nil)
		c.eventsLk.Lock()
		c.streamErr = err
		c.eventsLk.Unlock()
		cancel()
		close(c.done)
	}()
	return nil
}

func (c *Consumer) Count() int {
	c.eventsLk.Lock()
	defer c.eventsLk.Unlock()
	return len(c.Events)
}

func (c *Consumer) Clear() {
	c.eventsLk.Lock()
	defer c.eventsLk.Unlock()
	c.Events = []*stream.XRPCStreamEvent{}
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *Consumer) Shutdown() {
	if c.cancel != nil {
		c.cancel()
	}
}

// ConsumeEvents waits for count events after the last Clear and returns a stable snapshot.
func (c *Consumer) ConsumeEvents(count int) ([]*stream.XRPCStreamEvent, error) {
	if count < 0 {
		return nil, fmt.Errorf("negative event count: %d", count)
	}
	timer := time.NewTimer(c.Timeout)
	defer timer.Stop()
	for {
		c.eventsLk.Lock()
		seen := len(c.Events)
		lastSeq := c.LastSeq
		if seen >= count {
			events := append([]*stream.XRPCStreamEvent(nil), c.Events...)
			c.eventsLk.Unlock()
			return events, nil
		}
		changed := c.changed
		done := c.done
		streamErr := c.streamErr
		c.eventsLk.Unlock()

		if streamErr != nil {
			return nil, streamClosedError(seen, count, lastSeq, streamErr)
		}
		select {
		case <-changed:
		case <-done:
			c.eventsLk.Lock()
			streamErr = c.streamErr
			seen = len(c.Events)
			lastSeq = c.LastSeq
			if seen >= count {
				events := append([]*stream.XRPCStreamEvent(nil), c.Events...)
				c.eventsLk.Unlock()
				return events, nil
			}
			c.eventsLk.Unlock()
			return nil, streamClosedError(seen, count, lastSeq, streamErr)
		case <-timer.C:
			c.eventsLk.Lock()
			seen = len(c.Events)
			lastSeq = c.LastSeq
			if seen >= count {
				events := append([]*stream.XRPCStreamEvent(nil), c.Events...)
				c.eventsLk.Unlock()
				return events, nil
			}
			c.eventsLk.Unlock()
			return nil, fmt.Errorf("test stream consumer timeout after %s waiting for %d events (received %d, last seq %d)", c.Timeout, count, seen, lastSeq)
		}
	}
}

func streamClosedError(seen, count int, lastSeq int64, err error) error {
	if err == nil {
		return fmt.Errorf("test stream closed after %d/%d events (last seq %d)", seen, count, lastSeq)
	}
	return fmt.Errorf("test stream closed after %d/%d events (last seq %d): %w", seen, count, lastSeq, err)
}
