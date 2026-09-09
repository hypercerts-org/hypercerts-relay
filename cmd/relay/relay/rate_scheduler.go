package relay

import (
	"context"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/parallel"
)

type rateScheduler struct {
	*parallel.Scheduler
	wait func(context.Context, string) error
	host string
}

func (s rateScheduler) AddWork(ctx context.Context, repo string, ev *stream.XRPCStreamEvent) error {
	if s.wait != nil {
		if err := s.wait(ctx, s.host); err != nil {
			return err
		}
	}
	return s.Scheduler.AddWork(ctx, repo, ev)
}
