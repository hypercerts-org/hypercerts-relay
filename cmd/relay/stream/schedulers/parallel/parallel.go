package parallel

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers"

	"github.com/prometheus/client_golang/prometheus"
)

var ErrSchedulerStopped = errors.New("parallel scheduler stopped")

// Scheduler is a parallel scheduler that will run work on a fixed number of workers
type Scheduler struct {
	maxConcurrency int
	maxQueue       int

	do func(context.Context, *stream.XRPCStreamEvent) error

	feeder chan *consumerTask
	done   chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	stopOnce sync.Once
	workers  sync.WaitGroup

	lk      sync.Mutex
	active  map[string][]*consumerTask
	stopped bool
	err     error

	ident string

	// hypercerts: Acknowledge only contiguous successful source positions.
	ackQueue  []*consumerTask
	completed map[*consumerTask]bool
	lastSeq   atomic.Int64

	// metrics
	itemsAdded     prometheus.Counter
	itemsProcessed prometheus.Counter
	itemsActive    prometheus.Counter
	workersActive  prometheus.Gauge

	log *slog.Logger
}

func NewScheduler(maxC, maxQ int, ident string, do func(context.Context, *stream.XRPCStreamEvent) error) *Scheduler {
	if maxC < 1 {
		maxC = 1
	}
	if maxQ < 0 {
		maxQ = 0
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Scheduler{
		maxConcurrency: maxC,
		maxQueue:       maxQ,

		do: do,

		feeder:    make(chan *consumerTask, maxQ),
		done:      make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
		active:    make(map[string][]*consumerTask),
		completed: make(map[*consumerTask]bool),

		ident: ident,

		itemsAdded:     schedulers.WorkItemsAdded.WithLabelValues(ident, "parallel"),
		itemsProcessed: schedulers.WorkItemsProcessed.WithLabelValues(ident, "parallel"),
		itemsActive:    schedulers.WorkItemsActive.WithLabelValues(ident, "parallel"),
		workersActive:  schedulers.WorkersActive.WithLabelValues(ident, "parallel"),

		log: slog.Default().With("system", "parallel-scheduler"),
	}

	p.workers.Add(maxC)
	for range maxC {
		go p.worker()
	}

	p.workersActive.Set(float64(maxC))

	return p
}

func (p *Scheduler) Shutdown() {
	p.log.Info("shutting down parallel scheduler", "ident", p.ident)

	p.stop(nil)
	p.workers.Wait()
	p.workersActive.Set(0)

	p.log.Info("parallel scheduler shutdown complete", "ident", p.ident)
}

func (p *Scheduler) Done() <-chan struct{} {
	return p.done
}

func (p *Scheduler) Err() error {
	p.lk.Lock()
	defer p.lk.Unlock()
	return p.err
}

func (p *Scheduler) stop(err error) {
	p.stopOnce.Do(func() {
		p.lk.Lock()
		p.stopped = true
		p.err = err
		p.active = make(map[string][]*consumerTask)
		p.ackQueue = nil
		p.completed = make(map[*consumerTask]bool)
		p.lk.Unlock()

		p.cancel()
		close(p.done)
	})
}

type consumerTask struct {
	repo string
	val  *stream.XRPCStreamEvent
}

func (p *Scheduler) AddWork(ctx context.Context, repo string, val *stream.XRPCStreamEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	t := &consumerTask{
		repo: repo,
		val:  val,
	}
	p.lk.Lock()
	if p.err != nil {
		err := p.err
		p.lk.Unlock()
		return err
	}
	if p.stopped {
		p.lk.Unlock()
		return ErrSchedulerStopped
	}

	// hypercerts: Queued events also block acknowledgment of later source positions.
	if val.Sequence() > 0 {
		p.ackQueue = append(p.ackQueue, t)
	}
	a, ok := p.active[repo]
	if ok {
		p.active[repo] = append(a, t)
		p.lk.Unlock()
		p.itemsAdded.Inc()
		return nil
	}

	p.active[repo] = []*consumerTask{t}
	p.lk.Unlock()

	select {
	case p.feeder <- t:
		p.itemsAdded.Inc()
		return nil
	case <-ctx.Done():
		p.stop(ctx.Err())
		return ctx.Err()
	case <-p.ctx.Done():
		err := p.Err()
		if err != nil {
			return err
		}
		return ErrSchedulerStopped
	}
}

func (p *Scheduler) worker() {
	defer p.workers.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case work := <-p.feeder:
			for work != nil {
				if p.ctx.Err() != nil {
					return
				}

				p.itemsActive.Inc()
				if err := p.do(p.ctx, work.val); err != nil {
					p.log.Error("event handler failed", "ident", p.ident, "err", err)
					p.stop(err)
					return
				}
				p.itemsProcessed.Inc()

				work = p.completeAndNext(work)
			}
		}
	}
}

func (p *Scheduler) completeAndNext(work *consumerTask) *consumerTask {
	p.lk.Lock()
	defer p.lk.Unlock()

	if p.stopped {
		return nil
	}

	if work.val.Sequence() > 0 {
		p.completed[work] = true
		for len(p.ackQueue) > 0 && p.completed[p.ackQueue[0]] {
			head := p.ackQueue[0]
			p.lastSeq.Store(head.val.Sequence())
			delete(p.completed, head)
			p.ackQueue = p.ackQueue[1:]
		}
	}

	queued := p.active[work.repo]
	if len(queued) == 0 || queued[0] != work {
		delete(p.active, work.repo)
		return nil
	}

	queued = queued[1:]
	if len(queued) == 0 {
		delete(p.active, work.repo)
		return nil
	}

	next := queued[0]
	p.active[work.repo] = queued
	return next
}

func (p *Scheduler) LastSeq() int64 {
	return p.lastSeq.Load()
}
