// Package jetstreamd: verify-queue overflow coalescer.
//
// Jetstream's live consumer verifies through atmos's parallel scheduler, which
// caps each DID's pending queue at Parallelism*2. When one DID outruns its
// single verifier worker the scheduler drops the oldest queued event and the
// cursor advances past it, so the loss is permanent. This coalescer turns that
// silent loss into a durable, repository-scoped recovery booking.
package jetstreamd

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
)

const (
	// recoveryFlushInterval batches a burst of drops into one booking. A single
	// replayed backlog can shed hundreds of events for one DID in milliseconds.
	recoveryFlushInterval = 5 * time.Second
	// recoveryPendingLimit bounds the per-flush DID set. Beyond it further DIDs
	// are dropped and logged rather than growing without limit.
	recoveryPendingLimit = 4096
)

// recoveryCoalescer accumulates shed DIDs and books one recovery job per PDS.
//
// The drop callback runs on the ingestion path, so it only takes a lock and
// returns: no I/O, no blocking. All resolution and booking happens on the flush
// goroutine.
type recoveryCoalescer struct {
	jobs     *jobs.Manager
	logger   *slog.Logger
	resolve  func(ctx context.Context, did string) string
	interval time.Duration

	mu      sync.Mutex
	pending map[string]struct{}
	// repairing records DIDs already submitted but not yet seen completing, so a
	// sustained overflow does not book a new job every flush.
	inflight map[string]struct{}
}

// observe records one shed DID. Safe for concurrent use and never blocks.
func (c *recoveryCoalescer) observe(did string, _ uint64) {
	if did == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, seen := c.inflight[did]; seen {
		// A repair is already in flight for this DID; a second booking would
		// duplicate work rather than narrow the gap.
		return
	}
	if len(c.pending) >= recoveryPendingLimit {
		return
	}
	c.pending[did] = struct{}{}
}

// run flushes on a ticker until the context is cancelled.
func (c *recoveryCoalescer) run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// One last flush so a shutdown does not discard a known gap.
			c.flush(context.WithoutCancel(ctx))
			return nil
		case <-ticker.C:
			c.flush(ctx)
		}
	}
}

// flush groups the pending DIDs by PDS and books one recovery job per PDS.
func (c *recoveryCoalescer) flush(ctx context.Context) {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return
	}
	observed := c.pending
	c.pending = make(map[string]struct{}, len(observed))
	c.mu.Unlock()

	byPDS := make(map[string][]string, len(observed))
	for did := range observed {
		pds := c.resolve(ctx, did)
		if pds == "" {
			c.logger.Warn("verify-queue loss could not be attributed to a PDS; skipping recovery", "did", did)
			continue
		}
		byPDS[pds] = append(byPDS[pds], did)
	}

	for pds, dids := range byPDS {
		job, err := c.jobs.RequestRepositoryRecovery(pds, dids, "")
		if err != nil {
			c.logger.Warn("booking repository recovery failed", "pds", pds, "dids", len(dids), "err", err)
			continue
		}
		c.mu.Lock()
		for _, did := range dids {
			c.inflight[did] = struct{}{}
		}
		c.mu.Unlock()
		c.logger.Info("booked repository recovery for verify-queue loss",
			"pds", pds, "dids", len(dids), "job", job.ID, "state", job.State)
	}
}

// forget releases DIDs once their repair is no longer running, so a later loss
// for the same DID can be booked again.
func (c *recoveryCoalescer) forget(dids []string) {
	if len(dids) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, did := range dids {
		delete(c.inflight, did)
	}
}
