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
	"errors"
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
	// recoveryResolveAttempts bounds how many flushes retry a DID whose identity
	// lookup failed. The resolver cannot tell a transient outage from a missing
	// identity, so the retry is bounded: about five minutes at the flush interval.
	recoveryResolveAttempts = 60
)

// recoveryCoalescer accumulates shed DIDs and books one recovery job per PDS.
//
// The drop callback runs on the ingestion path, so it only takes a lock and
// returns: no I/O, no blocking. All resolution and booking happens on the flush
// goroutine.
type recoveryCoalescer struct {
	jobs interface {
		RequestRepositoryRecovery(string, []string, string) (jobs.Job, error)
	}
	logger *slog.Logger
	// resolve returns the DID's current PDS. An empty endpoint with a nil error
	// is terminal (no PDS to attribute); an error is a lookup failure to retry.
	resolve  func(ctx context.Context, did string) (string, error)
	interval time.Duration

	mu      sync.Mutex
	pending map[string]struct{}
	// resolveFailures counts consecutive failed lookups for requeued DIDs.
	resolveFailures map[string]int
}

// observe records one shed DID. Safe for concurrent use and never blocks.
func (c *recoveryCoalescer) observe(did string, _ uint64) {
	if did == "" {
		return
	}
	c.mu.Lock()
	_, alreadyPending := c.pending[did]
	if !alreadyPending && len(c.pending) >= recoveryPendingLimit {
		c.mu.Unlock()
		c.logger.Warn("verify-queue loss recovery pending limit reached", "did", did)
		return
	}
	c.pending[did] = struct{}{}
	c.mu.Unlock()
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
		pds, err := c.resolve(ctx, did)
		if err != nil {
			c.retryResolve(did, err)
			continue
		}
		c.clearResolveFailures(did)
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
			if !errors.Is(err, jobs.ErrConflict) && !errors.Is(err, jobs.ErrInvalidInput) {
				for _, did := range dids {
					c.observe(did, 0)
				}
			}
			continue
		}
		c.logger.Info("booked repository recovery for verify-queue loss",
			"pds", pds, "dids", len(dids), "job", job.ID, "state", job.State)
	}
}

// retryResolve requeues a DID whose identity lookup failed so the recovery
// intent survives a transient identity outage, up to recoveryResolveAttempts.
func (c *recoveryCoalescer) retryResolve(did string, err error) {
	c.mu.Lock()
	if c.resolveFailures == nil {
		c.resolveFailures = map[string]int{}
	}
	c.resolveFailures[did]++
	attempts := c.resolveFailures[did]
	if attempts >= recoveryResolveAttempts {
		delete(c.resolveFailures, did)
		c.mu.Unlock()
		c.logger.Warn("verify-queue loss identity lookup kept failing; skipping recovery",
			"did", did, "attempts", attempts, "err", err)
		return
	}
	c.mu.Unlock()
	c.logger.Warn("verify-queue loss identity lookup failed; retrying",
		"did", did, "attempts", attempts, "err", err)
	c.observe(did, 0)
	c.mu.Lock()
	if _, requeued := c.pending[did]; !requeued {
		// The pending limit dropped it; observe already logged the drop.
		delete(c.resolveFailures, did)
	}
	c.mu.Unlock()
}

func (c *recoveryCoalescer) clearResolveFailures(did string) {
	c.mu.Lock()
	delete(c.resolveFailures, did)
	c.mu.Unlock()
}
