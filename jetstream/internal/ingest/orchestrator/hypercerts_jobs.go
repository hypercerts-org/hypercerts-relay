package orchestrator

import (
	"context"
	"errors"
	"github.com/bluesky-social/jetstream/internal/ingest"
)

// ReconcileSnapshot shares the archive rewrite lock with compaction/import and
// delegates live-write serialization to the single steady-state writer.
func (o *Orchestrator) ReconcileSnapshot(ctx context.Context, snapshot ingest.Snapshot) error {
	return o.withRewriteLock(func() error {
		writer := o.steadyWriter.Load()
		if writer == nil {
			return errors.New("backfill requires steady state")
		}
		return writer.ReconcileSnapshot(ctx, snapshot)
	})
}
