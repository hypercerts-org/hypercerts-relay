package orchestrator

import (
	"context"
	"errors"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
)

// ReconcileSnapshot shares the archive rewrite lock with compaction/import and
// delegates live-write serialization to the single steady-state writer.
func (o *Orchestrator) ReconcileSnapshot(ctx context.Context, snapshot ingest.Snapshot) error {
	writer := o.steadyWriter.Load()
	if writer == nil {
		return errors.New("backfill requires steady state")
	}
	if o.cfg.Attribution == nil {
		return o.withRewriteLock(func() error { return writer.ReconcileSnapshot(ctx, snapshot) })
	}
	if policy, admitted := policygate.Admission(ctx); admitted {
		return o.withRewriteLock(func() error {
			return o.reconcileAdmitted(ctx, writer, snapshot, policy)
		})
	}
	gate := o.currentPolicyGate()
	if gate == nil {
		return errors.New("managed snapshot policy gate unavailable")
	}
	return gate.Admit(ctx, func(policy selection.Policy) error {
		admittedCtx := policygate.WithAdmission(ctx, policy)
		return o.withRewriteLock(func() error {
			return o.reconcileAdmitted(admittedCtx, writer, snapshot, policy)
		})
	})
}

func (o *Orchestrator) reconcileAdmitted(ctx context.Context, writer *ingest.Writer, snapshot ingest.Snapshot, policy selection.Policy) error {
	if policy.Revision == 0 {
		return errors.New("managed snapshot policy history unavailable")
	}
	fullPolicy, err := o.cfg.Attribution.LookupPolicy(policy.Revision)
	if err != nil {
		return err
	}
	snapshot.AppendMetadata = &ingest.AttributionMetadata{
		StreamID:   o.cfg.AttributionStreamID,
		Policy:     fullPolicy,
		Provenance: attribution.ProvenanceReconciliation,
	}
	return writer.ReconcileSnapshot(ctx, snapshot)
}
