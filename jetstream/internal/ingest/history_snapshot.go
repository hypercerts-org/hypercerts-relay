package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

func containsCollection(collections []string, target string) bool {
	for _, collection := range collections {
		if collection == target {
			return true
		}
	}
	return false
}

type syntheticSnapshotPlan struct {
	DID         string                 `json:"did"`
	Rev         string                 `json:"rev"`
	Collections []string               `json:"collections"`
	Policy      attribution.Policy     `json:"policy"`
	Provenance  attribution.Provenance `json:"provenance"`
	Rows        []segment.Event        `json:"rows"`
	BoundarySeq uint64                 `json:"boundarySeq"`
}

func (w *Writer) reconcileSnapshotHistoryLocked(ctx context.Context, snapshot Snapshot, rows []segment.Event, metadata *AttributionMetadata) error {
	w.mu.Unlock()
	defer w.mu.Lock()
	w.drainMu.Unlock()
	defer w.drainMu.Lock()
	if err := w.cfg.Attribution.ValidateBinding(w.cfg.Store, w.cfg.AttributionDomain); err != nil {
		return err
	}
	if err := w.cfg.History.ValidateBinding(w.cfg.Store); err != nil {
		return err
	}
	if metadata.Provenance != attribution.ProvenanceSnapshot && metadata.Provenance != attribution.ProvenanceReconciliation {
		return fmt.Errorf("ingest: invalid synthetic snapshot provenance: %w", attribution.ErrInvalid)
	}
	if metadata.Policy.Revision == 0 || metadata.StreamID == "" {
		return fmt.Errorf("ingest: incomplete synthetic snapshot attribution: %w", attribution.ErrInvalid)
	}
	id := "snapshot/" + snapshot.DID + "/" + snapshot.Rev + "/policy/" + fmt.Sprint(metadata.Policy.Revision)
	if metadata.Boundary.SnapshotID != nil {
		id = *metadata.Boundary.SnapshotID
	}
	if len(metadata.Policy.Collections) == 0 {
		return fmt.Errorf("ingest: snapshot policy does not cover its collections: %w", attribution.ErrInvalid)
	}
	for _, collection := range snapshot.Collections {
		if !containsCollection(metadata.Policy.Collections, collection) {
			return fmt.Errorf("ingest: snapshot collection outside admitted policy: %w", attribution.ErrInvalid)
		}
	}
	identity := sha256.Sum256([]byte(id))
	key := []byte("selected-history/synthetic-plan/" + hex.EncodeToString(identity[:]))
	plan := syntheticSnapshotPlan{DID: snapshot.DID, Rev: snapshot.Rev, Collections: append([]string(nil), snapshot.Collections...), Policy: metadata.Policy, Provenance: metadata.Provenance, Rows: rows, BoundarySeq: w.nextSeq}
	if b, closer, err := w.cfg.Store.Get(key); err == nil {
		defer closer.Close()
		if err := json.Unmarshal(b, &plan); err != nil {
			return err
		}
		if plan.DID != snapshot.DID || plan.Rev != snapshot.Rev || !slices.Equal(plan.Collections, snapshot.Collections) {
			return fmt.Errorf("ingest: synthetic snapshot identity conflicts with immutable plan")
		}
		metadata.Policy = plan.Policy
		metadata.Provenance = plan.Provenance
		metadata.Boundary = attribution.Boundary{Seq: &plan.BoundarySeq, SnapshotID: &id}
	} else if err != pebble.ErrNotFound {
		return err
	} else {
		if metadata.Boundary.Seq != nil && *metadata.Boundary.Seq != w.nextSeq {
			return fmt.Errorf("ingest: snapshot boundary does not match current segment coordinate: %w", attribution.ErrInvalid)
		}
		if metadata.Boundary.JournalSequence != nil {
			return fmt.Errorf("ingest: caller cannot supply synthetic journal boundary: %w", attribution.ErrInvalid)
		}
		b, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		if err := w.cfg.Store.Set(key, b, pebble.Sync); err != nil {
			return err
		}
	}
	var boundarySeq uint64
	for i, row := range plan.Rows {
		canonical, err := json.Marshal(row)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(canonical)
		source := history.Source{Domain: w.cfg.AttributionDomain, Epoch: "synthetic/" + id, Cursor: 1, Operation: uint32(i + 1), Path: row.Collection + "/" + row.Rkey, ContentHash: hex.EncodeToString(hash[:])}
		rowMetadata := *metadata
		rowMetadata.Provenance = plan.Provenance
		rowMetadata.Boundary = attribution.Boundary{Seq: &plan.BoundarySeq}
		rowMetadata.Boundary.SnapshotID = &id
		policy := history.Policy{Revision: plan.Policy.Revision, Collections: append([]string(nil), plan.Policy.Collections...)}
		receipt, err := w.CaptureAndProject(ctx, source, row, canonical, policy, string(plan.Provenance), rowMetadata)
		if err != nil {
			return err
		}
		boundarySeq = receipt.JournalSeq
	}
	canonical, err := json.Marshal(struct {
		DID, Rev    string
		Collections []string
		Policy      attribution.Policy
		Provenance  attribution.Provenance
		Rows        []segment.Event
	}{plan.DID, plan.Rev, plan.Collections, plan.Policy, plan.Provenance, plan.Rows})
	if err != nil {
		return err
	}
	hash := sha256.Sum256(canonical)
	source := history.Source{Domain: w.cfg.AttributionDomain, Epoch: "synthetic/" + id, Cursor: 1, Operation: 0, Path: "snapshot-boundary", ContentHash: hex.EncodeToString(hash[:])}
	boundaryMetadata := *metadata
	boundaryMetadata.Boundary = attribution.Boundary{Seq: &plan.BoundarySeq}
	boundaryMetadata.Boundary.SnapshotID = &id
	boundary := segment.Event{Kind: segment.KindSync, DID: snapshot.DID, Rev: snapshot.Rev}
	policy := history.Policy{Revision: plan.Policy.Revision, Collections: append([]string(nil), plan.Policy.Collections...)}
	boundary.Kind = segment.KindSync
	boundaryMetadata.Provenance = attribution.ProvenanceReconciliation
	boundaryMetadata.Source = nil
	// Stored as a no-op disposition below so a segment sync tombstone is never emitted.
	receipt, err := w.CaptureAndProject(ctx, source, boundary, canonical, policy, string(boundaryMetadata.Provenance), boundaryMetadata)
	if err != nil {
		return err
	}
	boundarySeq = receipt.JournalSeq
	metadata.Boundary = attribution.Boundary{Seq: &plan.BoundarySeq, SnapshotID: &id, JournalSequence: &boundarySeq}
	metadata.Policy = plan.Policy
	if err := w.drainHistoryLocked(ctx); err != nil {
		return err
	}
	return w.persistSnapshotBoundary(snapshot)
}
