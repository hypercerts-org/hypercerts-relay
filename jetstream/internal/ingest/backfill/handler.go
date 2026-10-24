// handler.go provides SegmentHandler, the atmos
// backfill.Handler that walks each downloaded repo's MST and emits
// one segment.KindCreate event per record into the shared
// ingest.Writer.

package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SegmentHandler walks each downloaded repo's MST and emits its records into
// the shared writer. atmos guarantees no two HandleRepo calls overlap for the
// same DID; ingest.Writer is safe across DIDs.
type SegmentHandler struct {
	writer        *ingest.Writer
	logger        *slog.Logger
	now           func() time.Time
	metrics       *Metrics
	dropMetrics   *ingest.DropMetrics
	onWriterError func(error)
	completions   *completionBatcher
	policyGate    *policygate.Gate
	attribution   *attribution.Manager
	streamID      string
}

const segmentHandlerAppendBatchSize = 1024

var ErrManagedPolicyConfig = errors.New("backfill: managed policy configuration requires policy gate and stream ID")

var _ atmosbackfill.Handler = (*SegmentHandler)(nil)

func NewSegmentHandler(writer *ingest.Writer, logger *slog.Logger, m *Metrics) *SegmentHandler {
	if writer == nil {
		panic("backfill: NewSegmentHandler: writer is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SegmentHandler{writer: writer, logger: logger.With(slog.String("component", "backfill/handler")), now: time.Now, metrics: m}
}

func (h *SegmentHandler) SetDropMetrics(m *ingest.DropMetrics) { h.dropMetrics = m }

func (h *SegmentHandler) SetCompletionBatcher(b *completionBatcher) { h.completions = b }

// HandleRepo snapshots managed policy once, then keeps its admission lease for
// the complete projection and every attributed append batch.
func (h *SegmentHandler) HandleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
	if h.attribution == nil {
		return h.handleRepo(ctx, did, r, commit, segment.KindCreate, false, nil)
	}
	if h.policyGate == nil || h.streamID == "" {
		return ErrManagedPolicyConfig
	}
	return h.policyGate.Admit(ctx, func(policy selection.Policy) error {
		if policy.Revision == 0 {
			return errors.New("backfill: managed policy history unavailable")
		}
		fullPolicy, err := h.attribution.LookupPolicy(policy.Revision)
		if err != nil {
			return fmt.Errorf("backfill: lookup policy history: %w", err)
		}
		boundary := h.writer.NextSeq()
		id := string(did) + "/" + commit.Rev
		metadata := &ingest.AttributionMetadata{StreamID: h.streamID, Policy: fullPolicy, Provenance: attribution.ProvenanceSnapshot, Boundary: attribution.Boundary{Seq: &boundary, SnapshotID: &id}}
		return h.handleRepo(ctx, did, r, commit, segment.KindCreate, false, metadata)
	})
}

// HandleRepoResync emits a whole-repo replacement stream for a previously
// failed repo retry. Managed resync remains under policy admission and uses the
// captured metadata for every durable append.
func (h *SegmentHandler) HandleRepoResync(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
	if h.attribution != nil {
		return h.handleManagedResync(ctx, did, r, commit)
	}
	if h.policyGate == nil {
		return h.handleRepoResyncCore(ctx, did, r, commit, nil, false, nil)
	}
	return h.policyGate.Admit(ctx, func(policy selection.Policy) error {
		return h.handleRepoResyncAdmitted(policygate.WithAdmission(ctx, policy), did, r, commit)
	})
}

func (h *SegmentHandler) handleManagedResync(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
	if h.policyGate == nil || h.streamID == "" {
		return ErrManagedPolicyConfig
	}
	return h.policyGate.Admit(ctx, func(policy selection.Policy) error {
		if policy.Revision == 0 {
			return errors.New("backfill: managed policy history unavailable")
		}
		fullPolicy, err := h.attribution.LookupPolicy(policy.Revision)
		if err != nil {
			return fmt.Errorf("backfill: lookup policy history: %w", err)
		}
		boundary := h.writer.NextSeq()
		id := string(did) + "/" + commit.Rev
		metadata := ingest.AttributionMetadata{StreamID: h.streamID, Policy: fullPolicy, Provenance: attribution.ProvenanceReconciliation, Boundary: attribution.Boundary{Seq: &boundary, SnapshotID: &id}}
		return h.handleRepoResyncAdmitted(policygate.WithAdmission(ctx, policy), did, r, commit, metadata)
	})
}

func (h *SegmentHandler) handleRepoResyncAdmitted(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, metadata ...ingest.AttributionMetadata) error {
	policy, ok := policygate.Admission(ctx)
	if !ok {
		return errors.New("backfill: scoped retry missing policy admission")
	}
	if h.attribution != nil && len(metadata) == 0 {
		return ErrManagedPolicyConfig
	}
	if len(metadata) > 0 {
		selectedRows, err := h.collectSelectedRows(ctx, did, r, commit, policy.Collections, segment.KindCreateResync)
		if err != nil {
			return err
		}
		_, err = h.writer.ReconcileSyncWithMetadata(ctx, string(did), commit.Rev, selectedRows, policy.Collections, metadata[0])
		return err
	}
	return h.handleRepoResyncCore(ctx, did, r, commit, policy.Collections, true, nil)
}

func (h *SegmentHandler) collectSelectedRows(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, collections []string, kind segment.Kind) ([]segment.Event, error) {
	var rows []segment.Event
	err := r.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection, rkey, reason := splitRecordPath(key)
		if reason != "" || !containsCollection(collections, collection) {
			return nil
		}
		payload, err := r.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		rows = append(rows, segment.Event{DID: string(did), Rev: commit.Rev, Kind: kind, Collection: collection, Rkey: rkey, Payload: payload, WitnessedAt: h.now().UnixMicro()})
		return nil
	})
	return rows, err
}

func containsCollection(collections []string, collection string) bool {
	for _, candidate := range collections {
		if candidate == collection {
			return true
		}
	}
	return false
}

func (h *SegmentHandler) handleRepoResyncCore(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, admittedCollections []string, managed bool, metadata *ingest.AttributionMetadata) error {
	needed := managed
	var err error
	if !managed {
		needed, err = h.writer.NeedsScopedSync(string(did))
	}
	if err != nil {
		return err
	}
	if needed {
		collections := admittedCollections
		if managed && collections == nil {
			collections = []string{}
		}
		rows, err := h.collectSelectedRows(ctx, did, r, commit, collections, segment.KindCreateResync)
		if err != nil {
			return err
		}
		if managed {
			metadataCopy := metadata
			if metadataCopy == nil {
				return ErrManagedPolicyConfig
			}
			_, err = h.writer.ReconcileSyncWithMetadata(ctx, string(did), commit.Rev, rows, collections, *metadataCopy)
		} else {
			_, err = h.writer.ReconcileSync(ctx, string(did), commit.Rev, rows)
		}
		if err != nil {
			h.abortOnWriterError(err)
		}
		return err
	}
	return h.handleRepo(ctx, did, r, commit, segment.KindCreateResync, true, metadata)
}

func (h *SegmentHandler) handleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, materializedKind segment.Kind, prependSync bool, metadata *ingest.AttributionMetadata) error {
	return obs.Span(ctx, func(ctx context.Context) (retErr error) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("did", string(did)))
		start := time.Now()
		defer func() { h.metrics.observeHandleRepo(start, retErr) }()
		if _, err := atmos.ParseTID(commit.Rev); err != nil {
			h.dropMetrics.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonInvalidRev)
			return fmt.Errorf("backfill: did=%s: commit rev is not a valid TID: %w", did, err)
		}
		now := h.now().UTC()
		witnessedAt := now.UnixMicro()
		appended := false
		lastSeq := uint64(0)
		batch := make([]segment.Event, 0, segmentHandlerAppendBatchSize)
		if prependSync {
			ev, err := syncTombstoneEvent(did, commit, witnessedAt, now)
			if err != nil {
				return err
			}
			batch = append(batch, ev)
			if err := h.validateRepoMaterializations(ctx, did, r, commit, materializedKind, witnessedAt); err != nil {
				return err
			}
		}
		flushBatch := func() error {
			if len(batch) == 0 {
				return nil
			}
			var err error
			if metadata != nil {
				err = h.writer.AppendBatchAttributed(ctx, batch, *metadata)
			} else {
				err = h.writer.AppendBatch(ctx, batch)
			}
			if err != nil {
				err = fmt.Errorf("backfill: did=%s append batch: %w", did, err)
				h.abortOnWriterError(err)
				return err
			}
			for _, ev := range batch {
				if ev.Seq > 0 {
					lastSeq = ev.Seq
					appended = true
				}
			}
			batch = batch[:0]
			return nil
		}
		if err := r.Tree.Walk(func(key string, cid cbor.CID) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			collection, rkey, ok := h.validRecordPath(ctx, did, key)
			if !ok || metadata != nil && !containsCollection(metadata.Policy.Collections, collection) {
				return nil
			}
			payload, err := r.Store.GetBlock(cid)
			if err != nil {
				return fmt.Errorf("backfill: did=%s get block %s/%s: %w", did, collection, rkey, err)
			}
			ev := segment.Event{WitnessedAt: witnessedAt, Kind: materializedKind, DID: string(did), Collection: collection, Rkey: rkey, Rev: commit.Rev, Payload: payload}
			if err := segment.ValidateEvent(ev); err != nil {
				if errors.Is(err, segment.ErrFieldTooLong) {
					h.dropMetrics.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonFieldTooLong)
					h.logger.WarnContext(ctx, "dropped unarchivable upstream record", "did", string(did), "did_len", len(string(did)), "collection_len", len(collection), "rkey_len", len(rkey), "rev_len", len(commit.Rev), "payload_len", len(payload), "err", err)
					return nil
				}
				return fmt.Errorf("backfill: did=%s invalid segment event %s/%s: %w", did, collection, rkey, err)
			}
			batch = append(batch, ev)
			if len(batch) >= segmentHandlerAppendBatchSize {
				return flushBatch()
			}
			return nil
		}); err != nil {
			return err
		}
		if err := flushBatch(); err != nil {
			return err
		}
		if h.completions != nil {
			h.completions.RecordWatermark(did, lastSeq, appended)
		}
		return nil
	})
}

func (h *SegmentHandler) validRecordPath(ctx context.Context, did atmos.DID, key string) (collection, rkey string, ok bool) {
	collection, rkey, reason := splitRecordPath(key)
	if reason == "" {
		return collection, rkey, true
	}
	h.dropMetrics.IncDropped(ingest.DropSourceBackfill, reason)
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent("dropped_invalid_record", attributeReasonDID(reason, did))
	}
	return "", "", false
}

func splitRecordPath(key string) (collection, rkey string, reason ingest.DropReason) {
	collection, rkey, found := strings.Cut(key, "/")
	if !found {
		return "", "", ingest.DropReasonInvalidCollection
	}
	if _, err := atmos.ParseNSID(collection); err != nil {
		return collection, rkey, ingest.DropReasonInvalidCollection
	}
	if _, err := atmos.ParseRecordKey(rkey); err != nil {
		return collection, rkey, ingest.DropReasonInvalidRkey
	}
	return collection, rkey, ""
}

func attributeReasonDID(reason ingest.DropReason, did atmos.DID) trace.SpanStartEventOption {
	return trace.WithAttributes(attribute.String("reason", string(reason)), attribute.String("did", string(did)))
}

func (h *SegmentHandler) validateRepoMaterializations(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, materializedKind segment.Kind, witnessedAt int64) error {
	return r.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection, rkey, reason := splitRecordPath(key)
		if reason != "" {
			return nil
		}
		payload, err := r.Store.GetBlock(cid)
		if err != nil {
			return fmt.Errorf("backfill: did=%s get block %s/%s: %w", did, collection, rkey, err)
		}
		ev := segment.Event{WitnessedAt: witnessedAt, Kind: materializedKind, DID: string(did), Collection: collection, Rkey: rkey, Rev: commit.Rev, Payload: payload}
		if err := segment.ValidateEvent(ev); err != nil && !errors.Is(err, segment.ErrFieldTooLong) {
			return fmt.Errorf("backfill: did=%s invalid segment event %s/%s: %w", did, collection, rkey, err)
		}
		return nil
	})
}

func syncTombstoneEvent(did atmos.DID, commit *repo.Commit, witnessedAt int64, now time.Time) (segment.Event, error) {
	payload, err := (&comatproto.SyncSubscribeRepos_Sync{DID: string(did), Rev: commit.Rev, Time: now.Format(time.RFC3339Nano)}).MarshalCBOR()
	if err != nil {
		return segment.Event{}, fmt.Errorf("backfill: did=%s marshal synthetic sync: %w", did, err)
	}
	ev := segment.Event{WitnessedAt: witnessedAt, Kind: segment.KindSync, DID: string(did), Rev: commit.Rev, Payload: payload}
	if err := segment.ValidateEvent(ev); err != nil {
		return segment.Event{}, fmt.Errorf("backfill: did=%s invalid synthetic sync: %w", did, err)
	}
	return ev, nil
}

func (h *SegmentHandler) abortOnWriterError(err error) {
	if h.onWriterError != nil {
		h.onWriterError(err)
	}
}
