package live

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/streaming"
)

type OriginalCapture struct {
	Journal *history.Store
	Domain  string
	Epoch   string
	Policy  func() selection.Policy
}

type SourceEvent struct {
	Cursor uint64
	Raw    []byte
	Rows   []OriginalOperation
}
type OriginalOperation struct {
	Ordinal                               uint32
	Path, DID, Collection, Revision, Kind string
	ObservedAt                            int64
	Payload                               []byte
}

func (c OriginalCapture) CaptureOriginal(src SourceEvent) ([]history.Receipt, error) {
	if c.Journal == nil || c.Policy == nil {
		return nil, fmt.Errorf("live history capture: journal and policy required")
	}
	policy := c.Policy()
	out := make([]history.Receipt, 0, len(src.Rows))
	for _, row := range src.Rows {
		if row.Collection != "" && !slices.Contains(policy.Collections, row.Collection) {
			continue
		}
		canonical, err := json.Marshal(struct {
			Version        int    `json:"version"`
			Ordinal        uint32 `json:"ordinal"`
			Path           string `json:"path"`
			DID            string `json:"did"`
			Collection     string `json:"collection"`
			Revision       string `json:"revision"`
			Kind           string `json:"kind"`
			Operation      []byte `json:"operation"`
			Payload        []byte `json:"payload"`
			SourceEvidence []byte `json:"sourceEvidence"`
		}{1, row.Ordinal, row.Path, row.DID, row.Collection, row.Revision, row.Kind, row.Payload, row.Payload, src.Raw})
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(canonical)
		receipt, err := c.Journal.Append(history.Append{Source: history.Source{Domain: c.Domain, Epoch: c.Epoch, Cursor: src.Cursor, Operation: row.Ordinal, Path: row.Path, ContentHash: hex.EncodeToString(hash[:])}, Kind: row.Kind, DID: row.DID, Collection: row.Collection, Revision: row.Revision, Time: row.ObservedAt, Original: canonical, InputEvent: segment.Event{Kind: eventKind(row.Kind), WitnessedAt: row.ObservedAt, DID: row.DID, Collection: row.Collection, Rkey: pathRkey(row.Path), Rev: row.Revision, Payload: append([]byte(nil), row.Payload...)}, Policy: history.Policy{Revision: policy.Revision, Collections: append([]string(nil), policy.Collections...)}, Provenance: "upstream-original", Disposition: history.Apply, Attribution: history.Attribution{StreamID: c.Journal.StreamID(), Policy: history.EffectivePolicy{Revision: policy.Revision, Collections: append([]string(nil), policy.Collections...)}, Provenance: "upstream-original"}})
		if err != nil {
			return nil, err
		}
		out = append(out, receipt)
	}
	return out, nil
}

func (c *Consumer) processOriginalHistory(ctx context.Context, evt streaming.Event, witnessedAt int64, segEvts []segment.Event) (bool, error) {
	if c.cfg.HistoryCapture == nil {
		return false, nil
	}
	if evt.Info != nil {
		return true, nil
	}
	if evt.Commit == nil && evt.Sync == nil && evt.Identity == nil && evt.Account == nil {
		return true, nil
	}
	if evt.Resync == streaming.ResyncSyncEvent {
		return false, nil
	}
	if evt.Commit == nil && (evt.Identity != nil || evt.Account != nil) {
		if c.cfg.History == nil || c.cfg.HistoryCapture.Journal != c.cfg.History || c.cfg.HistoryCapture.Domain == "" || c.cfg.HistoryCapture.Epoch == "" || c.cfg.PolicyGate == nil || c.cfg.Attribution == nil {
			return false, fmt.Errorf("livestream: history capture requires bound history and admission")
		}
		if evt.Seq <= 0 || len(segEvts) != 1 {
			return false, fmt.Errorf("livestream: invalid lifecycle source event")
		}
		var handled bool
		err := c.cfg.PolicyGate.Admit(ctx, func(policy selection.Policy) error {
			if policy.Revision == 0 {
				return fmt.Errorf("livestream: missing policy history: %w", attribution.ErrInvalid)
			}
			kind := "identity"
			if evt.Account != nil {
				kind = "account"
			}
			canonical, err := json.Marshal(struct {
				Version  int    `json:"version"`
				Envelope []byte `json:"envelope"`
			}{1, segEvts[0].Payload})
			if err != nil {
				return err
			}
			hash := sha256.Sum256(canonical)
			fullPolicy, err := c.cfg.Attribution.LookupPolicy(policy.Revision)
			if err != nil {
				return err
			}
			receipt, err := c.writer.CaptureLifecycle(ctx, history.Source{Domain: c.cfg.HistoryCapture.Domain, Epoch: c.cfg.HistoryCapture.Epoch, Cursor: uint64(evt.Seq), Operation: 0, Path: "$" + kind, ContentHash: hex.EncodeToString(hash[:])}, segEvts[0], canonical, history.Policy{Revision: policy.Revision, Collections: append([]string(nil), policy.Collections...)}, string(attribution.ProvenanceLive), ingest.AttributionMetadata{StreamID: c.cfg.AttributionStreamID, Policy: fullPolicy, Provenance: attribution.ProvenanceLive})
			if err != nil {
				return err
			}
			if receipt.NewProjection && receipt.Disposition == history.Apply && receipt.SegmentSeq != 0 {
				segEvts[0].Seq = receipt.SegmentSeq
				if c.cfg.OnEvent != nil {
					c.cfg.OnEvent(&segEvts[0])
				}
			}
			handled = true
			return nil
		})
		if err != nil {
			return true, fmt.Errorf("livestream: capture original lifecycle event: %w", err)
		}
		if err := c.writer.DrainDurability(ctx); err != nil {
			c.failHistoryEvent(evt.Seq)
			return true, err
		}
		c.completeHistoryEvent(evt.Seq)
		c.noteUpstreamSeq(evt.Seq)
		if err := c.writer.DrainDurability(ctx); err != nil {
			c.failHistoryEvent(evt.Seq)
			return true, err
		}
		return handled, nil
	}
	if evt.Commit == nil && evt.Sync == nil {
		return false, fmt.Errorf("livestream: history capture does not yet support this source event")
	}
	if c.cfg.History == nil || c.cfg.HistoryCapture.Journal != c.cfg.History || c.cfg.HistoryCapture.Domain == "" || c.cfg.HistoryCapture.Epoch == "" || c.cfg.PolicyGate == nil || c.cfg.Attribution == nil || c.cfg.CollectionPolicy == nil {
		return false, fmt.Errorf("livestream: history capture requires bound history and admission")
	}
	syntheticResync := evt.Resync == streaming.ResyncAsync
	if evt.Seq <= 0 && !syntheticResync {
		return true, fmt.Errorf("livestream: invalid source cursor")
	}

	if syntheticResync && evt.Sync == nil {
		return true, fmt.Errorf("livestream: synthetic resync missing sync envelope")
	}
	var forward []segment.Event
	var selectedState []segment.Event
	err := c.cfg.PolicyGate.Admit(ctx, func(policy selection.Policy) error {
		if policy.Revision == 0 {
			return fmt.Errorf("livestream: missing policy history: %w", attribution.ErrInvalid)
		}
		var ops []SourceOperation
		var err error
		sourceEvent := evt
		if evt.Sync != nil {
			ops, err = ConvertSyncSourceEvent(sourceEvent, witnessedAt, policy.Collections)
		} else {
			ops, err = ConvertSourceEvent(sourceEvent, witnessedAt, policy.Collections)
		}
		if err != nil {
			if _, dropped := err.(*DroppedOpsError); !dropped {
				return err
			}
		}
		forward = make([]segment.Event, 0, len(ops))
		for _, op := range ops {
			if op.Event.Kind.IsMaterialization() {
				selectedState = append(selectedState, op.Event)
			}
			if syntheticResync {
				continue
			}
			receipt, err := c.captureOriginalEvent(ctx, evt, policy, op)
			if err != nil {
				return err
			}
			if receipt.Disposition == history.Apply && receipt.SegmentSeq != 0 {
				op.Event.Seq = receipt.SegmentSeq
				if receipt.SegmentSeq < c.writer.ReadLog().DurableSeq() {
					stored, err := c.cfg.Attribution.Lookup(receipt.SegmentSeq)
					if err != nil {
						return err
					}
					page, err := c.cfg.History.Read(receipt.JournalSeq-1, receipt.JournalSeq, 1)
					if err != nil || len(page.Events) != 1 {
						if err != nil {
							return err
						}
						return fmt.Errorf("livestream: saved history receipt missing")
					}
					op.Event = page.Events[0].InputEvent
					op.Event.Seq = stored.Seq
				}
				forward = append(forward, op.Event)
			}
		}
		if evt.Sync != nil {
			admittedCtx := policygate.WithAdmission(ctx, policy)
			if syntheticResync {
				if c.cfg.ReconcileSnapshot == nil {
					return fmt.Errorf("livestream: managed snapshot reconciler unavailable: %w", attribution.ErrInvalid)
				}
				fullPolicy, err := c.cfg.Attribution.LookupPolicy(policy.Revision)
				if err != nil {
					return err
				}
				rows := eventsForSnapshot(selectedState)
				for i := range rows {
					rows[i].Kind = segment.KindCreateResync
				}
				metadata := ingest.AttributionMetadata{StreamID: c.cfg.AttributionStreamID, Policy: fullPolicy, Provenance: attribution.ProvenanceSnapshot}
				snapshot := ingest.Snapshot{DID: evt.Sync.DID, Rev: evt.Sync.Rev, Collections: append([]string(nil), policy.Collections...), Records: rows, AppendMetadata: &metadata}
				if err := c.cfg.ReconcileSnapshot(admittedCtx, snapshot); err != nil {
					return err
				}
				return nil
			}
			if c.cfg.ReconcileSnapshot == nil {
				return fmt.Errorf("livestream: managed snapshot reconciler unavailable: %w", attribution.ErrInvalid)
			}
			fullPolicy, err := c.cfg.Attribution.LookupPolicy(policy.Revision)
			if err != nil {
				return err
			}
			metadata := ingest.AttributionMetadata{StreamID: c.cfg.AttributionStreamID, Policy: fullPolicy, Provenance: attribution.ProvenanceReconciliation}
			if _, err := c.writer.ReconcileSyncWithMetadata(admittedCtx, evt.Sync.DID, evt.Sync.Rev, eventsForSnapshot(selectedState), policy.Collections, metadata); err != nil {
				return err
			}
			marker := segEvts[0]
			stripped := *evt.Sync
			stripped.Blocks = nil
			markerPayload, err := stripped.MarshalCBOR()
			if err != nil {
				return err
			}
			marker.Payload = markerPayload
			original, err := json.Marshal(struct {
				Version  int    `json:"version"`
				Envelope []byte `json:"envelope"`
			}{1, markerPayload})
			if err != nil {
				return err
			}
			hash := sha256.Sum256(original)
			_, err = c.cfg.History.Append(history.Append{Source: history.Source{Domain: c.cfg.HistoryCapture.Domain, Epoch: c.cfg.HistoryCapture.Epoch, Cursor: uint64(evt.Seq), Operation: ^uint32(0), Path: "$sync", ContentHash: hex.EncodeToString(hash[:])}, Kind: "sync", DID: evt.Sync.DID, Revision: evt.Sync.Rev, Time: marker.WitnessedAt, Original: original, InputEvent: marker, Policy: history.Policy{Revision: policy.Revision, Collections: append([]string(nil), policy.Collections...)}, Provenance: string(attribution.ProvenanceLive), Disposition: history.Noop, Attribution: history.Attribution{StreamID: c.cfg.AttributionStreamID, Policy: history.EffectivePolicy{Revision: fullPolicy.Revision, Collections: append([]string(nil), fullPolicy.Collections...)}, Provenance: string(attribution.ProvenanceLive)}})
			return err
		}
		return nil
	})
	if err != nil {
		return true, fmt.Errorf("livestream: capture original history: %w", err)
	}
	if c.cfg.OnEvent != nil {
		for i := range forward {
			c.cfg.OnEvent(&forward[i])
		}
	}
	c.promoteSyncState(segEvts)
	if err := c.writer.DrainDurability(ctx); err != nil {
		return true, fmt.Errorf("livestream: checkpoint captured source event: %w", err)
	}
	if evt.Seq > 0 {
		c.completeHistoryEvent(evt.Seq)
		c.noteUpstreamSeq(evt.Seq)
	}
	if err := c.writer.DrainDurability(ctx); err != nil {
		return true, fmt.Errorf("livestream: checkpoint completed source event: %w", err)
	}
	return true, nil
}

func eventsForSnapshot(events []segment.Event) []segment.Event {
	out := make([]segment.Event, 0, len(events))
	for _, event := range events {
		if event.Kind != segment.KindSync {
			out = append(out, event)
		}
	}
	return out
}
func (c *Consumer) captureOriginalEvent(ctx context.Context, evt streaming.Event, policy selection.Policy, op SourceOperation) (ingest.CaptureProjectedReceipt, error) {
	fullPolicy, err := c.cfg.Attribution.LookupPolicy(policy.Revision)
	if err != nil {
		return ingest.CaptureProjectedReceipt{}, err
	}
	canonical, err := json.Marshal(struct {
		Version        int    `json:"version"`
		Ordinal        uint32 `json:"ordinal"`
		Operation      []byte `json:"operation"`
		Payload        []byte `json:"payload"`
		SourceEvidence []byte `json:"sourceEvidence"`
	}{1, op.Ordinal, op.CBOR, op.Event.Payload, op.CBOR})
	if err != nil {
		return ingest.CaptureProjectedReceipt{}, err
	}
	hash := sha256.Sum256(canonical)
	epoch := c.cfg.HistoryCapture.Epoch
	cursor := uint64(evt.Seq)
	provenance := attribution.ProvenanceLive
	if evt.Resync == streaming.ResyncAsync {
		epoch = "synthetic-resync/" + evt.Sync.DID + "/" + evt.Sync.Rev
		cursor = 1
		provenance = attribution.ProvenanceReconciliation
	}
	return c.writer.CaptureAndProject(ctx, history.Source{Domain: c.cfg.HistoryCapture.Domain, Epoch: epoch, Cursor: cursor, Operation: op.Ordinal, Path: op.Event.Collection + "/" + op.Event.Rkey, ContentHash: hex.EncodeToString(hash[:])}, op.Event, canonical, history.Policy{Revision: policy.Revision, Collections: append([]string(nil), policy.Collections...)}, string(provenance), ingest.AttributionMetadata{StreamID: c.cfg.AttributionStreamID, Policy: fullPolicy, Provenance: provenance})
}
func pathRkey(path string) string {
	_, rkey, found := strings.Cut(path, "/")
	if found {
		return rkey
	}
	return ""
}
func eventKind(kind string) segment.Kind {
	switch kind {
	case "create":
		return segment.KindCreate
	case "update":
		return segment.KindUpdate
	case "delete":
		return segment.KindDelete
	case "identity":
		return segment.KindIdentity
	case "account":
		return segment.KindAccount
	case "sync":
		return segment.KindSync
	}
	return 0
}
