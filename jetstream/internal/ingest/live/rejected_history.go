package live

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/jcalabro/atmos/streaming"
)

type rejectedSourceOutcome struct {
	Version uint8  `json:"version"`
	Reason  string `json:"reason"`
	Cursor  int64  `json:"cursor"`
}

func (c *Consumer) persistSkippedSource(ctx context.Context, evt streaming.Event, reason string, observedAt int64) error {
	if evt.Seq <= 0 {
		return nil
	}
	if c.cfg.HistoryCapture == nil || c.cfg.History == nil || c.cfg.HistoryCapture.Journal != c.cfg.History || c.cfg.PolicyGate == nil || c.cfg.Attribution == nil {
		return fmt.Errorf("livestream: rejected source outcome requires bound history and admission")
	}
	err := c.cfg.PolicyGate.Admit(ctx, func(policy selection.Policy) error {
		if policy.Revision == 0 {
			return fmt.Errorf("livestream: missing policy history: %w", attribution.ErrInvalid)
		}
		fullPolicy, err := c.cfg.Attribution.LookupPolicy(policy.Revision)
		if err != nil {
			return err
		}
		metadata, err := json.Marshal(rejectedSourceOutcome{Version: 1, Reason: reason, Cursor: evt.Seq})
		if err != nil {
			return err
		}
		hash := sha256.Sum256(metadata)
		_, err = c.cfg.History.Append(history.Append{
			Source: history.Source{Domain: c.cfg.HistoryCapture.Domain, Epoch: c.cfg.HistoryCapture.Epoch, Cursor: uint64(evt.Seq), Path: "$rejected/" + reason, ContentHash: hex.EncodeToString(hash[:])},
			Kind:   "source-gap", Time: observedAt, Original: metadata, Policy: history.Policy{Revision: policy.Revision, Collections: append([]string(nil), policy.Collections...)},
			Provenance: string(attribution.ProvenanceLive), Disposition: history.Noop,
			Attribution: history.Attribution{StreamID: c.cfg.AttributionStreamID, Policy: history.EffectivePolicy{Revision: fullPolicy.Revision, Collections: append([]string(nil), fullPolicy.Collections...)}, Provenance: string(attribution.ProvenanceLive)},
		})
		return err
	})
	if err != nil {
		c.failHistoryEvent(evt.Seq)
		return fmt.Errorf("livestream: persist rejected source outcome: %w", err)
	}
	return c.finishHistoryEvent(ctx, evt.Seq)
}

func (c *Consumer) completeNoopHistoryEvent(ctx context.Context, evt streaming.Event, observedAt int64) error {
	if evt.Seq <= 0 {
		return nil
	}
	if c.cfg.HistoryCapture == nil || c.cfg.History == nil || c.cfg.HistoryCapture.Journal != c.cfg.History || c.cfg.PolicyGate == nil || c.cfg.Attribution == nil {
		return fmt.Errorf("livestream: info completion requires bound history and admission")
	}
	err := c.cfg.PolicyGate.Admit(ctx, func(policy selection.Policy) error {
		if policy.Revision == 0 {
			return fmt.Errorf("livestream: missing policy history: %w", attribution.ErrInvalid)
		}
		fullPolicy, err := c.cfg.Attribution.LookupPolicy(policy.Revision)
		if err != nil {
			return err
		}
		metadata, err := json.Marshal(rejectedSourceOutcome{Version: 1, Reason: "informational", Cursor: evt.Seq})
		if err != nil {
			return err
		}
		hash := sha256.Sum256(metadata)
		_, err = c.cfg.History.Append(history.Append{Source: history.Source{Domain: c.cfg.HistoryCapture.Domain, Epoch: c.cfg.HistoryCapture.Epoch, Cursor: uint64(evt.Seq), Path: "$info", ContentHash: hex.EncodeToString(hash[:])}, Kind: "source-info", Time: observedAt, Original: metadata, Policy: history.Policy{Revision: policy.Revision, Collections: append([]string(nil), policy.Collections...)}, Provenance: string(attribution.ProvenanceLive), Disposition: history.Noop, Attribution: history.Attribution{StreamID: c.cfg.AttributionStreamID, Policy: history.EffectivePolicy{Revision: fullPolicy.Revision, Collections: append([]string(nil), fullPolicy.Collections...)}, Provenance: string(attribution.ProvenanceLive)}})
		return err
	})
	if err != nil {
		c.failHistoryEvent(evt.Seq)
		return fmt.Errorf("livestream: persist informational source outcome: %w", err)
	}
	return c.finishHistoryEvent(ctx, evt.Seq)
}

func (c *Consumer) finishHistoryEvent(ctx context.Context, seq int64) error {
	if err := c.writer.DrainDurability(ctx); err != nil {
		c.failHistoryEvent(seq)
		return fmt.Errorf("livestream: checkpoint source outcome: %w", err)
	}
	c.completeHistoryEvent(seq)
	if err := c.writer.DrainDurability(ctx); err != nil {
		c.failHistoryEvent(seq)
		return fmt.Errorf("livestream: checkpoint source completion: %w", err)
	}
	return nil
}
