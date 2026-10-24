package subscribe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
)

func runSelectedSubscriberLoop(ctx context.Context, conn *websocket.Conn, deps Subscription, filter *FilterV2, config SelectedConfig, start uint64, compress bool, logger *slog.Logger) {
	writer := deps.writer()
	if writer == nil || deps.SelectedAttribution == nil || filter == nil {
		return
	}
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	cursor, announced := start, uint64(0)
	announce := func(policy attribution.Policy) bool {
		if policy.Revision <= announced {
			return true
		}
		frame, err := EncodeV2SelectedInfo("PolicyAdopted", "Selected collection policy adopted", map[string]any{"contract": SelectedContract, "streamId": config.StreamID, "policyRevision": policy.Revision, "collections": policy.Collections, "effectiveAfterSeq": policy.EffectiveAfterSeq})
		if err != nil || !writeFrame(ctx, conn, compress, frame) {
			return false
		}
		announced = policy.Revision
		return true
	}
	for {
		readlog := writer.ReadLog()
		entries, notify, ok, atTip := readlog.ReadCommittedFrom(cursor, deps.Tail.ReadBatch())
		if !ok {
			if !atTip && cursor < readlog.FloorSeq() {
				batch, next, err := deps.Tail.ReadFrom(ctx, cursor, deps.Tail.ReadBatch())
				if err != nil {
					return
				}
				if len(batch) == 0 {
					// A committed cursor with no cold result indicates a gap or a
					// reader invariant failure. Never jump it silently.
					if next > cursor && next < readlog.DurableSeq() {
						cursor = next
						continue
					}
					return
				}
				last := cursor
				for _, entry := range batch {
					if entry.Event.Seq >= readlog.DurableSeq() {
						break
					}
					if err := processSelectedRow(ctx, conn, entry.Event, deps, filter, config, compress, logger, &announced); err != nil {
						return
					}
					last = entry.Event.Seq + 1
				}
				if last == cursor && next > cursor && next < readlog.DurableSeq() {
					cursor = next
				} else {
					cursor = last
				}
				continue
			}
			history, err := deps.SelectedAttribution.ReadPolicyHistory()
			lastDelivered := uint64(0)
			if cursor > 0 {
				lastDelivered = cursor - 1
			}
			if err != nil || !announceApplicablePolicies(history, lastDelivered, announced, announce) {
				logger.Error("selected policy history read failed", "err", err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ping.C:
				pingCtx, cancel := context.WithTimeout(ctx, frameWriteTimeout)
				err := conn.Ping(pingCtx)
				cancel()
				if err != nil {
					return
				}
			case <-notify:
			}
			continue
		}
		for _, row := range entries {
			ev := row.Event()
			if ev.Seq >= readlog.DurableSeq() {
				break
			}
			if err := processSelectedRow(ctx, conn, ev, deps, filter, config, compress, logger, &announced); err != nil {
				return
			}
			cursor = ev.Seq + 1
		}
	}
}

func announceApplicablePolicies(history []attribution.Policy, lastDeliveredSeq, announced uint64, announce func(attribution.Policy) bool) bool {
	for _, policy := range history {
		if policy.Revision <= announced {
			continue
		}
		if policy.EffectiveAfterSeq != nil && *policy.EffectiveAfterSeq > lastDeliveredSeq {
			continue
		}
		if !announce(policy) {
			return false
		}
		announced = policy.Revision
	}
	return true
}

func processSelectedRow(ctx context.Context, conn *websocket.Conn, ev *segment.Event, deps Subscription, filter *FilterV2, config SelectedConfig, compress bool, logger *slog.Logger, announced *uint64) error {
	if !filter.Wants(ev) {
		return nil
	}
	if deps.SelectedAttribution == nil {
		return fmt.Errorf("selected attribution unavailable")
	}
	_, err := deps.SelectedAttribution.Lookup(ev.Seq)
	if err != nil && !errors.Is(err, attribution.ErrLegacyUnknown) {
		return err
	}
	history, err := deps.SelectedAttribution.ReadPolicyHistory()
	if err != nil {
		return err
	}
	if ev.Seq > 0 && !announceApplicablePolicies(history, ev.Seq-1, *announced, func(policy attribution.Policy) bool {
		frame, err := EncodeV2SelectedInfo("PolicyAdopted", "Selected collection policy adopted", map[string]any{"contract": SelectedContract, "streamId": config.StreamID, "policyRevision": policy.Revision, "collections": policy.Collections, "effectiveAfterSeq": policy.EffectiveAfterSeq})
		if err != nil || !writeFrame(ctx, conn, compress, frame) {
			return false
		}
		*announced = policy.Revision
		return true
	}) {
		return fmt.Errorf("failed to announce governing policy")
	}
	return deliverSelected(ctx, conn, ev, deps, filter, compress, logger)
}

func deliverSelected(ctx context.Context, conn *websocket.Conn, ev *segment.Event, deps Subscription, filter *FilterV2, compress bool, logger *slog.Logger) error {
	if !filter.Wants(ev) {
		return nil
	}
	if deps.SelectedAttribution == nil {
		return fmt.Errorf("selected attribution unavailable")
	}
	meta, err := deps.SelectedAttribution.Lookup(ev.Seq)
	if err != nil && !errors.Is(err, attribution.ErrLegacyUnknown) {
		logger.Error("selected attribution lookup failed", "seq", ev.Seq, "err", err)
		return err
	}
	selectedMetadata := SelectedMetadata(meta)
	if ev.Kind == segment.KindSync {
		if meta.PolicyRev == nil {
			return fmt.Errorf("selected sync missing policy attribution")
		}
		policy, policyErr := deps.SelectedAttribution.LookupPolicy(*meta.PolicyRev)
		if policyErr != nil {
			logger.Error("selected sync policy lookup failed", "seq", ev.Seq, "err", policyErr)
			return policyErr
		}
		selectedMetadata = selectedSyncMetadata(meta, ev.DID, policy)
	}
	body, err := EncodeV2Selected(ev, selectedMetadata)
	if err != nil {
		logger.Error("selected event encoding failed", "seq", ev.Seq, "err", err)
		return err
	}
	msgType := websocket.MessageText
	if compress {
		body = compressFrameV2(body)
		msgType = websocket.MessageBinary
	}
	writeCtx, cancel := context.WithTimeout(ctx, frameWriteTimeout)
	defer cancel()
	return conn.Write(writeCtx, msgType, body)
}
