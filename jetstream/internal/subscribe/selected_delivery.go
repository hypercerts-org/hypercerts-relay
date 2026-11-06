package subscribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
)

func runSelectedSubscriberLoop(ctx context.Context, conn *websocket.Conn, deps Subscription, filter *FilterV2, config SelectedConfig, start uint64, compress bool, logger *slog.Logger) {
	if deps.SelectedJournalMode && deps.SelectedHistory != nil && config.HistoryMode == "original-events" {
		runJournalSelectedSubscriberLoop(ctx, conn, deps, filter, config, start, compress, logger)
		return
	}
	runLegacySelectedSubscriberLoop(ctx, conn, deps, filter, config, start, compress, logger)
}

func runJournalSelectedSubscriberLoop(ctx context.Context, conn *websocket.Conn, deps Subscription, filter *FilterV2, config SelectedConfig, start uint64, compress bool, logger *slog.Logger) {
	if !deps.SelectedJournalMode || deps.SelectedHistory == nil || config.HistoryMode != "original-events" {
		runLegacySelectedSubscriberLoop(ctx, conn, deps, filter, config, start, compress, logger)
		return
	}
	journal := deps.SelectedHistory
	if !deps.SelectedJournalMode {
		runLegacySelectedSubscriberLoop(ctx, conn, deps, filter, config, start, compress, logger)
		return
	}
	if journal == nil || filter == nil {
		return
	}
	announced := uint64(0)
	announce := func(adoption history.PolicyAdoption) bool {
		var boundary *uint64
		if adoption.EffectiveAfterJournalSeq != nil {
			n := *adoption.EffectiveAfterJournalSeq
			boundary = &n
		}
		frame, err := EncodeV2SelectedInfo("PolicyAdopted", "Selected collection policy adopted", map[string]any{"contract": SelectedContract, "streamId": config.StreamID, "policyRevision": adoption.Policy.Revision, "collections": adoption.Policy.Collections, "effectiveAfterSeq": boundary})
		if err != nil || !writeFrame(ctx, conn, compress, frame) {
			return false
		}
		announced = adoption.Policy.Revision
		return true
	}
	if !announceJournalAdoptions(config.PolicyAdoptions, start-1, &announced, announce) {
		return
	}
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	cursor := start
	for {
		if !deps.SelectedJournalMode {
			readlog := deps.writer().ReadLog()
			entries, _, ok, _ := readlog.ReadCommittedFrom(cursor, deps.Tail.ReadBatch())
			if !ok {
				return
			}
			for _, entry := range entries {
				ev := entry.Event()
				if ev.Seq < readlog.DurableSeq() && filter.Wants(ev) {
					if err := processSelectedRow(ctx, conn, ev, deps, filter, config, compress, logger, &announced); err != nil {
						return
					}
				}
			}
			if len(entries) > 0 {
				cursor = entries[len(entries)-1].Event().Seq + 1
			}
			historyRows, err := deps.SelectedAttribution.ReadPolicyHistory()
			if err != nil || !announceApplicablePolicies(historyRows, cursor-1, announced, func(policy attribution.Policy) bool {
				frame, encodeErr := EncodeV2SelectedInfo("PolicyAdopted", "Selected collection policy adopted", map[string]any{"contract": SelectedContract, "streamId": config.StreamID, "policyRevision": policy.Revision, "collections": policy.Collections, "effectiveAfterSeq": policy.EffectiveAfterSeq})
				return encodeErr == nil && writeFrame(ctx, conn, compress, frame)
			}) {
				return
			}
		}
		page, wake, err := journal.WatchRead(cursor-1, deps.Tail.ReadBatch())
		if err != nil {
			logger.Error("selected journal read failed", "err", err)
			return
		}
		adoptions, adoptionErr := journal.ReadPolicyAdoptions()
		if adoptionErr != nil {
			logger.Error("selected journal policy adoption read failed", "err", adoptionErr)
			return
		}
		for _, row := range page.Events {
			if !announceJournalAdoptions(adoptions, row.Sequence-1, &announced, announce) {
				return
			}
			if err := processJournalRow(ctx, conn, row, filter, config, compress, logger); err != nil {
				return
			}
			cursor = row.Sequence + 1
		}
		if len(page.Events) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ping.C:
			pingCtx, cancel := context.WithTimeout(ctx, frameWriteTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func runLegacySelectedSubscriberLoop(ctx context.Context, conn *websocket.Conn, deps Subscription, filter *FilterV2, config SelectedConfig, start uint64, compress bool, logger *slog.Logger) {
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

func announceJournalAdoptions(adoptions []history.PolicyAdoption, boundary uint64, announced *uint64, announce func(history.PolicyAdoption) bool) bool {
	for _, adoption := range adoptions {
		if adoption.Policy.Revision <= *announced || adoption.EffectiveAfterJournalSeq == nil || *adoption.EffectiveAfterJournalSeq > boundary {
			continue
		}
		if !announce(adoption) {
			return false
		}
		*announced = adoption.Policy.Revision
	}
	return true
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
	policies, err := deps.SelectedAttribution.ReadPolicyHistory()
	if err != nil {
		return err
	}
	if ev.Seq > 0 && !announceApplicablePolicies(policies, ev.Seq-1, *announced, func(policy attribution.Policy) bool {
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
	meta, err := deps.SelectedAttribution.Lookup(ev.Seq)
	if err != nil && !errors.Is(err, attribution.ErrLegacyUnknown) {
		return err
	}
	selectedMetadata := SelectedMetadata(meta)
	if ev.Kind == segment.KindSync {
		if meta.PolicyRev == nil {
			return fmt.Errorf("selected sync missing policy attribution")
		}
		policy, err := deps.SelectedAttribution.LookupPolicy(*meta.PolicyRev)
		if err != nil {
			return err
		}
		selectedMetadata = selectedSyncMetadata(meta, ev.DID, policy)
	}
	body, err := EncodeV2Selected(ev, selectedMetadata)
	if err != nil {
		logger.Error("selected event encoding failed", "seq", ev.Seq, "err", err)
		return err
	}
	return writeSelectedFrame(ctx, conn, body, compress)
}

func writeSelectedFrame(ctx context.Context, conn *websocket.Conn, body []byte, compress bool) error {
	msgType := websocket.MessageText
	if compress {
		body = compressFrameV2(body)
		msgType = websocket.MessageBinary
	}
	writeCtx, cancel := context.WithTimeout(ctx, frameWriteTimeout)
	defer cancel()
	return conn.Write(writeCtx, msgType, body)
}

func processJournalRow(ctx context.Context, conn *websocket.Conn, row history.Event, filter *FilterV2, config SelectedConfig, compress bool, logger *slog.Logger) error {
	ev := row.InputEvent
	ev.Seq = row.Sequence
	if ev.Kind == 0 {
		if row.Kind != "source-gap" && row.Kind != "source-info" {
			return fmt.Errorf("selected journal row has unsupported control kind %q", row.Kind)
		}
		var outcome struct {
			Version uint8  `json:"version"`
			Reason  string `json:"reason"`
			Cursor  int64  `json:"cursor"`
		}
		if err := json.Unmarshal(row.Original, &outcome); err != nil || outcome.Version != 1 || outcome.Reason == "" {
			return fmt.Errorf("selected journal row has invalid control outcome")
		}
		name := "SourceGap"
		if row.Kind == "source-info" {
			name = "SourceInfo"
		}
		frame, err := EncodeV2SelectedInfo(name, outcome.Reason, map[string]any{"contract": SelectedContract, "streamId": config.StreamID, "cursor": row.Sequence, "sourceCursor": outcome.Cursor, "projectionDisposition": row.Disposition, "projectionBoundary": row.Boundary, "provenance": row.Provenance, "policyRevision": row.Policy.Revision, "reason": outcome.Reason})
		if err != nil {
			return err
		}
		if !writeFrame(ctx, conn, compress, frame) {
			return fmt.Errorf("selected journal control write failed")
		}
		return nil
	}
	if !filter.Wants(&ev) || !filterJournalEvent(ev, row.Policy) {
		return nil
	}
	revision := row.Attribution.Policy.Revision
	meta := SelectedEventMetadata{StreamID: config.StreamID, PolicyRev: &revision, Provenance: attribution.Provenance(row.Attribution.Provenance), ProjectionDisposition: row.Disposition}
	if row.Attribution.Boundary.Seq != nil || row.Attribution.Boundary.SnapshotID != nil || row.Attribution.Boundary.JournalSequence != nil {
		boundary := attribution.Boundary{Seq: row.Attribution.Boundary.Seq, SnapshotID: row.Attribution.Boundary.SnapshotID}
		meta.Boundary = &boundary
	}
	if row.Attribution.Boundary.JournalSequence != nil {
		boundary := *row.Attribution.Boundary.JournalSequence
		meta.ProjectionBoundary = &boundary
	} else if row.Boundary != 0 {
		boundary := row.Boundary
		meta.ProjectionBoundary = &boundary
	}
	if meta.Provenance == "" {
		meta.Provenance = attribution.Provenance(row.Provenance)
	}
	if ev.Kind == segment.KindSync {
		meta.ReconciliationRequired = true
		meta.Scope = &SelectedSyncScope{DID: row.DID, Collections: append([]string(nil), row.Policy.Collections...)}
	}
	body, err := EncodeV2Selected(&ev, meta)
	if err != nil {
		logger.Error("selected journal event encoding failed", "seq", row.Sequence, "err", err)
		return err
	}
	if !writeFrame(ctx, conn, compress, body) {
		return fmt.Errorf("selected journal write failed")
	}
	return nil
}

func filterJournalEvent(ev segment.Event, policy history.Policy) bool {
	if ev.Kind == segment.KindIdentity || ev.Kind == segment.KindAccount || ev.Kind == segment.KindSync {
		return true
	}
	if ev.Collection == "" {
		return true
	}
	for _, collection := range policy.Collections {
		if collection == ev.Collection {
			return true
		}
	}
	return false
}
