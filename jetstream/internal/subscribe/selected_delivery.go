package subscribe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
)

type selectedConnection struct {
	conn      *websocket.Conn
	deps      Subscription
	filter    *FilterV2
	config    SelectedConfig
	compress  bool
	logger    *slog.Logger
	cursor    uint64
	announced uint64
}

func (s *selectedConnection) runSelectedSubscriberLoop(ctx context.Context) {
	writer := s.deps.writer()
	if writer == nil || s.deps.SelectedAttribution == nil || s.filter == nil {
		return
	}
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		readlog := writer.ReadLog()
		entries, notify, ok, atTip := readlog.ReadCommittedFrom(s.cursor, s.deps.Tail.ReadBatch())
		var keepRunning bool
		switch {
		case ok:
			keepRunning = s.deliverResident(ctx, readlog, entries)
		case !atTip && s.cursor < readlog.FloorSeq():
			keepRunning = s.deliverCold(ctx, readlog)
		default:
			keepRunning = s.announceIdlePolicies(ctx) && s.wait(ctx, notify, ping.C)
		}
		if !keepRunning {
			return
		}
	}
}

func (s *selectedConnection) deliverCold(ctx context.Context, readlog *ingest.ReadableLog) bool {
	batch, next, err := s.deps.Tail.ReadFrom(ctx, s.cursor, s.deps.Tail.ReadBatch())
	if err != nil {
		s.coldReadFailed(ctx, err)
		return false
	}
	if len(batch) == 0 {
		if next > s.cursor && next < readlog.DurableSeq() {
			s.cursor = next
			return true
		}
		return false
	}
	last := s.cursor
	for _, entry := range batch {
		if entry.Event.Seq >= readlog.DurableSeq() {
			break
		}
		if err := s.processSelectedRow(ctx, entry.Event); err != nil {
			return false
		}
		last = entry.Event.Seq + 1
	}
	if last == s.cursor && next > s.cursor && next < readlog.DurableSeq() {
		s.cursor = next
	} else {
		s.cursor = last
	}
	return true
}

func (s *selectedConnection) coldReadFailed(ctx context.Context, err error) {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return
	}
	message := "stream read failed; reconnect"
	if errors.Is(err, errColdUnavailable) {
		message = "archive replay unavailable; reconnect"
	}
	s.logger.Warn("selected cold replay failed", "cursor", s.cursor, "streamId", s.config.StreamID)
	writeFrame(ctx, s.conn, s.compress, EncodeV2Error("InternalError", message))
}

func (s *selectedConnection) deliverResident(ctx context.Context, readlog *ingest.ReadableLog, entries []*ingest.ReadLogEntry) bool {
	for _, row := range entries {
		ev := row.Event()
		if ev.Seq >= readlog.DurableSeq() {
			break
		}
		if err := s.processSelectedRow(ctx, ev); err != nil {
			return false
		}
		s.cursor = ev.Seq + 1
	}
	return true
}

func (s *selectedConnection) announceIdlePolicies(ctx context.Context) bool {
	history, err := s.deps.SelectedAttribution.ReadPolicyHistory()
	lastDelivered := uint64(0)
	if s.cursor > 0 {
		lastDelivered = s.cursor - 1
	}
	if err != nil || !announceApplicablePolicies(history, lastDelivered, s.announced, func(policy attribution.Policy) bool {
		return s.announcePolicy(ctx, policy)
	}) {
		s.logger.Error("selected policy history read failed", "err", err)
		return false
	}
	return true
}

func (s *selectedConnection) wait(ctx context.Context, notify <-chan struct{}, ping <-chan time.Time) bool {
	select {
	case <-ctx.Done():
		return false
	case <-ping:
		pingCtx, cancel := context.WithTimeout(ctx, frameWriteTimeout)
		err := s.conn.Ping(pingCtx)
		cancel()
		return err == nil
	case <-notify:
		return true
	}
}

func (s *selectedConnection) announcePolicy(ctx context.Context, policy attribution.Policy) bool {
	if policy.Revision <= s.announced {
		return true
	}
	frame, err := EncodeV2SelectedInfo("PolicyAdopted", "Selected collection policy adopted", map[string]any{"contract": SelectedContract, "streamId": s.config.StreamID, "policyRevision": policy.Revision, "collections": policy.Collections, "effectiveAfterSeq": policy.EffectiveAfterSeq})
	if err != nil || !writeFrame(ctx, s.conn, s.compress, frame) {
		return false
	}
	s.announced = policy.Revision
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

func (s *selectedConnection) processSelectedRow(ctx context.Context, ev *segment.Event) error {
	if !s.filter.Wants(ev) {
		return nil
	}
	if s.deps.SelectedAttribution == nil {
		return fmt.Errorf("selected attribution unavailable")
	}
	_, err := s.deps.SelectedAttribution.Lookup(ev.Seq)
	if err != nil && !errors.Is(err, attribution.ErrLegacyUnknown) {
		return err
	}
	history, err := s.deps.SelectedAttribution.ReadPolicyHistory()
	if err != nil {
		return err
	}
	if ev.Seq > 0 && !announceApplicablePolicies(history, ev.Seq-1, s.announced, func(policy attribution.Policy) bool {
		return s.announcePolicy(ctx, policy)
	}) {
		return fmt.Errorf("failed to announce governing policy")
	}
	return deliverSelected(ctx, s.conn, ev, s.deps, s.filter, s.compress, s.logger)
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
		selectedMetadata, err = selectedDeliverySyncMetadata(ev, meta, deps.SelectedAttribution, logger)
		if err != nil {
			return err
		}
	}
	body, err := EncodeV2Selected(ev, selectedMetadata)
	if err != nil {
		logger.Error("selected event encoding failed", "seq", ev.Seq, "err", err)
		return err
	}
	if max := filter.MaxMessageSizeBytes(); max > 0 && uint64(len(body)) > uint64(max) {
		deps.Metrics.incEventsOversize()
		return nil
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

func selectedDeliverySyncMetadata(ev *segment.Event, meta attribution.Event, manager *attribution.Manager, logger *slog.Logger) (SelectedEventMetadata, error) {
	if meta.PolicyRev == nil {
		return SelectedEventMetadata{}, ErrReplayUnavailable
	}
	policy, policyErr := manager.LookupPolicy(*meta.PolicyRev)
	if policyErr != nil {
		logger.Error("selected sync policy lookup failed", "seq", ev.Seq, "err", policyErr)
		return SelectedEventMetadata{}, policyErr
	}
	scope, scopeErr := tombstone.DecodeScopedSync(ev)
	if scopeErr != nil {
		return SelectedEventMetadata{}, scopeErr
	}
	if scope != nil {
		collections := make([]string, 0, len(scope.Collections))
		for _, collection := range scope.Collections {
			if slices.Contains(policy.Collections, collection) {
				collections = append(collections, collection)
			}
		}
		policy.Collections = collections
	}
	return selectedSyncMetadata(meta, ev.DID, policy), nil
}
