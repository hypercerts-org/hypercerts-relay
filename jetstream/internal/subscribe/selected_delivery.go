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
	scheme    string
	logger    *slog.Logger
	cursor    uint64
	announced uint64
	history   []attribution.Policy
}

func (s *selectedConnection) runSelectedSubscriberLoop(ctx context.Context) {
	defer func() {
		if ctx.Err() != nil {
			s.deps.Metrics.incCleanDisconnects()
		}
	}()
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
	s.history = nil
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
		s.deliveryFailed(ctx, "empty cold batch", ErrReplayUnavailable)
		return false
	}
	start := s.cursor
	last := s.cursor
	for _, entry := range batch {
		if entry.Event.Seq >= readlog.DurableSeq() {
			break
		}
		if err := s.processSelectedRow(ctx, entry.Event); err != nil {
			s.deliveryFailed(ctx, "cold row", err)
			return false
		}
		last = entry.Event.Seq + 1
	}
	s.cursor = last
	if s.cursor == start {
		s.deliveryFailed(ctx, "cold replay made no progress", ErrReplayUnavailable)
		return false
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
	s.history = nil
	for _, row := range entries {
		ev := row.Event()
		if ev.Seq >= readlog.DurableSeq() {
			break
		}
		if err := s.processSelectedRow(ctx, ev); err != nil {
			s.deliveryFailed(ctx, "resident row", err)
			return false
		}
		s.cursor = ev.Seq + 1
	}
	return true
}

var errSelectedTransport = errors.New("selected transport failed")

func (s *selectedConnection) deliveryFailed(ctx context.Context, stage string, err error) {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, errSelectedTransport) {
		return
	}
	s.logger.Warn("selected delivery failed", "stage", stage, "cursor", s.cursor, "errorType", fmt.Sprintf("%T", err))
	writeFrame(ctx, s.conn, s.compress, EncodeV2Error("InternalError", "selected delivery failed; reconnect"))
}

func (s *selectedConnection) announceIdlePolicies(ctx context.Context) bool {
	history, err := s.deps.SelectedAttribution.ReadPolicyHistory()
	if err != nil {
		s.deliveryFailed(ctx, "idle policy history", err)
		return false
	}
	lastDelivered := uint64(0)
	if s.cursor > 0 {
		lastDelivered = s.cursor - 1
	}
	if err := s.announcePolicies(ctx, history, lastDelivered); err != nil {
		s.deliveryFailed(ctx, "idle policy announcement", err)
		return false
	}
	return true
}

func (s *selectedConnection) announcePolicies(ctx context.Context, history []attribution.Policy, lastDelivered uint64) error {
	var announceErr error
	announceApplicablePolicies(history, lastDelivered, s.announced, func(policy attribution.Policy) bool {
		announceErr = s.announcePolicy(ctx, policy)
		return announceErr == nil
	})
	return announceErr
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

func (s *selectedConnection) announcePolicy(ctx context.Context, policy attribution.Policy) error {
	if policy.Revision <= s.announced {
		return nil
	}
	frame, err := EncodeV2SelectedInfo("PolicyAdopted", "Selected collection policy adopted", map[string]any{"contract": SelectedContract, "streamId": s.config.StreamID, "policyRevision": policy.Revision, "collections": policy.Collections, "effectiveAfterSeq": policy.EffectiveAfterSeq})
	if err != nil {
		return err
	}
	if err := s.writeSelectedFrame(ctx, frame, false); err != nil {
		return err
	}
	s.announced = policy.Revision
	return nil
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
	meta, err := s.deps.SelectedAttribution.Lookup(ev.Seq)
	if err != nil && !errors.Is(err, attribution.ErrLegacyUnknown) {
		return err
	}
	if s.history == nil || meta.PolicyRev != nil && (len(s.history) == 0 || *meta.PolicyRev > s.history[len(s.history)-1].Revision) {
		s.history, err = s.deps.SelectedAttribution.ReadPolicyHistory()
		if err != nil {
			return err
		}
	}
	if ev.Seq > 0 {
		if err := s.announcePolicies(ctx, s.history, ev.Seq-1); err != nil {
			return err
		}
	}
	return s.deliverSelected(ctx, ev, meta)
}

func (s *selectedConnection) deliverSelected(ctx context.Context, ev *segment.Event, meta attribution.Event) error {
	var err error
	selectedMetadata := SelectedMetadata(meta)
	if ev.Kind == segment.KindSync {
		selectedMetadata, err = selectedDeliverySyncMetadata(ev, meta, s.deps.SelectedAttribution, s.logger)
		if err != nil {
			return err
		}
	}
	body, err := EncodeV2Selected(ev, selectedMetadata)
	if err != nil {
		s.logger.Error("selected event encoding failed", "seq", ev.Seq, "errorType", fmt.Sprintf("%T", err))
		return err
	}
	if maxBytes := s.filter.MaxMessageSizeBytes(); maxBytes > 0 && uint64(len(body)) > uint64(maxBytes) {
		s.deps.Metrics.incEventsOversize()
		return nil
	}
	return s.writeSelectedFrame(ctx, body, true)
}

func (s *selectedConnection) writeSelectedFrame(ctx context.Context, body []byte, event bool) error {
	encodedBytes := len(body)
	msgType := websocket.MessageText
	if s.compress {
		body = compressFrameV2(body)
		msgType = websocket.MessageBinary
	}
	writeCtx, cancel := context.WithTimeout(ctx, frameWriteTimeout)
	defer cancel()
	if err := s.conn.Write(writeCtx, msgType, body); err != nil {
		return fmt.Errorf("%w: %w", errSelectedTransport, err)
	}
	if event {
		s.deps.Metrics.incEventsSent(s.scheme, len(body), encodedBytes)
	}
	return nil
}

func selectedDeliverySyncMetadata(ev *segment.Event, meta attribution.Event, manager *attribution.Manager, logger *slog.Logger) (SelectedEventMetadata, error) {
	if meta.PolicyRev == nil {
		return SelectedEventMetadata{}, ErrReplayUnavailable
	}
	policy, policyErr := manager.LookupPolicy(*meta.PolicyRev)
	if policyErr != nil {
		logger.Error("selected sync policy lookup failed", "seq", ev.Seq, "errorType", fmt.Sprintf("%T", policyErr))
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
