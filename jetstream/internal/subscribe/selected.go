package subscribe

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
)

const SelectedContract = "hypercerts-selected-v1"

var (
	ErrUnsupportedContract = fmt.Errorf("UnsupportedContract")
	ErrStreamMismatch      = fmt.Errorf("StreamMismatch")
	ErrCursorOutOfRange    = fmt.Errorf("CursorOutOfRange")
	ErrReplayUnavailable   = fmt.Errorf("ReplayUnavailable")
)

type SelectedConfig struct {
	StreamID                string
	PolicyRevision          uint64
	PolicyEffectiveAfterSeq *uint64
	Collections             []string
	EffectiveAfterSeq       *uint64
	ReplayFloor             *uint64
	CurrentSeq              uint64
	ReplayEnabled           bool
	HistoryMode             string
	OriginalHistoryFromSeq  *uint64
	Coverage                string
	Gaps                    []SelectedGap
}
type SelectedGap struct {
	FromSeq    *uint64 `json:"fromSeq"`
	ThroughSeq *uint64 `json:"throughSeq"`
	Reason     string  `json:"reason"`
	Collection *string `json:"collection"`
}
type SelectedConfigProvider interface {
	SelectedConfig() (SelectedConfig, error)
}
type SelectedConfigProviderFunc func() (SelectedConfig, error)

func (f SelectedConfigProviderFunc) SelectedConfig() (SelectedConfig, error) { return f() }

type SelectedRequest struct {
	Contract string
	StreamID string
	Cursor   *uint64
}

func ParseSelectedRequest(contract, streamID, rawCursor string, config SelectedConfig) (SelectedRequest, error) {
	if contract != "" && contract != SelectedContract {
		return SelectedRequest{}, ErrUnsupportedContract
	}
	r := SelectedRequest{Contract: contract, StreamID: streamID}
	if contract == "" {
		return r, nil
	}
	if (streamID == "") != (rawCursor == "") {
		return SelectedRequest{}, ErrInvalidCursor
	}
	if rawCursor == "" {
		return r, nil
	}
	cursor, err := strconv.ParseUint(rawCursor, 10, 64)
	if err != nil || strings.TrimSpace(rawCursor) != rawCursor || strings.HasPrefix(rawCursor, "+") {
		return SelectedRequest{}, ErrInvalidCursor
	}
	r.Cursor = &cursor
	if streamID != config.StreamID {
		return SelectedRequest{}, ErrStreamMismatch
	}
	if cursor > config.CurrentSeq {
		return SelectedRequest{}, ErrCursorOutOfRange
	}
	if !config.ReplayEnabled || (config.ReplayFloor != nil && cursor < *config.ReplayFloor) {
		return SelectedRequest{}, ErrReplayUnavailable
	}
	return r, nil
}

type SelectedEventMetadata struct {
	StreamID               string                 `json:"streamId"`
	PolicyRev              *uint64                `json:"policyRev"`
	Provenance             attribution.Provenance `json:"provenance"`
	Boundary               *attribution.Boundary  `json:"reconciliationBoundary,omitempty"`
	ReconciliationRequired bool                   `json:"reconciliationRequired,omitempty"`
	Scope                  *SelectedSyncScope     `json:"scope,omitempty"`
}

type SelectedSyncScope struct {
	DID         string   `json:"did"`
	Collections []string `json:"collections"`
}

// PolicyGaps derives exact intervals during which a collection was excluded.
// A removal takes effect after its boundary; a later re-addition closes the
// interval inclusively at that policy's boundary. A nil ThroughSeq is open.
func PolicyGaps(history []attribution.Policy) []SelectedGap {
	type openGap struct{ from uint64 }
	open := make(map[string]openGap)
	var gaps []SelectedGap
	for i, policy := range history {
		if i == 0 {
			continue
		}
		prior := make(map[string]bool, len(history[i-1].Collections))
		for _, collection := range history[i-1].Collections {
			prior[collection] = true
		}
		current := make(map[string]bool, len(policy.Collections))
		for _, collection := range policy.Collections {
			current[collection] = true
		}
		boundary := uint64(0)
		if policy.EffectiveAfterSeq != nil {
			boundary = *policy.EffectiveAfterSeq
		}
		for collection := range prior {
			if !current[collection] {
				open[collection] = openGap{from: boundary + 1}
			}
		}
		for collection := range current {
			if !prior[collection] {
				if gap, ok := open[collection]; ok {
					name := collection
					from, through := gap.from, boundary
					gaps = append(gaps, SelectedGap{FromSeq: &from, ThroughSeq: &through, Reason: "policy-disabled", Collection: &name})
					delete(open, collection)
				}
			}
		}
	}
	for collection, gap := range open {
		name, from := collection, gap.from
		gaps = append(gaps, SelectedGap{FromSeq: &from, Reason: "policy-disabled", Collection: &name})
	}
	sort.Slice(gaps, func(i, j int) bool { return *gaps[i].FromSeq < *gaps[j].FromSeq })
	return gaps
}

func SelectedMetadata(event attribution.Event) SelectedEventMetadata {
	metadata := SelectedEventMetadata{StreamID: event.StreamID, PolicyRev: event.PolicyRev, Provenance: event.Provenance}
	if event.Boundary.Seq != nil || event.Boundary.SnapshotID != nil {
		boundary := event.Boundary
		metadata.Boundary = &boundary
	}
	return metadata
}

func selectedSyncMetadata(event attribution.Event, did string, policy attribution.Policy) SelectedEventMetadata {
	metadata := SelectedMetadata(event)
	metadata.ReconciliationRequired = true
	metadata.Scope = &SelectedSyncScope{DID: did, Collections: append([]string(nil), policy.Collections...)}
	return metadata
}

func EncodeV2Selected(evt *segment.Event, metadata SelectedEventMetadata) ([]byte, error) {
	if evt.Kind == segment.KindSync {
		return encodeV2SelectedSync(evt, metadata)
	}
	if metadata.StreamID == "" {
		return nil, fmt.Errorf("selected stream: empty stream identity")
	}
	if metadata.Provenance != attribution.ProvenanceLive && metadata.Provenance != attribution.ProvenanceSnapshot && metadata.Provenance != attribution.ProvenanceReconciliation && metadata.Provenance != attribution.ProvenanceLegacyUnknown {
		return nil, fmt.Errorf("selected stream: invalid provenance %q", metadata.Provenance)
	}
	if metadata.PolicyRev != nil && *metadata.PolicyRev > math.MaxInt64 {
		return nil, fmt.Errorf("selected stream: policy revision out of range")
	}
	base, err := EncodeV2(evt)
	if err != nil {
		return nil, err
	}
	return addHypercerts(base, metadata)
}
func encodeV2SelectedSync(evt *segment.Event, metadata SelectedEventMetadata) ([]byte, error) {
	if metadata.StreamID == "" || !metadata.ReconciliationRequired || metadata.Scope == nil {
		return nil, fmt.Errorf("selected stream: incomplete sync scope")
	}
	if metadata.Provenance != attribution.ProvenanceLive && metadata.Provenance != attribution.ProvenanceSnapshot && metadata.Provenance != attribution.ProvenanceReconciliation && metadata.Provenance != attribution.ProvenanceLegacyUnknown {
		return nil, fmt.Errorf("selected stream: invalid provenance %q", metadata.Provenance)
	}
	if metadata.PolicyRev != nil && *metadata.PolicyRev > math.MaxInt64 {
		return nil, fmt.Errorf("selected stream: policy revision out of range")
	}
	// Rebuild the typed sync payload from approved lifecycle fields. The CAR
	// in evt.Payload is intentionally never decoded or serialized here.
	syncEvent := comatproto.SyncSubscribeRepos_Sync{DID: evt.DID, Rev: evt.Rev, Time: wireTime(evt.WitnessedAt), Blocks: []byte{}}
	msg := jetstream.JetstreamSubscribeEvents_Message{JetstreamSubscribeEvents_Sync: gt.SomeRef(jetstream.JetstreamSubscribeEvents_Sync{Seq: int64(evt.Seq), DID: evt.DID, Time: wireTime(evt.WitnessedAt), Sync: syncEvent})}
	base := append([]byte(nil), messageFramePrefix...)
	var err error
	base, err = msg.AppendJSON(base)
	if err != nil {
		return nil, fmt.Errorf("selected stream: encode sync: %w", err)
	}
	base = append(base, messageFrameSuffix...)
	return addHypercerts(base, metadata)
}

func EncodeV2SelectedInfo(name, message string, metadata any) ([]byte, error) {
	base, err := EncodeV2Info(name, message)
	if err != nil {
		return nil, err
	}
	return addHypercerts(base, metadata)
}
func addHypercerts(frame []byte, metadata any) ([]byte, error) {
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(frame, &outer); err != nil {
		return nil, err
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(outer["payload"], &payload); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	payload["hypercerts"] = encoded
	outer["payload"], err = json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(outer)
}
