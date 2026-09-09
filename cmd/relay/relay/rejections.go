package relay

import (
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/relay/models"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"gorm.io/gorm/clause"
)

const RejectionPolicyRevision = "verification-d08-v1"

func (r *Relay) rejectionPolicyRevision() string {
	if r.Config.LenientSyncValidation {
		return RejectionPolicyRevision + "-lenient"
	}
	return RejectionPolicyRevision
}

const (
	defaultRejectedEventsLimit = 100
	maxRejectedEventsLimit     = 1_000
)

const (
	rejectionReasonMalformedEvent   = "malformed_event"
	rejectionReasonMalformedCommit  = "malformed_commit"
	rejectionReasonInvalidCommit    = "invalid_commit"
	rejectionReasonInvalidMST       = "invalid_mst"
	rejectionReasonInvalidSignature = "invalid_signature"
)

// permanentEventError deliberately keeps its public message bounded. Its cause
// is available to local callers through errors.Is/As but is never persisted.
type permanentEventError struct {
	reasonCode string
	cause      error
}

func (e *permanentEventError) Error() string {
	return "permanently rejected event: " + e.reasonCode
}

func (e *permanentEventError) Unwrap() error {
	return e.cause
}

func newPermanentEventError(reasonCode string, cause error) error {
	return &permanentEventError{reasonCode: reasonCode, cause: cause}
}

func permanentRejectionReason(err error) (string, bool) {
	var rejection *permanentEventError
	if !errors.As(err, &rejection) {
		return "", false
	}
	return rejection.reasonCode, true
}

type rejectedEventSource struct {
	hostID   uint64
	position int64
	did      string
	kind     string
}

func rejectedEventSourceFor(evt *stream.XRPCStreamEvent, hostID uint64) (rejectedEventSource, bool) {
	var source rejectedEventSource
	source.hostID = hostID

	switch {
	case evt.RepoCommit != nil:
		source.position = evt.RepoCommit.Seq
		source.did = normalizedRejectionDID(evt.RepoCommit.Repo)
		source.kind = "commit"
	case evt.RepoSync != nil:
		source.position = evt.RepoSync.Seq
		source.did = normalizedRejectionDID(evt.RepoSync.Did)
		source.kind = "sync"
	case evt.RepoIdentity != nil:
		source.position = evt.RepoIdentity.Seq
		source.did = normalizedRejectionDID(evt.RepoIdentity.Did)
		source.kind = "identity"
	case evt.RepoAccount != nil:
		source.position = evt.RepoAccount.Seq
		source.did = normalizedRejectionDID(evt.RepoAccount.Did)
		source.kind = "account"
	default:
		return rejectedEventSource{}, false
	}

	return source, true
}

func normalizedRejectionDID(didStr string) string {
	did, err := syntax.ParseDID(didStr)
	if err != nil {
		return ""
	}
	return NormalizeDID(did).String()
}

func (s rejectedEventSource) idempotencyKey(policy string) string {
	return fmt.Sprintf("%d:%d:%s:%s", s.hostID, s.position, s.kind, policy)
}

func (r *Relay) wasRejectedEvent(ctx context.Context, evt *stream.XRPCStreamEvent, hostID uint64) (bool, error) {
	source, ok := rejectedEventSourceFor(evt, hostID)
	if !ok {
		return false, nil
	}
	var count int64
	err := r.db.WithContext(ctx).Model(models.RejectedEvent{}).
		Where("idempotency_key = ?", source.idempotencyKey(r.rejectionPolicyRevision())).Count(&count).Error
	return count > 0, err
}

func (r *Relay) persistRejectedEvent(ctx context.Context, evt *stream.XRPCStreamEvent, hostID uint64, reasonCode string) error {
	source, ok := rejectedEventSourceFor(evt, hostID)
	if !ok {
		return fmt.Errorf("permanent rejection has no supported source event")
	}

	rejection := &models.RejectedEvent{
		SourceHostID:   source.hostID,
		SourcePosition: source.position,
		DID:            source.did,
		EventKind:      source.kind,
		PolicyRevision: r.rejectionPolicyRevision(),
		ReasonCode:     reasonCode,
		IdempotencyKey: source.idempotencyKey(r.rejectionPolicyRevision()),
	}
	if err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "idempotency_key"}},
		DoNothing: true,
	}).Create(rejection).Error; err != nil {
		return fmt.Errorf("persisting rejected event: %w", err)
	}
	return nil
}

// ListRejectedEvents returns a bounded page after the supplied database ID.
func (r *Relay) ListRejectedEvents(ctx context.Context, afterID uint64, limit int) ([]*models.RejectedEvent, error) {
	if limit <= 0 {
		limit = defaultRejectedEventsLimit
	}
	if limit > maxRejectedEventsLimit {
		limit = maxRejectedEventsLimit
	}

	events := make([]*models.RejectedEvent, 0, limit)
	if err := r.db.WithContext(ctx).
		Where("id > ?", afterID).
		Order("id ASC").
		Limit(limit).
		Find(&events).Error; err != nil {
		return nil, fmt.Errorf("listing rejected events: %w", err)
	}
	return events, nil
}
