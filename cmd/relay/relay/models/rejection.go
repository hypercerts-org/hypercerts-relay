package models

import "time"

// RejectedEvent records a permanently invalid upstream event without retaining
// its payload or an unbounded upstream error message.
type RejectedEvent struct {
	ID uint64 `gorm:"column:id;primarykey" json:"id"`

	CreatedAt time.Time `json:"createdAt"`

	SourceHostID   uint64 `gorm:"column:source_host_id;not null;index" json:"sourceHostID"`
	SourcePosition int64  `gorm:"column:source_position;not null;index" json:"sourcePosition"`
	DID            string `gorm:"column:did;not null;default:'';index" json:"did,omitempty"`
	EventKind      string `gorm:"column:event_kind;not null" json:"eventKind"`
	PolicyRevision string `gorm:"column:policy_revision;not null" json:"policyRevision"`
	ReasonCode     string `gorm:"column:reason_code;not null" json:"reasonCode"`

	// IdempotencyKey is derived only from durable source metadata and policy.
	IdempotencyKey string `gorm:"column:idempotency_key;not null;uniqueIndex" json:"idempotencyKey"`
}

func (RejectedEvent) TableName() string {
	return "rejected_event"
}
