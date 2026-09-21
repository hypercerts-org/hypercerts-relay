package models

import "time"

// SourceState is the administrator's desired acquisition state. It is separate
// from Host.Status, which continues to reflect legacy/runtime host status.
type SourceState string

const (
	SourceStateEnabled  = SourceState("enabled")
	SourceStateDisabled = SourceState("disabled")
	SourceStateRemoved  = SourceState("removed")
)

// SourceValidationStatus records the latest explicit admission check.
type SourceValidationStatus string

const (
	SourceValidationPending = SourceValidationStatus("pending")
	SourceValidationPassed  = SourceValidationStatus("passed")
	SourceValidationFailed  = SourceValidationStatus("failed")
)

// SourceValidationReason is deliberately a bounded, non-diagnostic code.
type SourceValidationReason string

const (
	SourceValidationReasonNone            = SourceValidationReason("")
	SourceValidationReasonHostCheckFailed = SourceValidationReason("host_check_failed")
	SourceValidationReasonCheckerMissing  = SourceValidationReason("host_checker_unavailable")
)

// Source is the managed-source policy row for a Relay Host. Source state is
// intentionally independent of incidental runtime and cursor updates on Host.
type Source struct {
	HostID uint64 `gorm:"column:host_id;primaryKey" json:"hostID"`

	State         SourceState `gorm:"column:state;not null;default:enabled;index" json:"state"`
	Revision      uint64      `gorm:"column:revision;not null;default:1" json:"revision"`
	LastOperation string      `gorm:"column:last_operation;not null;default:''" json:"-"`

	ValidationStatus    SourceValidationStatus `gorm:"column:validation_status;not null;default:pending" json:"validationStatus"`
	ValidationCheckedAt *time.Time             `gorm:"column:validation_checked_at" json:"validationCheckedAt,omitempty"`
	ValidationReason    SourceValidationReason `gorm:"column:validation_reason;not null;default:''" json:"validationReason,omitempty"`

	// RecoveryRequired remains true until TECH-637 records a
	// durable boundary. Relay lifecycle changes never clear it.
	// Creation paths explicitly require recovery; preserve a completed boundary's false value.
	RecoveryRequired bool `gorm:"column:recovery_required;not null" json:"recoveryRequired"`
	// RecoveryPolicyRevision is the latest Jetstream collection-policy
	// revision that may clear RecoveryRequired. Zero preserves pre-mirror
	// source rows until Jetstream advances their policy through the private seam.
	RecoveryPolicyRevision uint64 `gorm:"column:recovery_policy_revision;not null;default:0" json:"-"`
}

func (Source) TableName() string {
	return "source"
}

// RecoveryReceipt is the Relay-owned acknowledgement of one Jetstream job's
// durable current-state boundary. It records only bounded coordinates, never
// job input, archive payloads, or a remote error.
//
// The composite key deliberately includes every versioned boundary. A receipt
// for a prior source or collection-policy revision cannot acknowledge a later
// recovery requirement.
type RecoveryReceipt struct {
	ID uint64 `gorm:"column:id;primarykey" json:"id"`

	CreatedAt time.Time `json:"createdAt"`

	PDS             string `gorm:"column:pds;not null;uniqueIndex:idx_recovery_receipt_boundary,priority:1" json:"pds"`
	HostID          uint64 `gorm:"column:host_id;not null;index" json:"hostID"`
	SourceRevision  uint64 `gorm:"column:source_revision;not null;uniqueIndex:idx_recovery_receipt_boundary,priority:2" json:"sourceRevision"`
	PolicyRevision  uint64 `gorm:"column:policy_revision;not null;uniqueIndex:idx_recovery_receipt_boundary,priority:3" json:"policyRevision"`
	JobID           string `gorm:"column:job_id;not null;uniqueIndex:idx_recovery_receipt_boundary,priority:4" json:"jobID"`
	DurableBoundary string `gorm:"column:durable_boundary;not null;uniqueIndex:idx_recovery_receipt_boundary,priority:5" json:"durableBoundary"`
}

func (RecoveryReceipt) TableName() string {
	return "recovery_receipt"
}

// AccountSourceObservation preserves source attribution independently from an
// account's current HostID. One row is maintained for each DID/source pair.
type AccountSourceObservation struct {
	DID            string `gorm:"column:did;primaryKey;index:idx_observation_host_did,priority:2" json:"did"`
	ObservedHostID uint64 `gorm:"column:observed_host_id;primaryKey;index:idx_observation_host_did,priority:1" json:"observedHostID"`

	ResolvedHostname string    `gorm:"column:resolved_hostname;not null" json:"resolvedHostname"`
	ObservedAt       time.Time `gorm:"column:observed_at;not null" json:"observedAt"`
	ResolvedAt       time.Time `gorm:"column:resolved_at;not null" json:"resolvedAt"`

	// AdmissionReason is a stable reason captured when the account was
	// observed. It never contains upstream errors or event payloads.
	AdmissionReason string `gorm:"column:admission_reason;not null;default:''" json:"admissionReason,omitempty"`
}

func (AccountSourceObservation) TableName() string {
	return "account_source_observation"
}
