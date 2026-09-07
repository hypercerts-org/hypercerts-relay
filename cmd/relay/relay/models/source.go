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

	// RecoveryRequired remains true until the Plan 006 recovery owner records a
	// durable boundary. Relay lifecycle changes never clear it.
	RecoveryRequired bool `gorm:"column:recovery_required;not null;default:true" json:"recoveryRequired"`
}

func (Source) TableName() string {
	return "source"
}

// AccountSourceObservation preserves source attribution independently from an
// account's current HostID. One row is maintained for each DID/source pair.
type AccountSourceObservation struct {
	DID            string `gorm:"column:did;primaryKey" json:"did"`
	ObservedHostID uint64 `gorm:"column:observed_host_id;primaryKey" json:"observedHostID"`

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
