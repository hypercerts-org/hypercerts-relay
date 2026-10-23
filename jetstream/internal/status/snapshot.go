package status

import (
	"encoding/json"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
)

// Snapshot is the rendering-agnostic view of jetstream's state at a
// single moment. Treated as immutable after construction; consumers
// share a *Snapshot pointer without locks.
type Snapshot struct {
	GeneratedAt      time.Time
	Request          Request
	Process          ProcessInfo
	Phase            PhaseInfo
	Backfill         BackfillStats
	Live             LiveStats
	SegmentAggregate *SegmentAggregate
	Pebble           PebbleStats
	CursorLookback   CursorLookbackStats
	Hosts            HostDiagnostics
	Account          AccountLookup
	Import           *ImportInfo
	Coverage         *CoverageInfo
}

// ImportInfo is the status-page view of the current (or most recent)
// timestamp-import job. Nil on the Snapshot when import has never run this
// data dir. Populated from the importer job manager via an ImportReporter, so
// the status package stays decoupled from the importer's concrete types.
type ImportInfo struct {
	JobID                 string
	State                 string
	Phase                 string
	Error                 string
	SubmittedAt           time.Time
	FinishedAt            time.Time
	Bucketed              bool
	SegmentsToApply       int
	SegmentsApplied       int
	RowsTotal             uint64
	RowsValid             uint64
	RowsRejected          uint64
	SegmentsExamined      int
	SegmentsPatched       int
	RowsMutated           uint64
	RowsMatchedSpecific   uint64
	SpecificCIDsUnmatched uint64
	RowsCorruptOffset     uint64
}

// ImportReporter yields the current or most recent import job for the status
// page. Implemented by the importer job manager (adapted in jetstreamd). ok is
// false when no import has run.
type ImportReporter interface{ CurrentImport() (ImportInfo, bool) }

// CoverageReporter projects current-policy acquisition evidence into a safe
// public-status DTO. Its payload intentionally does not expose job internals.
type CoverageReporter interface{ CurrentCoverage() (CoverageInfo, bool) }

type CoveragePolicy struct {
	Revision    uint64   `json:"revision"`
	Collections []string `json:"collections"`
}
type CoverageAggregate struct {
	Scope          string `json:"scope"`
	PolicyRevision uint64 `json:"policyRevision"`
	EnabledSources int    `json:"enabledSources"`
	State          string `json:"state"`
}
type CoverageCount struct {
	Known bool `json:"-"`
	Value int  `json:"-"`
}

func (c CoverageCount) MarshalJSON() ([]byte, error) {
	if !c.Known {
		return []byte("null"), nil
	}
	return json.Marshal(c.Value)
}

type CoverageCounts struct {
	InitialInventory    CoverageCount `json:"initialInventory"`
	Scanned             CoverageCount `json:"scanned"`
	Matching            CoverageCount `json:"matching"`
	NoMatch             CoverageCount `json:"noMatch"`
	Unresolved          CoverageCount `json:"unresolved"`
	AttributableRecords CoverageCount `json:"attributableRecords"`
}
type CoverageSource struct {
	PDS             string         `json:"pds"`
	SourceRevision  uint64         `json:"sourceRevision"`
	PolicyRevision  uint64         `json:"policyRevision"`
	Collections     []string       `json:"collections"`
	MatchingJob     bool           `json:"matchingJob"`
	State           string         `json:"state"`
	ProgressScope   string         `json:"progressScope"`
	Counts          CoverageCounts `json:"counts"`
	FailureCategory string         `json:"failureCategory,omitempty"`
	Attempts        int            `json:"attempts"`
	LastProgressAt  *time.Time     `json:"lastProgressAt"`
	RetryAction     string         `json:"retryAction"`
	HistoryState    string         `json:"historyState"`
	LiveState       string         `json:"liveState"`
}
type CoverageInfo struct {
	SchemaVersion int               `json:"schemaVersion"`
	Scope         string            `json:"scope"`
	Policy        CoveragePolicy    `json:"policy"`
	Aggregate     CoverageAggregate `json:"aggregate"`
	Sources       []CoverageSource  `json:"sources"`
}

// Request selects the status-page view and optional account lookup.
type Request struct {
	Tab      string
	Account  string
	DID      string
	Handle   string
	HostSort string
}

// ProcessInfo carries the per-process build + uptime context.
type ProcessInfo struct {
	Version   string
	Commit    string
	BuiltAt   string
	StartedAt time.Time
	Uptime    time.Duration
	GoVersion string
}

// PhaseInfo holds the current persisted phase plus its transition timestamp.
type PhaseInfo struct {
	Phase          lifecycle.Phase
	PhaseEnteredAt time.Time
}

// BackfillStats summarizes the repo/ keyspace.
type BackfillStats struct {
	TotalDIDs          uint64
	Discovered         uint64
	Pending            uint64
	Complete           uint64
	Failed             uint64
	Unavailable        uint64
	PercentComplete    float64
	StartedAt          time.Time
	CompletedAt        time.Time
	Duration           time.Duration
	HostsTotal         uint64
	HostsDrained       uint64
	HostsExhausted     uint64
	HostsInFlight      uint64
	RelayAccountFloor  uint64
	EnumeratedAccounts uint64
	TopRemainingHosts  []BackfillHostRow
}

// BackfillHostRow is one control-plane roster row selected for the backfill summary.
type BackfillHostRow struct {
	Hostname           string
	State              string
	RelayAccounts      uint64
	EnumeratedAccounts uint64
	RemainingEstimate  uint64
	Attempts           int
	LastError          string
}

// HostDiagnostics is the host/PDS aggregate table.
type HostDiagnostics struct {
	Rows       []HostRow
	TopFailing []HostRow
}

// HostRow is the rendering view for one PDS host bucket.
type HostRow struct {
	Host             string
	Total            uint64
	Active           uint64
	NotStarted       uint64
	Pending          uint64
	Complete         uint64
	Failed           uint64
	Unavailable      uint64
	LastAttemptedAt  time.Time
	LatestError      string
	LatestErrorClass string
	ErrorClassCounts map[string]uint64
	RecentErrors     []HostErrorRow
}

// HostErrorRow is one bounded recent error sample for a host bucket.
type HostErrorRow struct {
	DID         string
	AttemptedAt time.Time
	Class       string
	Error       string
}

// AccountLookup is the account diagnostics panel for one DID or handle query.
type AccountLookup struct {
	Query           string
	QueryKind       string
	Found           bool
	Error           string
	DID             string
	Handle          string
	PDS             string
	Host            string
	Active          bool
	Backfill        string
	Attempts        int
	LastError       string
	BackfillRev     string
	LatestRev       string
	UpdatedAt       time.Time
	LastAttemptedAt time.Time
	RecordCount     int64
	TotalBytes      int64
}

// LiveStats summarizes the upstream cursor and seq counters.
type LiveStats struct {
	UpstreamCursor                  int64
	NextSeq                         uint64
	BootstrapSeq                    uint64
	LastSeenUpstreamEventAt         time.Time
	LastSeenUpstreamEventAge        time.Duration
	LastSeenUpstreamEventAgeSeconds *int64 `json:"lastSeenUpstreamEventAgeSeconds"`
}

// PebbleStats summarizes meta.pebble/ on disk plus per-prefix key counts.
type PebbleStats struct {
	DiskBytes      int64
	KeyspaceCounts map[string]uint64
}

// CursorLookbackStats summarizes cursor replay observability.
type CursorLookbackStats struct {
	ConfiguredLookback   time.Duration
	ManifestSegmentCount int
	OldestRetainedSeq    uint64
	OldestRetainedAt     time.Time
}

// SegmentSummary mirrors segment.Inspection's user-facing fields.
type SegmentSummary struct {
	Index           uint64
	Sealed          bool
	EventCount      uint64
	UniqueDIDCount  uint32
	BlockCount      uint32
	CollectionCount int
	MinSeq          uint64
	MaxSeq          uint64
	MinWitnessedAt  time.Time
	MaxWitnessedAt  time.Time
	SizeBytes       int64
}
