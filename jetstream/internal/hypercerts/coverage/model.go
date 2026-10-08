// Package coverage projects current-policy evidence; it owns no mutable store.
package coverage

import (
	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/jcalabro/atmos"
	"sort"
	"strings"
	"time"
)

// Counts use null for absent durable evidence. InitialInventory is the frozen
// active repository census; Unresolved includes every inventory member without
// a successful verified scan (including permanent invalid inputs).
type Counts struct {
	InitialInventory    *int `json:"initialInventory"`
	Scanned             *int `json:"scanned"`
	Matching            *int `json:"matching"`
	NoMatch             *int `json:"noMatch"`
	Unresolved          *int `json:"unresolved"`
	AttributableRecords *int `json:"attributableRecords"`
}
type Diagnostics struct {
	FailureCategory    string     `json:"failureCategory,omitempty"`
	LastProgressAt     *time.Time `json:"lastProgressAt"`
	Attempts           int        `json:"attempts"`
	AffectedRepository string     `json:"affectedRepository,omitempty"`
	RetryAction        string     `json:"retryAction,omitempty"`
}

// IndependentEvidence deliberately makes no history or PDS-live claim from
// snapshot completion, global bootstrap metrics, or a stream cursor.
type IndependentEvidence struct {
	State string `json:"state"`
}

// AcquisitionState is independent of durable execution State. RetryWaiting is
// reserved until the job layer supplies a durable acquisition retry schedule.
type AcquisitionState string

const (
	Unknown           AcquisitionState = "unknown"
	Running           AcquisitionState = "running"
	RetryWaiting      AcquisitionState = "retry_waiting"
	StoppedIncomplete AcquisitionState = "stopped_incomplete"
	Failed            AcquisitionState = "failed"
	Canceled          AcquisitionState = "canceled"
	Complete          AcquisitionState = "complete"
)

type Acquisition struct {
	State         AcquisitionState `json:"state"`
	Counts        Counts           `json:"counts"`
	Diagnostics   Diagnostics      `json:"diagnostics"`
	ProgressScope string           `json:"progressScope"`
}

// LegacyProgress preserves execution-history fields for existing consumers.
// Numeric fields are null without a matching job; these are not attribution counts.
type LegacyProgress struct {
	Reason          string     `json:"reason"`
	State           jobs.State `json:"state"`
	CompletedRepos  *int       `json:"completedRepos"`
	TotalRepos      *int       `json:"totalRepos"`
	TotalReposKnown bool       `json:"totalReposKnown"`
	ErrorCode       string     `json:"errorCode,omitempty"`
	CreatedAt       time.Time  `json:"createdAt"`
	Coverage        string     `json:"coverage"`
}
type Source struct {
	*LegacyProgress

	PDS            string              `json:"pds"`
	SourceRevision uint64              `json:"sourceRevision"`
	Policy         selection.Policy    `json:"policy"`
	MatchingJob    bool                `json:"matchingJob"`
	JobID          string              `json:"jobId,omitempty"`
	Acquisition    Acquisition         `json:"acquisition"`
	History        IndependentEvidence `json:"history"`
	Live           IndependentEvidence `json:"live"`
}
type Aggregate struct {
	Scope          string           `json:"scope"`
	Policy         selection.Policy `json:"policy"`
	EnabledSources int              `json:"enabledSources"`
	State          string           `json:"state"`
}

// Snapshot schema version 1 is scoped to enabled sources and current policy.
// Aggregate always covers all enabled sources, regardless of HTTP pagination.
type Snapshot struct {
	SchemaVersion int              `json:"schemaVersion"`
	Scope         string           `json:"scope"`
	Policy        selection.Policy `json:"policy"`
	Items         []Source         `json:"items"`
	Aggregate     Aggregate        `json:"aggregate"`
	NextCursor    string           `json:"nextCursor,omitempty"`
}

func integer(n int) *int { return &n }
func JobAcquisition(j jobs.Job) Acquisition {
	a := Acquisition{State: "unknown", ProgressScope: "job_inventory", Diagnostics: Diagnostics{Attempts: j.Attempts, FailureCategory: boundedCategory(j.ErrorCode), AffectedRepository: safeRepository(j.FailureRepository)}}
	switch j.State {
	case jobs.Pending, jobs.Running:
		a.State = "running"
	case jobs.Incomplete:
		a.State = "stopped_incomplete"
		a.Diagnostics.RetryAction = "explicit_retry"
	case jobs.Failed:
		a.State = "failed"
		a.Diagnostics.RetryAction = "explicit_retry"
	case jobs.Canceled:
		a.State = "canceled"
	case jobs.Complete:
		a.State = "complete"
	}
	if j.TotalReposKnown {
		a.Counts.InitialInventory = integer(j.TotalRepos)
	}
	if e := j.Evidence; e != nil {
		a.Counts.Scanned = integer(e.Scanned)
		a.Counts.Matching = integer(e.Matching)
		a.Counts.NoMatch = integer(e.NoMatch)
		a.Counts.AttributableRecords = integer(e.AttributableRecords)
		if j.TotalReposKnown {
			a.Counts.Unresolved = integer(max(0, j.TotalRepos-e.Scanned))
		}
		if !e.LastProgressAt.IsZero() {
			t := e.LastProgressAt
			a.Diagnostics.LastProgressAt = &t
		}
	}
	return a
}
func boundedCategory(code string) string {
	if code == "" {
		return ""
	}
	if len(code) > 64 || strings.IndexFunc(code, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) >= 0 {
		return "unknown_failure"
	}
	return code
}
func safeRepository(did string) string {
	if len(did) > 2048 {
		return ""
	}
	if _, err := atmos.ParseDID(did); err != nil {
		return ""
	}
	return did
}

// New generations supersede all earlier attempts at the same coordinate.
// Legacy jobs without generations use existing durable attempt/progress times;
// ties are deterministic. No terminal failure permanently outranks later work.
func newerAcquisition(a, b jobs.Job) bool {
	if a.AcquisitionGeneration != b.AcquisitionGeneration {
		return a.AcquisitionGeneration > b.AcquisitionGeneration
	}
	acquisitionTime := func(j jobs.Job) time.Time {
		latest := j.CreatedAt
		if j.StartedAt.After(latest) {
			latest = j.StartedAt
		}
		if j.Evidence != nil && j.Evidence.LastProgressAt.After(latest) {
			latest = j.Evidence.LastProgressAt
		}
		return latest
	}
	at, bt := acquisitionTime(a), acquisitionTime(b)
	return at.After(bt) || (at.Equal(bt) && a.ID > b.ID)
}

func latestMatchingAcquisition(pds string, sourceRevision, policyRevision uint64, history []jobs.Job) *jobs.Job {
	var selected *jobs.Job
	for i := range history {
		j := &history[i]
		if j.PDS != pds || j.SourceRevision != sourceRevision || j.Policy.Revision != policyRevision {
			continue
		}
		if selected == nil || newerAcquisition(*j, *selected) {
			selected = j
		}
	}
	return selected
}

func New(enabled map[string]bool, revisions map[string]uint64, policy selection.Policy, history []jobs.Job) Snapshot {
	out := Snapshot{SchemaVersion: 1, Scope: "enabled_sources_current_policy", Policy: policy, Items: []Source{}}
	for pds, active := range enabled {
		if !active {
			continue
		}
		row := Source{LegacyProgress: &LegacyProgress{State: jobs.State("unknown"), Coverage: "current_state"}, PDS: pds, SourceRevision: revisions[pds], Policy: policy, Acquisition: Acquisition{State: "unknown", ProgressScope: "job_inventory"}, History: IndependentEvidence{State: "unknown"}, Live: IndependentEvidence{State: "unknown"}}
		selected := latestMatchingAcquisition(pds, revisions[pds], policy.Revision, history)
		if selected != nil {
			row.LegacyProgress = &LegacyProgress{Reason: selected.Reason, State: selected.State, CompletedRepos: integer(len(selected.CompletedRepos)), TotalRepos: integer(selected.TotalRepos), TotalReposKnown: selected.TotalReposKnown, ErrorCode: selected.ErrorCode, CreatedAt: selected.CreatedAt, Coverage: "current_state"}
			row.MatchingJob = true
			row.JobID = selected.ID
			row.Acquisition = JobAcquisition(*selected)
		}
		out.Items = append(out.Items, row)
	}
	sort.Slice(out.Items, func(i, j int) bool { return out.Items[i].PDS < out.Items[j].PDS })
	aggregate := Aggregate{Scope: out.Scope, Policy: policy, EnabledSources: len(out.Items), State: "complete"}
	unknown := len(out.Items) == 0
	for _, row := range out.Items {
		if !row.MatchingJob || row.Acquisition.State == "unknown" {
			unknown = true
		}
		if row.Acquisition.State != "complete" || (row.Acquisition.Counts.Unresolved != nil && *row.Acquisition.Counts.Unresolved > 0) {
			aggregate.State = "incomplete"
		}
	}
	if unknown {
		aggregate.State = "unknown"
	}
	out.Aggregate = aggregate
	return out
}
