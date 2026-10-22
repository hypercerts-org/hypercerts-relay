package jetstreamd

import (
	"slices"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/coverage"
	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/status"
)

type statusInputsProvider interface {
	StatusInputs() (map[string]bool, map[string]uint64, selection.Policy, []jobs.Job)
}

type coverageReporter struct{ source statusInputsProvider }

func (r coverageReporter) CurrentCoverage() (status.CoverageInfo, bool) {
	if r.source == nil {
		return status.CoverageInfo{}, false
	}
	if manager, ok := r.source.(*jobs.Manager); ok && manager == nil {
		return status.CoverageInfo{}, false
	}
	enabled, revisions, policy, history := r.source.StatusInputs()
	projected := coverage.New(enabled, revisions, policy, history)
	out := status.CoverageInfo{
		SchemaVersion: projected.SchemaVersion, Scope: projected.Scope,
		Policy:    status.CoveragePolicy{Revision: projected.Policy.Revision, Collections: slices.Clone(projected.Policy.Collections)},
		Aggregate: status.CoverageAggregate{Scope: projected.Aggregate.Scope, PolicyRevision: projected.Aggregate.Policy.Revision, EnabledSources: projected.Aggregate.EnabledSources, State: projected.Aggregate.State},
		Sources:   make([]status.CoverageSource, 0, len(projected.Items)),
	}
	for _, row := range projected.Items {
		a := row.Acquisition
		counts := status.CoverageCounts{InitialInventory: coverageCount(a.Counts.InitialInventory), Scanned: coverageCount(a.Counts.Scanned), Matching: coverageCount(a.Counts.Matching), NoMatch: coverageCount(a.Counts.NoMatch), Unresolved: coverageCount(a.Counts.Unresolved), AttributableRecords: coverageCount(a.Counts.AttributableRecords)}
		category := ""
		switch a.Diagnostics.FailureCategory {
		case "source_unavailable", "verification_failed", "":
			category = a.Diagnostics.FailureCategory
		default:
			category = "unknown_failure"
		}
		retry := "unknown"
		if a.Diagnostics.RetryAction == "explicit_retry" {
			retry = "explicit_retry"
		}
		var progress *time.Time
		if a.Diagnostics.LastProgressAt != nil {
			t := *a.Diagnostics.LastProgressAt
			progress = &t
		}
		out.Sources = append(out.Sources, status.CoverageSource{
			PDS: row.PDS, SourceRevision: row.SourceRevision,
			PolicyRevision: row.Policy.Revision, Collections: slices.Clone(row.Policy.Collections), MatchingJob: row.MatchingJob,
			State: string(a.State), ProgressScope: a.ProgressScope, Counts: counts,
			FailureCategory: category, Attempts: a.Diagnostics.Attempts, LastProgressAt: progress, RetryAction: retry,
			HistoryState: row.History.State, LiveState: row.Live.State,
		})
	}
	return out, true
}

func coverageCount(v *int) status.CoverageCount {
	if v == nil {
		return status.CoverageCount{}
	}
	return status.CoverageCount{Known: true, Value: *v}
}
