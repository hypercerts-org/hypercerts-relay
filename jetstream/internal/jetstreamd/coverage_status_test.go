package jetstreamd

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

type fakeCoverageInputs struct {
	calls     int
	enabled   map[string]bool
	revisions map[string]uint64
	policy    selection.Policy
	history   []jobs.Job
}

func (f *fakeCoverageInputs) StatusInputs() (map[string]bool, map[string]uint64, selection.Policy, []jobs.Job) {
	f.calls++
	return f.enabled, f.revisions, f.policy, f.history
}

func TestCoverageReporterUsesSharedProjectionAndSafeDTO(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	zero := 0
	inputs := &fakeCoverageInputs{
		enabled: map[string]bool{"https://pds.example": true}, revisions: map[string]uint64{"https://pds.example": 4},
		policy: selection.Policy{Revision: 9, Collections: []string{"app.certified.actor.profile"}},
		history: []jobs.Job{{ID: "private-job-sentinel", PDS: "https://pds.example", SourceRevision: 4,
			Policy: selection.Policy{Revision: 9, Collections: []string{"app.certified.actor.profile"}}, State: jobs.Complete,
			FailureRepository: "did:plc:private-sentinel", ErrorCode: "private-error-sentinel", TotalRepos: 0, TotalReposKnown: true,
			Evidence: &jobs.AcquisitionEvidence{Scanned: zero, Matching: zero, NoMatch: zero, AttributableRecords: zero, LastProgressAt: now}}},
	}
	got, ok := (coverageReporter{source: inputs}).CurrentCoverage()
	require.True(t, ok)
	require.Equal(t, 1, inputs.calls)
	require.Equal(t, "enabled_sources_current_policy", got.Scope)
	require.Len(t, got.Sources, 1)
	require.True(t, got.Sources[0].Counts.Scanned.Known)
	require.Zero(t, got.Sources[0].Counts.Scanned.Value)
	require.Equal(t, "unknown", got.Sources[0].HistoryState)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{"private-job-sentinel", "private-sentinel", "private-error-sentinel", "affectedRepository", "receiptRetryAt", "cursor"} {
		require.NotContains(t, string(encoded), secret)
	}
	require.NotContains(t, strings.ToLower(string(encoded)), "jobid")
}

func TestCoverageReporterCopiesAndProjectsCurrentInputs(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	progress := now
	inputs := &fakeCoverageInputs{
		enabled:   map[string]bool{"https://stale.example": true, "https://disabled.example": false, "https://current.example": true},
		revisions: map[string]uint64{"https://stale.example": 8, "https://disabled.example": 2, "https://current.example": 4},
		policy:    selection.Policy{Revision: 9, Collections: []string{"app.certified.actor.profile"}},
		history: []jobs.Job{
			{ID: "stale-source", PDS: "https://stale.example", SourceRevision: 7, Policy: selection.Policy{Revision: 9, Collections: []string{"app.certified.actor.profile"}}, State: jobs.Complete, CreatedAt: now.Add(time.Hour)},
			{ID: "old-policy", PDS: "https://current.example", SourceRevision: 4, Policy: selection.Policy{Revision: 8, Collections: []string{"app.certified.actor.profile"}}, State: jobs.Complete, CreatedAt: now.Add(time.Hour)},
			{ID: "current-old", PDS: "https://current.example", SourceRevision: 4, Policy: selection.Policy{Revision: 9, Collections: []string{"app.certified.actor.profile"}}, State: jobs.Failed, CreatedAt: now.Add(time.Hour), AcquisitionGeneration: 1},
			{ID: "current", PDS: "https://current.example", SourceRevision: 4, Policy: selection.Policy{Revision: 9, Collections: []string{"app.certified.actor.profile"}}, State: jobs.Complete, CreatedAt: now.Add(-time.Hour), AcquisitionGeneration: 2, Evidence: &jobs.AcquisitionEvidence{Scanned: 0, Matching: 0, NoMatch: 0, AttributableRecords: 0, LastProgressAt: progress}},
		},
	}
	got, ok := (coverageReporter{source: inputs}).CurrentCoverage()
	require.True(t, ok)
	require.Len(t, got.Sources, 2)
	var current *status.CoverageSource
	for i := range got.Sources {
		if got.Sources[i].PDS == "https://current.example" {
			current = &got.Sources[i]
		}
	}
	require.NotNil(t, current)
	require.Equal(t, uint64(9), current.PolicyRevision)
	require.Equal(t, "", current.FailureCategory)
	require.Equal(t, "complete", current.State)
	require.True(t, current.Counts.Scanned.Known)
	require.Equal(t, 0, current.Counts.Scanned.Value)
	require.Equal(t, "unknown", current.HistoryState)
	require.Equal(t, "unknown", current.LiveState)
	got.Policy.Collections[0] = "mutated"
	current.Collections[0] = "mutated"
	current.LastProgressAt = nil
	require.Equal(t, "app.certified.actor.profile", inputs.policy.Collections[0])
	require.Equal(t, "app.certified.actor.profile", inputs.history[2].Policy.Collections[0])
	require.Equal(t, progress, inputs.history[3].Evidence.LastProgressAt)
}

func intPtrCoverage(v int) *int { return &v }

func TestCoverageReporterUsesRealManager(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	policy, err := selection.Open(db, []string{"app.certified.actor.profile"})
	require.NoError(t, err)
	manager, err := jobs.Open(db, policy)
	require.NoError(t, err)
	_, err = manager.AddSource("https://pds.example")
	require.NoError(t, err)

	got, ok := (coverageReporter{source: manager}).CurrentCoverage()
	require.True(t, ok)
	require.Len(t, got.Sources, 1)
	require.Equal(t, "https://pds.example", got.Sources[0].PDS)
	require.Equal(t, uint64(1), got.Policy.Revision)
	require.Equal(t, "running", got.Sources[0].State)
	require.Equal(t, "enabled_sources_current_policy", got.Scope)
}

func TestCoverageReporterUnavailable(t *testing.T) {
	got, ok := (coverageReporter{}).CurrentCoverage()
	require.False(t, ok)
	require.Empty(t, got.Sources)
}
