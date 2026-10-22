package coverage

import (
	"github.com/bluesky-social/jetstream/internal/hypercerts/jobs"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCurrentPolicyCoordinatesAndAggregate(t *testing.T) {
	policy := selection.Policy{Revision: 2, Collections: []string{"app.bsky.feed.post"}}
	complete := jobs.Job{ID: "current", PDS: "https://a.example", SourceRevision: 3, Policy: policy, State: jobs.Complete, TotalReposKnown: true, TotalRepos: 2, Evidence: &jobs.AcquisitionEvidence{Scanned: 2, NoMatch: 2}}
	stale := complete
	stale.ID = "newer-stale"
	stale.Policy.Revision = 1
	stale.CreatedAt = time.Now()
	sourceStale := stale
	sourceStale.Policy = policy
	sourceStale.SourceRevision = 2
	for _, tc := range []struct {
		name    string
		enabled map[string]bool
		history []jobs.Job
		state   string
	}{
		{"no sources", map[string]bool{}, nil, "unknown"},
		{"complete", map[string]bool{"https://a.example": true}, []jobs.Job{complete, stale, sourceStale}, "complete"},
		{"missing current", map[string]bool{"https://a.example": true}, []jobs.Job{stale, sourceStale}, "unknown"},
		{"missing source evidence", map[string]bool{"https://a.example": true, "https://b.example": true}, []jobs.Job{complete}, "unknown"},
		{"disabled ignored", map[string]bool{"https://a.example": true, "https://b.example": false}, []jobs.Job{complete}, "complete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := New(tc.enabled, map[string]uint64{"https://a.example": 3}, policy, tc.history)
			require.Equal(t, tc.state, snapshot.Aggregate.State)
			for _, row := range snapshot.Items {
				require.Equal(t, "unknown", row.History.State)
				require.Equal(t, "unknown", row.Live.State)
				require.Equal(t, policy, row.Policy)
			}
			if tc.name == "complete" {
				require.Equal(t, "current", snapshot.Items[0].JobID)
				require.Equal(t, 2, *snapshot.Items[0].Acquisition.Counts.NoMatch)
				require.Zero(t, *snapshot.Items[0].Acquisition.Counts.Matching)
			}
		})
	}
	for _, state := range []jobs.State{jobs.Pending, jobs.Running, jobs.Incomplete, jobs.Failed, jobs.Canceled} {
		second := complete
		second.PDS = "https://b.example"
		second.SourceRevision = 0
		second.State = state
		snapshot := New(map[string]bool{complete.PDS: true, second.PDS: true}, map[string]uint64{complete.PDS: 3}, policy, []jobs.Job{complete, second})
		require.Equal(t, "incomplete", snapshot.Aggregate.State)
	}
}
func TestOldJobAttributionStaysUnknown(t *testing.T) {
	a := JobAcquisition(jobs.Job{State: jobs.Complete, TotalReposKnown: true, TotalRepos: 1, CompletedRepos: map[string]string{"did:plc:a": "rev"}})
	require.Equal(t, Complete, a.State)
	require.Equal(t, 1, *a.Counts.InitialInventory)
	require.Nil(t, a.Counts.Scanned)
	require.Nil(t, a.Counts.Matching)
	require.Nil(t, a.Counts.NoMatch)
	require.Nil(t, a.Counts.Unresolved)
	require.Nil(t, a.Counts.AttributableRecords)
	require.Nil(t, a.Diagnostics.LastProgressAt)
}

func TestDiagnosticsBoundRawEvidence(t *testing.T) {
	a := JobAcquisition(jobs.Job{State: jobs.Failed, ErrorCode: "raw credential=do-not-expose", FailureRepository: "untrusted CAR payload"})
	require.Equal(t, "unknown_failure", a.Diagnostics.FailureCategory)
	require.Empty(t, a.Diagnostics.AffectedRepository)
	require.Equal(t, "explicit_retry", a.Diagnostics.RetryAction)
}

func TestRetriedOlderJobSupersedesNewerCreatedCompletion(t *testing.T) {
	policy := selection.Policy{Revision: 1, Collections: []string{}}
	origin := "https://pds.example"
	created := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	newer := jobs.Job{ID: "newer", PDS: origin, Policy: policy, State: jobs.Complete, CreatedAt: created.Add(time.Hour), AcquisitionGeneration: 2}
	older := jobs.Job{ID: "older", PDS: origin, Policy: policy, CreatedAt: created, AcquisitionGeneration: 3}
	for _, state := range []jobs.State{jobs.Pending, jobs.Running, jobs.Incomplete, jobs.Failed, jobs.Complete} {
		older.State = state
		snapshot := New(map[string]bool{origin: true}, nil, policy, []jobs.Job{newer, older})
		require.Equal(t, older.ID, snapshot.Items[0].JobID, "explicit retry cannot hide behind creation time")
		expected := "incomplete"
		if state == jobs.Complete {
			expected = "complete"
		}
		require.Equal(t, expected, snapshot.Aggregate.State)
	}
	superseding := newer
	superseding.AcquisitionGeneration = 4
	older.State = jobs.Failed
	snapshot := New(map[string]bool{origin: true}, nil, policy, []jobs.Job{older, superseding})
	require.Equal(t, superseding.ID, snapshot.Items[0].JobID)
	require.Equal(t, "complete", snapshot.Aggregate.State)
	for _, generation := range []uint64{0, 5} {
		a, b := newer, newer
		a.ID = "a"
		b.ID = "b"
		a.AcquisitionGeneration = generation
		b.AcquisitionGeneration = generation
		first := New(map[string]bool{origin: true}, nil, policy, []jobs.Job{a, b})
		second := New(map[string]bool{origin: true}, nil, policy, []jobs.Job{b, a})
		require.Equal(t, "b", first.Items[0].JobID)
		require.Equal(t, first, second)
	}
}

func TestAcquisitionDiagnosticSnapshots(t *testing.T) {
	deadline := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	job := jobs.Job{ID: "job", PDS: "https://pds.example", SourceRevision: 3, Policy: selection.Policy{Revision: 4}, AcquisitionGeneration: 5, State: jobs.Pending}
	waiting := jobs.JobDiagnosticSnapshot{Job: job, Diagnostics: jobs.JobDiagnostics{Execution: jobs.JobExecutionWaiting, Reason: "repository_retry", RetryAt: &deadline}}
	for _, tc := range []struct {
		name      string
		state     jobs.State
		execution jobs.JobExecution
		expected  AcquisitionState
	}{
		{"waiting", jobs.Pending, jobs.JobExecutionWaiting, RetryWaiting},
		{"ready", jobs.Pending, jobs.JobExecutionQueued, Running},
		{"running", jobs.Running, jobs.JobExecutionWaiting, Running},
		{"incomplete", jobs.Incomplete, jobs.JobExecutionWaiting, StoppedIncomplete},
		{"failed", jobs.Failed, jobs.JobExecutionWaiting, Failed},
		{"canceled", jobs.Canceled, jobs.JobExecutionWaiting, Canceled},
		{"complete", jobs.Complete, jobs.JobExecutionWaiting, Complete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := job
			j.State = tc.state
			snap := waiting
			snap.Job = j
			snap.Diagnostics.Execution = tc.execution
			a := JobAcquisition(j, snap)
			require.Equal(t, tc.expected, a.State)
			if tc.expected == RetryWaiting {
				require.Equal(t, "automatic_retry", a.Diagnostics.RetryAction)
				require.Equal(t, &deadline, a.Diagnostics.RetryAt)
				require.Equal(t, "repository_retry", a.Diagnostics.WaitReason)
			} else {
				require.Nil(t, a.Diagnostics.RetryAt)
				require.Empty(t, a.Diagnostics.WaitReason)
			}
		})
	}
	require.Equal(t, Running, JobAcquisition(job).State)
	for _, change := range []func(*jobs.Job){func(j *jobs.Job) { j.ID = "other" }, func(j *jobs.Job) { j.PDS = "https://other.example" }, func(j *jobs.Job) { j.SourceRevision++ }, func(j *jobs.Job) { j.Policy.Revision++ }, func(j *jobs.Job) { j.AcquisitionGeneration++ }} {
		snap := waiting
		change(&snap.Job)
		require.Equal(t, Running, JobAcquisition(job, snap).State)
	}
	for _, both := range []bool{false, true} {
		snap := waiting
		snap.Diagnostics.Reason = "pds_cooldown"
		snap.Diagnostics.PDSCooldownUntil = &deadline
		if !both {
			snap.Diagnostics.RetryAt = nil
		}
		a := JobAcquisition(job, snap)
		require.Equal(t, RetryWaiting, a.State)
		require.Equal(t, &deadline, a.Diagnostics.PDSCooldownUntil)
		require.Equal(t, "pds_cooldown", a.Diagnostics.WaitReason)
		if !both {
			require.Nil(t, a.Diagnostics.RetryAt)
		}
	}
	waiting.Diagnostics.Reason = "raw response body"
	waiting.Diagnostics.RetryAt = nil
	a := JobAcquisition(job, waiting)
	require.Empty(t, a.Diagnostics.WaitReason)
	require.Nil(t, a.Diagnostics.RetryAt)
	snapshot := New(map[string]bool{job.PDS: true}, map[string]uint64{job.PDS: 3}, job.Policy, []jobs.Job{job}, waiting)
	require.Equal(t, RetryWaiting, snapshot.Items[0].Acquisition.State)
	require.Equal(t, "incomplete", snapshot.Aggregate.State)
}

func TestClassifiedFailuresAndLatestDurableProgress(t *testing.T) {
	early := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	late := early.Add(time.Hour)
	job := jobs.Job{State: jobs.Incomplete, ErrorCode: "source_unavailable", CreatedAt: late.Add(time.Hour), LastProgressAt: &early, LastFailure: &jobs.JobFailure{Category: "http", FailedAt: late.Add(2 * time.Hour)}}
	a := JobAcquisition(job)
	require.Equal(t, "http", a.Diagnostics.FailureCategory)
	require.Equal(t, &early, a.Diagnostics.LastProgressAt)
	require.Nil(t, a.Counts.Scanned)
	job.Evidence = &jobs.AcquisitionEvidence{LastProgressAt: late}
	require.Equal(t, &late, JobAcquisition(job).Diagnostics.LastProgressAt)
	job.LastProgressAt = &late
	job.Evidence.LastProgressAt = early
	require.Equal(t, &late, JobAcquisition(job).Diagnostics.LastProgressAt)
	job.LastFailure.Category = "raw credential data"
	require.Equal(t, "source_unavailable", JobAcquisition(job).Diagnostics.FailureCategory)
	job.LastFailure.Category = "invented_category"
	require.Equal(t, "source_unavailable", JobAcquisition(job).Diagnostics.FailureCategory)
	job.LastProgressAt = &time.Time{}
	job.Evidence = nil
	require.Nil(t, JobAcquisition(job).Diagnostics.LastProgressAt)
}
