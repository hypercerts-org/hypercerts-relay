package status_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

type fixedCoverage struct {
	info  status.CoverageInfo
	ok    bool
	calls int
}

func (f *fixedCoverage) CurrentCoverage() (status.CoverageInfo, bool) { f.calls++; return f.info, f.ok }

func TestCollect_CoverageReporterAndNullableCounters(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	zero := status.CoverageCount{Known: true, Value: 0}
	reporter := &fixedCoverage{ok: true, info: status.CoverageInfo{Scope: "enabled_sources_current_policy", Sources: []status.CoverageSource{{State: "unknown", Counts: status.CoverageCounts{Scanned: zero}, HistoryState: "unknown", LiveState: "unknown"}}}}
	c, err := status.New(status.Options{Store: st, DataDir: dataDir, Now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }, CoverageReporter: reporter})
	require.NoError(t, err)
	a, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	b, err := c.SnapshotForRequest(context.Background(), status.Request{Tab: "summary"})
	require.NoError(t, err)
	require.Equal(t, 2, reporter.calls)
	require.NotNil(t, a.Coverage)
	require.Equal(t, a.Coverage, b.Coverage)
	data, err := json.Marshal(a.Coverage)
	require.NoError(t, err)
	require.JSONEq(t, `{"schemaVersion":0,"scope":"enabled_sources_current_policy","policy":{"revision":0,"collections":null},"aggregate":{"scope":"","policyRevision":0,"enabledSources":0,"state":""},"sources":[{"pds":"","sourceRevision":0,"policyRevision":0,"collections":null,"matchingJob":false,"state":"unknown","progressScope":"","counts":{"initialInventory":null,"scanned":0,"matching":null,"noMatch":null,"unresolved":null,"attributableRecords":null},"attempts":0,"lastProgressAt":null,"retryAction":"","historyState":"unknown","liveState":"unknown"}]}`, string(data))
	require.NotContains(t, string(data), `"Known"`)
	require.NotContains(t, string(data), `"Value"`)
}
