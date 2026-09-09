package relay

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRatePolicyPersistsAndEnforcesBothScopes(t *testing.T) {
	r, _ := newSourceRelay(t)
	require.NoError(t, r.loadRatePolicies())
	view, err := r.AddSource(t.Context(), "https://rate.example")
	require.NoError(t, err)
	require.NotNil(t, view)
	_, err = r.SetRatePolicy(t.Context(), "global", 2)
	require.NoError(t, err)
	_, err = r.SetRatePolicy(t.Context(), "https://rate.example", 1)
	require.NoError(t, err)
	require.NoError(t, r.waitRateCapacity(t.Context(), "rate.example"))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.waitRateCapacity(ctx, "rate.example"), context.DeadlineExceeded)
	// A different source still has global capacity, but cannot exceed the global burst.
	require.NoError(t, r.waitRateCapacity(t.Context(), "other.example"))
	ctx2, cancel2 := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel2()
	require.ErrorIs(t, r.waitRateCapacity(ctx2, "other.example"), context.DeadlineExceeded)
	require.NoError(t, r.loadRatePolicies())
	rows, _, err := r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	_, err = r.SetRatePolicy(t.Context(), "global", 0)
	require.ErrorIs(t, err, ErrInvalidRatePolicy)
	rows, _, err = r.ListRatePolicies(t.Context(), "")
	require.NoError(t, err)
	require.Equal(t, int64(2), rows[0].EventsPerSecond)
}
