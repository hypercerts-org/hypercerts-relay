package subscribe

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/stretchr/testify/require"
)

func TestPolicyGapsDiscardSameBoundaryIntervals(t *testing.T) {
	for _, boundary := range []*uint64{nil, ptrBoundary(0), ptrBoundary(8)} {
		history := []attribution.Policy{
			{Revision: 1, Collections: []string{"app.example.a"}},
			{Revision: 2, EffectiveAfterSeq: boundary},
			{Revision: 3, Collections: []string{"app.example.a"}, EffectiveAfterSeq: boundary},
		}
		require.Empty(t, PolicyGaps(history))
		history = append(history, attribution.Policy{Revision: 4, EffectiveAfterSeq: ptrBoundary(12)}, attribution.Policy{Revision: 5, Collections: []string{"app.example.a"}, EffectiveAfterSeq: ptrBoundary(14)})
		gaps := PolicyGaps(history)
		require.Len(t, gaps, 1)
		require.EqualValues(t, 13, *gaps[0].FromSeq)
		require.EqualValues(t, 14, *gaps[0].ThroughSeq)
	}
}

func ptrBoundary(value uint64) *uint64 { return &value }
