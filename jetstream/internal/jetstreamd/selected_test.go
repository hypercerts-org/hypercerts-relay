package jetstreamd

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

func TestSelectedPolicySnapshot(t *testing.T) {
	first := selection.Policy{Revision: 1, Collections: []string{"app.example.one"}}
	second := selection.Policy{Revision: 2, Collections: []string{"app.example.two"}}
	boundary := uint64(7)
	firstHistory := []attribution.Policy{{Revision: 1, Collections: first.Collections}}
	secondHistory := []attribution.Policy{firstHistory[0], {Revision: 2, Collections: second.Collections, EffectiveAfterSeq: &boundary}}

	t.Run("stable initial nil boundary", func(t *testing.T) {
		reads := 0
		policy, history, err := selectedPolicySnapshot(func() selection.Policy { return first }, func() ([]attribution.Policy, error) { reads++; return firstHistory, nil })
		require.NoError(t, err)
		require.Equal(t, first, policy)
		require.Nil(t, history[0].EffectiveAfterSeq)
		require.Equal(t, 1, reads)
	})
	t.Run("change retries with coherent history", func(t *testing.T) {
		currentReads, historyReads := 0, 0
		policy, history, err := selectedPolicySnapshot(func() selection.Policy {
			currentReads++
			if currentReads == 1 {
				return first
			}
			return second
		}, func() ([]attribution.Policy, error) {
			historyReads++
			if historyReads == 1 {
				return firstHistory, nil
			}
			return secondHistory, nil
		})
		require.NoError(t, err)
		require.Equal(t, second, policy)
		require.Equal(t, 4, currentReads)
		require.Equal(t, 2, historyReads)
		require.Equal(t, &boundary, history[len(history)-1].EffectiveAfterSeq)
		gaps := subscribe.PolicyGaps(history)
		require.Len(t, gaps, 1)
		require.EqualValues(t, 8, *gaps[0].FromSeq)
		require.Equal(t, "app.example.one", *gaps[0].Collection)
	})
	t.Run("unstable exhausts three attempts", func(t *testing.T) {
		currentReads, historyReads := 0, 0
		_, _, err := selectedPolicySnapshot(func() selection.Policy {
			currentReads++
			if currentReads%2 == 1 {
				return first
			}
			return second
		}, func() ([]attribution.Policy, error) { historyReads++; return firstHistory, nil })
		require.Error(t, err)
		require.Equal(t, 6, currentReads)
		require.Equal(t, 3, historyReads)
	})
	for name, history := range map[string][]attribution.Policy{
		"missing":               nil,
		"older revision":        firstHistory,
		"different collections": {{Revision: 2, Collections: first.Collections}},
		"newer revision":        append(secondHistory, attribution.Policy{Revision: 3, Collections: second.Collections}),
	} {
		t.Run(name, func(t *testing.T) {
			reads := 0
			_, _, err := selectedPolicySnapshot(func() selection.Policy { return second }, func() ([]attribution.Policy, error) { reads++; return history, nil })
			require.Error(t, err)
			require.Equal(t, 3, reads)
		})
	}
	t.Run("same revision changed collections", func(t *testing.T) {
		reads := 0
		_, _, err := selectedPolicySnapshot(func() selection.Policy {
			reads++
			if reads%2 == 1 {
				return second
			}
			return selection.Policy{Revision: second.Revision, Collections: first.Collections}
		}, func() ([]attribution.Policy, error) { return secondHistory, nil })
		require.Error(t, err)
		require.Equal(t, 6, reads)
	})
	t.Run("history error propagates immediately", func(t *testing.T) {
		failure := errors.New("history read failed")
		currentReads, historyReads := 0, 0
		_, _, err := selectedPolicySnapshot(func() selection.Policy { currentReads++; return first }, func() ([]attribution.Policy, error) { historyReads++; return nil, failure })
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, currentReads)
		require.Equal(t, 1, historyReads)
	})
}
