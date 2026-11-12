package jetstreamd

import (
	"errors"
	"slices"

	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
)

func selectedPolicySnapshot(current func() selection.Policy, readHistory func() ([]attribution.Policy, error)) (selection.Policy, []attribution.Policy, error) {
	for attempt := 0; attempt < 3; attempt++ {
		before := current()
		history, err := readHistory()
		if err != nil {
			return selection.Policy{}, nil, err
		}
		after := current()
		if before.Revision != after.Revision || !slices.Equal(before.Collections, after.Collections) || len(history) == 0 {
			continue
		}
		latest := history[len(history)-1]
		if latest.Revision == after.Revision && slices.Equal(latest.Collections, after.Collections) {
			return after, history, nil
		}
	}
	return selection.Policy{}, nil, errors.New("selected stream policy history is not consistent")
}
