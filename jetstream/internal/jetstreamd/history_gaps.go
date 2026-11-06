package jetstreamd

import (
	"sort"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/subscribe"
)

func selectedPolicyGaps(state history.PolicyState, tip uint64) []subscribe.SelectedGap {
	byCollection := make(map[string][]history.PolicyInterval)
	for _, interval := range state.Intervals {
		byCollection[interval.Collection] = append(byCollection[interval.Collection], interval)
	}
	collections := make([]string, 0, len(byCollection))
	for collection := range byCollection {
		collections = append(collections, collection)
	}
	sort.Strings(collections)
	gaps := make([]subscribe.SelectedGap, 0)
	for _, collection := range collections {
		intervals := byCollection[collection]
		sort.Slice(intervals, func(i, j int) bool {
			if intervals[i].FromJournalSequence == intervals[j].FromJournalSequence {
				return intervals[i].Revision < intervals[j].Revision
			}
			return intervals[i].FromJournalSequence < intervals[j].FromJournalSequence
		})
		for i, interval := range intervals {
			if interval.ThroughJournalSequence == nil && interval.ClosedAt == 0 {
				continue
			}
			from := interval.FromJournalSequence
			if interval.ThroughJournalSequence == nil && interval.ClosedAt != 0 {
				from = interval.FromJournalSequence
				if i > 0 && intervals[i-1].ThroughJournalSequence != nil {
					from = *intervals[i-1].ThroughJournalSequence + 1
				}
			}
			if interval.ThroughJournalSequence != nil {
				from = *interval.ThroughJournalSequence + 1
			} else if i+1 < len(intervals) {
				continue
			}
			var through *uint64
			if i+1 < len(intervals) {
				value := intervals[i+1].FromJournalSequence - 1
				if value < from {
					continue
				}
				through = &value
			} else {
				through = nil
			}
			name := collection
			gaps = append(gaps, subscribe.SelectedGap{FromSeq: &from, ThroughSeq: through, Reason: "policy-disabled", Collection: &name})
		}
	}
	return gaps
}
