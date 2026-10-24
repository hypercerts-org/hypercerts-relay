// merge_runner.go owns the per-source-segment
// drain loop. One goroutine, serial, no fan-out. Spec:
// specs/notes/2026-05-27-merge-phase-design.md §4.2.

package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/hypercerts/policygate"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
)

type mergeRunner struct {
	dst                       *ingest.Writer
	store                     *store.Store
	sourceDir                 string
	fs                        vfs.FS
	logger                    *slog.Logger
	metrics                   *Metrics
	crashInjector             crashpoint.Injector
	now                       func() time.Time // overridable for tests
	cache                     *repoStatusLookup
	sourceAttribution         *attribution.Manager
	sourceDomain              string
	sourceAttributionStreamID string
	destinationAttribution    *attribution.Manager
	destinationStreamID       string
	policyGate                *policygate.Gate
	collectionPolicy          *selection.Manager
}

// newMergeRunner builds a drain runner. injector is the test-only crash
// simulator (crashpoint.Injector); production callers thread
// o.cfg.CrashInjector, which is nil in production, making every
// simulateCrash checkpoint a no-op. Pass nil to disable injection.
func newMergeRunner(dst *ingest.Writer, st *store.Store, sourceDir string, fs vfs.FS, logger *slog.Logger, m *Metrics, injector crashpoint.Injector) *mergeRunner {
	r := &mergeRunner{dst: dst, store: st, sourceDir: sourceDir, fs: fs, logger: logger.With(slog.String("component", "orchestrator/merge")), metrics: m, crashInjector: injector, now: func() time.Time { return time.Now().UTC() }}
	r.cache = newRepoStatusLookup(st, m.incMergeDIDLookups)
	return r
}

func (r *mergeRunner) run(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		fromIdx, err := loadMergeCursor(r.store)
		if err != nil {
			return err
		}
		all, err := ingest.SegmentFilesFS(r.fs, r.sourceDir)
		if err != nil {
			return fmt.Errorf("orchestrator: merge: list source segments: %w", err)
		}
		var todo []ingest.SegmentFile
		expectIdx := fromIdx
		for _, sf := range all {
			if sf.Idx < fromIdx {
				continue
			}
			if sf.Idx != expectIdx {
				return fmt.Errorf("orchestrator: merge: source index gap: expected %d, got %d", expectIdx, sf.Idx)
			}
			todo = append(todo, sf)
			expectIdx++
		}
		for _, sf := range todo {
			if err := ctx.Err(); err != nil {
				return err
			}
			perDID, err := r.processSourceSegment(ctx, sf)
			if err != nil {
				return err
			}
			if err := r.simulateCrash(ctx, crashpoint.AfterMergeDstFlushBeforeSourceCommit); err != nil {
				return err
			}
			if err := commitSourceComplete(r.store, r.cache, sf.Idx+1, perDID, r.now()); err != nil {
				return err
			}
			r.metrics.incMergeSegmentsConsumed()
			r.metrics.addMergeRepoRevsUpdated(len(perDID))
		}
		return nil
	})
}

func (r *mergeRunner) simulateCrash(ctx context.Context, point crashpoint.Point) error {
	if r.crashInjector == nil {
		return nil
	}
	return r.crashInjector.SimulateCrash(ctx, point)
}

func sourcePolicyFrom(manager *attribution.Manager, source attribution.Event) ([]attribution.Policy, error) {
	if source.PolicyRev == nil {
		return nil, nil
	}
	policy, err := manager.LookupPolicy(*source.PolicyRev)
	if err != nil {
		return nil, err
	}
	return []attribution.Policy{policy}, nil
}

// processSourceSegment copies selected rows while preserving source evidence;
// the destination flush precedes the durable source cursor commit.
func (r *mergeRunner) processSourceSegment(ctx context.Context, sf ingest.SegmentFile) (map[string]string, error) {
	return obs.Span2(ctx, func(ctx context.Context) (map[string]string, error) {
		rd, err := segment.Open(segment.ReaderConfig{Path: sf.Path, FS: r.fs})
		if err != nil {
			return nil, fmt.Errorf("orchestrator: merge: open %s: %w", sf.Path, err)
		}
		defer func() { _ = rd.Close() }()
		perDID := make(map[string]string)
		for i := range int(rd.Header().BlockCount) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			events, err := rd.DecodeBlock(i)
			if err != nil {
				return nil, fmt.Errorf("orchestrator: merge: decode %s block %d: %w", sf.Path, i, err)
			}
			for j := range events {
				ev := &events[j]
				rs, lerr := r.cache.get(ev.DID)
				if lerr != nil {
					return nil, lerr
				}
				if !shouldKeep(ev, rs) {
					r.metrics.incMergeEventsDropped()
					continue
				}
				if r.sourceAttribution != nil && r.destinationAttribution != nil {
					original := *ev
					source, lookupErr := r.sourceAttribution.Lookup(original.Seq)
					legacyUnknown := errors.Is(lookupErr, attribution.ErrLegacyUnknown)
					if lookupErr != nil && !legacyUnknown {
						return nil, fmt.Errorf("orchestrator: merge: lookup source attribution seq %d: %w", original.Seq, lookupErr)
					}
					if legacyUnknown {
						source = attribution.Event{StreamID: r.sourceAttributionStreamID, Seq: original.Seq, Provenance: attribution.ProvenanceLegacyUnknown}
					}
					if len(source.Payload) == 0 {
						payload, err := json.Marshal(struct {
							Operation string `json:"operation"`
						}{Operation: fmt.Sprint(original.Kind)})
						if err != nil {
							return nil, fmt.Errorf("orchestrator: merge: encode legacy source operation: %w", err)
						}
						source.Payload = payload
					}
					sourcePolicy, err := sourcePolicyFrom(r.sourceAttribution, source)
					if err != nil {
						return nil, fmt.Errorf("orchestrator: merge: lookup source policy: %w", err)
					}
					appendImported := func(destination ingest.AttributionMetadata) error {
						witnessedAt, indexedAt := ev.WitnessedAt, ev.IndexedAt
						if destination.Boundary.Seq == nil || destination.Boundary.SnapshotID == nil {
							boundary, id := r.dst.NextSeq(), "merge/reconciliation"
							destination.Boundary = attribution.Boundary{Seq: &boundary, SnapshotID: &id}
						}
						ev.WitnessedAt = r.now().UnixMicro()
						source.Source = &attribution.ImportedSource{WitnessedAt: witnessedAt, IndexedAt: indexedAt, SegmentFingerprint: attribution.FingerprintSegment(original)}
						return r.dst.AppendImported(ctx, ev, source, r.sourceDomain, destination, sourcePolicy...)
					}
					var appendErr error
					if r.collectionPolicy == nil {
						full, err := r.destinationAttribution.LookupPolicy(1)
						if err != nil {
							appendErr = err
						} else {
							appendErr = appendImported(ingest.AttributionMetadata{StreamID: r.destinationStreamID, Policy: full, Provenance: attribution.ProvenanceReconciliation})
						}
					} else if r.policyGate != nil {
						appendErr = r.policyGate.Admit(ctx, func(current selection.Policy) error {
							full, err := r.destinationAttribution.LookupPolicy(current.Revision)
							if err != nil {
								return err
							}
							return appendImported(ingest.AttributionMetadata{StreamID: r.destinationStreamID, Policy: full, Provenance: attribution.ProvenanceReconciliation})
						})
					} else {
						appendErr = fmt.Errorf("orchestrator: merge: managed policy gate unavailable")
					}
					if appendErr != nil {
						return nil, fmt.Errorf("orchestrator: merge: append imported: %w", appendErr)
					}
				} else {
					ev.WitnessedAt = r.now().UnixMicro()
					if err := r.dst.Append(ctx, ev); err != nil {
						return nil, fmt.Errorf("orchestrator: merge: append: %w", err)
					}
				}
				if ev.Seq != 0 {
					r.metrics.incMergeEventsKept()
					if isBackfillRevFilteredKind(ev.Kind) && ev.Rev != "" {
						perDID[ev.DID] = ev.Rev
					}
				}
			}
		}
		if err := r.dst.Flush(ctx); err != nil {
			return nil, fmt.Errorf("orchestrator: merge: flush dst: %w", err)
		}
		return perDID, nil
	})
}
