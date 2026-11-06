package ingest

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/attribution"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestWriterHistoryProjectionOwnershipIsIndependent(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()
	h, err := history.Open(db)
	require.NoError(t, err)
	mainAttr, err := attribution.Open(db, h.StreamID(), "owner-main")
	require.NoError(t, err)
	bootAttr, err := attribution.Open(db, h.StreamID(), "owner-boot")
	require.NoError(t, err)
	base := t.TempDir()
	open := func(name, seqKey string, target history.ProjectionTarget, attr *attribution.Manager, domain string) *Writer {
		w, openErr := Open(Config{SegmentsDir: filepath.Join(base, name), Store: db, SeqKey: seqKey, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: NewMetrics(prometheus.NewRegistry()), MaxEventsPerBlock: 1, History: h, HistoryProjectionTarget: target, Attribution: attr, AttributionDomain: domain})
		require.NoError(t, openErr)
		return w
	}
	main := open("main", "owner/main/next", history.TargetMain, mainAttr, "owner-main")
	boot := open("boot", "owner/boot/next", history.TargetBootstrapLive, bootAttr, "owner-boot")
	defer func() {
		_ = main.Close()
		_ = boot.Close()
	}()
	appendRow := func(sequence uint64, target history.ProjectionTarget) {
		body := []byte(fmt.Sprintf("row-%d", sequence))
		event := segment.Event{Kind: segment.KindCreate, DID: "did:plc:owner", Collection: "org.example.record", Rkey: "same", Rev: fmt.Sprintf("revision-%d", sequence), Payload: body, WitnessedAt: int64(sequence), IndexedAt: int64(sequence + 1000)}
		owner, attr, domain := main, mainAttr, "owner-main"
		if target == history.TargetBootstrapLive {
			owner, attr, domain = boot, bootAttr, "owner-boot"
		}
		_, appendErr := owner.CaptureAndProject(t.Context(), history.Source{Domain: domain, Epoch: "epoch", Cursor: sequence, Operation: 0, Path: "org.example.record/same", ContentHash: history.OriginalHash(body)}, event, body, history.Policy{Revision: 1, Collections: []string{event.Collection}}, "live", AttributionMetadata{StreamID: h.StreamID(), Policy: attribution.Policy{Revision: 1, Collections: []string{event.Collection}}, Provenance: attribution.ProvenanceLive})
		require.NoError(t, appendErr)
		observed, lookupErr := attr.Lookup(owner.NextSeq() - 1)
		require.NoError(t, lookupErr)
		require.Equal(t, h.StreamID(), observed.StreamID)
		require.Equal(t, attribution.ProvenanceLive, observed.Provenance)
		require.NotEmpty(t, observed.PolicyRev)
	}
	owners := make([]history.ProjectionTarget, 0, 262)
	owners = append(owners, history.TargetBootstrapLive, history.TargetMain, history.TargetBootstrapLive, history.TargetMain)
	for i := 0; i < 258; i++ {
		if i%2 == 0 {
			owners = append(owners, history.TargetBootstrapLive)
		} else {
			owners = append(owners, history.TargetMain)
		}
	}
	for i, target := range owners {
		appendRow(uint64(i+1), target)
	}
	require.NoError(t, main.Close())
	main = open("main", "owner/main/next", history.TargetMain, mainAttr, "owner-main")
	require.EqualValues(t, 132, main.NextSeq())
	require.EqualValues(t, 132, boot.NextSeq())
	appliedMain, err := h.AppliedThroughFor(history.TargetMain)
	require.NoError(t, err)
	require.EqualValues(t, 262, appliedMain)
	appliedBoot, err := h.AppliedThroughFor(history.TargetBootstrapLive)
	require.NoError(t, err)
	require.EqualValues(t, 261, appliedBoot)
	require.EqualValues(t, 132, boot.NextSeq())
	require.NoError(t, main.SealActiveAndClose())
	require.NoError(t, boot.SealActiveAndClose())
	mainRows, err := SegmentFiles(filepath.Join(base, "main"))
	require.NoError(t, err)
	bootRows, err := SegmentFiles(filepath.Join(base, "boot"))
	require.NoError(t, err)
	require.NotEmpty(t, mainRows)
	require.NotEmpty(t, bootRows)
	for _, tc := range []struct {
		files []SegmentFile
		want  int
	}{{mainRows, 131}, {bootRows, 131}} {
		var count int
		for _, file := range tc.files {
			reader, openErr := segment.Open(segment.ReaderConfig{Path: file.Path})
			require.NoError(t, openErr)
			for block := range reader.Blocks() {
				events, decodeErr := reader.DecodeBlock(block)
				require.NoError(t, decodeErr)
				count += len(events)
			}
			require.NoError(t, reader.Close())
		}
		require.Equal(t, tc.want, count)
	}
	page, err := h.Read(0, 262, 300)
	require.NoError(t, err)
	require.Len(t, page.Events, 262)
	for i, row := range page.Events {
		require.EqualValues(t, i+1, row.Sequence)
		require.Equal(t, fmt.Sprintf("row-%d", i+1), string(row.Original))
	}
	require.NoError(t, main.Close())
	require.NoError(t, boot.Close())
	main = open("main", "owner/main/next", history.TargetMain, mainAttr, "owner-main")
	boot = open("boot", "owner/boot/next", history.TargetBootstrapLive, bootAttr, "owner-boot")
	require.EqualValues(t, 132, main.NextSeq())
	require.EqualValues(t, 132, boot.NextSeq())
	require.NoError(t, main.Close())
	require.NoError(t, boot.Close())
}
