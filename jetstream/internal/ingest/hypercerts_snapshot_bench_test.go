package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/prometheus/client_golang/prometheus"
)

const snapshotBenchCollection = "org.hypercerts.claim.activity"

func snapshotBenchRoot(b *testing.B) string {
	b.Helper()
	root := os.Getenv("BENCH_DIR")
	if root == "" {
		return b.TempDir()
	}
	dir, err := os.MkdirTemp(root, "jsreconcile")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// newSnapshotBenchWriter opens a writer whose archive already holds archiveRows
// selected-collection rows spread across 4,096 other DIDs, sealed into
// segments of segmentBytes compressed bytes. Each row carries a 300-byte
// incompressible payload, so segment counts match the stored volume.
func newSnapshotBenchWriter(b *testing.B, archiveRows int, segmentBytes int64) *Writer {
	b.Helper()
	root := snapshotBenchRoot(b)
	st, err := store.Open(filepath.Join(root, "meta"), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	policy, err := selection.Open(st, []string{snapshotBenchCollection})
	if err != nil {
		b.Fatal(err)
	}
	w, err := Open(Config{
		SegmentsDir:      filepath.Join(root, "segments"),
		Store:            st,
		CollectionPolicy: policy,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:          NewMetrics(prometheus.NewRegistry()),
		MaxSegmentBytes:  segmentBytes,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	random := rand.New(rand.NewPCG(1, 2))
	payloads := make([]byte, 300*archiveRows)
	for i := range payloads {
		payloads[i] = byte(random.Uint32())
	}
	clock := atmos.NewTIDClock(1)
	const batchSize = 4096
	batch := make([]segment.Event, 0, batchSize)
	for row := range archiveRows {
		rev := clock.Next().String()
		batch = append(batch, segment.Event{
			Kind:       segment.KindCreate,
			DID:        fmt.Sprintf("did:plc:archive%017d", row%4096),
			Collection: snapshotBenchCollection,
			Rkey:       rev,
			Rev:        rev,
			Payload:    payloads[row*300 : (row+1)*300],
		})
		if len(batch) == batchSize || row == archiveRows-1 {
			if err := w.AppendBatch(context.Background(), batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if err := w.Flush(context.Background()); err != nil {
		b.Fatal(err)
	}
	return w
}

// BenchmarkWriterReconcileSnapshot measures the real serial section of one
// direct-PDS repository reconciliation: a full archive scan, a one-record
// replacement append, and the synced snapshot boundary. Each iteration
// reconciles a newer revision of one repository, so every iteration writes.
// Set BENCH_DIR to a disk-backed directory to include real fsync cost.
func BenchmarkWriterReconcileSnapshot(b *testing.B) {
	for _, shape := range []struct {
		archiveRows  int
		segmentBytes int64
	}{
		// A near-empty archive isolates the fixed flush and boundary cost.
		{1_000, 64 << 20},
		{100_000, 64 << 20},
		{1_000_000, 64 << 20},
		// Smaller segments: the same rows sealed into many more files.
		{1_000_000, 8 << 20},
	} {
		archiveRows := shape.archiveRows
		b.Run(fmt.Sprintf("archiveRows=%d/segmentBytes=%d", archiveRows, shape.segmentBytes), func(b *testing.B) {
			w := newSnapshotBenchWriter(b, archiveRows, shape.segmentBytes)
			files, err := SegmentFilesFS(w.cfg.FS, w.cfg.SegmentsDir)
			if err != nil {
				b.Fatal(err)
			}
			const did = "did:plc:reconciletarget000000000"
			clock := atmos.NewTIDClock(2)
			payload := []byte(`{"$type":"org.hypercerts.claim.activity","title":"bench"}`)
			b.ResetTimer()
			for range b.N {
				rev := clock.Next().String()
				snapshot := Snapshot{
					DID:         did,
					Rev:         rev,
					Collections: []string{snapshotBenchCollection},
					Records: []segment.Event{{
						Kind:       segment.KindCreateResync,
						DID:        did,
						Rev:        rev,
						Collection: snapshotBenchCollection,
						Rkey:       "self",
						Payload:    payload,
					}},
				}
				if err := w.ReconcileSnapshot(context.Background(), snapshot); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(archiveRows), "ns/archive-row")
			b.ReportMetric(float64(len(files)), "segments-at-start")
		})
	}
}
