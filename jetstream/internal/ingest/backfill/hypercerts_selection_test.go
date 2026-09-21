package backfill

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// TestT15SelectionDirectPDSPermittedTemporaryStorage serves one complete CAR
// containing selected and excluded records through the production bootstrap
// engine and the direct-PDS retry downloader. The CAR is only held by the
// test server and downloader in process memory; the configured persistent
// roots must not contain the excluded fixture value after either acquisition.
func TestT15SelectionDirectPDSPermittedTemporaryStorage(t *testing.T) {
	const excluded = "t15-selection-excluded-payload"
	did := atmos.DID("did:plc:t15selection")
	fixture := selectionEvidenceFixture(t, did, excluded)

	t.Run("bootstrap Run", func(t *testing.T) {
		dataDir, db, writer := selectionEvidenceWriter(t)
		srv := newStubServer(t, map[atmos.DID]repoFixture{did: fixture})
		require.NoError(t, Run(t.Context(), Config{
			Store: db, Writer: writer, HTTPClient: &http.Client{Timeout: 5 * time.Second},
			RelayURL: srv.srv.URL, NewHostClient: stubHostClient(srv),
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
			RetryBaseDelay: time.Millisecond, RetryMaxDelay: 10 * time.Millisecond,
		}))
		require.Equal(t, int64(1), srv.getRepoHit.Load(), "Run must download the served CAR")
		closeAndInspectSelectionEvidence(t, dataDir, db, writer, excluded)
	})

	t.Run("retry download", func(t *testing.T) {
		dataDir, db, writer := selectionEvidenceWriter(t)
		srv := newStubServer(t, map[atmos.DID]repoFixture{did: fixture})
		backfillStore := NewStore(db, nil)
		now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
		require.NoError(t, backfillStore.onDiscover(t.Context(), "pds.stub.test", sync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, backfillStore.OnFail(t.Context(), did, "pds.stub.test", errors.New("bootstrap unavailable"), 1))
		require.NoError(t, backfillStore.RecordRetryFailure(t.Context(), did, "pds.stub.test", errors.New("retry due"), now.Add(-time.Minute)))
		runner, err := newRetryRunner(RetryConfig{
			Store: db, Writer: writer, HTTPClient: srv.srv.Client(), RelayURL: srv.srv.URL,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Workers: 1, HostWorkers: 1,
			MaxDelay: time.Hour, NewHostClient: stubHostClient(srv), now: func() time.Time { return now },
		})
		require.NoError(t, err)
		require.NoError(t, runner.runPass(t.Context()))
		require.Equal(t, int64(1), srv.getRepoHit.Load(), "retry must download the served CAR directly")
		closeAndInspectSelectionEvidence(t, dataDir, db, writer, excluded)
	})
}

func selectionEvidenceFixture(t *testing.T, did atmos.DID, excluded string) repoFixture {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	blocks := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: blocks, Tree: mst.NewTree(blocks)}
	require.NoError(t, repo.Create("app.bsky.feed.post", "selected", map[string]any{"text": "selected"}))
	require.NoError(t, repo.Create("app.bsky.feed.like", "excluded", map[string]any{"text": excluded}))
	_, err = repo.Commit(key)
	require.NoError(t, err)
	var car bytes.Buffer
	require.NoError(t, repo.ExportCAR(&car, key))
	return repoFixture{did: did, car: car.Bytes()}
}

func selectionEvidenceWriter(t *testing.T) (string, *store.Store, *ingest.Writer) {
	t.Helper()
	dataDir := t.TempDir()
	db, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	policy, err := selection.Open(db, []string{"app.bsky.feed.post"})
	require.NoError(t, err)
	writer, err := ingest.Open(ingest.Config{
		DataDir: dataDir, SegmentsDir: filepath.Join(dataDir, "segments"), Store: db,
		CollectionPolicy: policy, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4, MaxSegmentBytes: 1 << 30,
	})
	require.NoError(t, err)
	return dataDir, db, writer
}

func closeAndInspectSelectionEvidence(t *testing.T, dataDir string, db *store.Store, writer *ingest.Writer, excluded string) {
	t.Helper()
	require.NoError(t, writer.Close())
	rows := collectActiveEvents(t, filepath.Join(dataDir, "segments", ingest.SegmentFilename(0)))
	commits := 0
	for _, row := range rows {
		if !row.Kind.IsCommit() {
			continue
		}
		commits++
		require.Equal(t, "app.bsky.feed.post", row.Collection)
		require.Equal(t, "selected", row.Rkey)
		require.NotContains(t, string(row.Payload), excluded)
	}
	require.Equal(t, 1, commits, "the served CAR must materialize exactly its selected record")
	require.NoError(t, db.Close())
	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		contents, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(contents), excluded) {
			return fmt.Errorf("excluded direct-PDS record payload persisted in %s", path)
		}
		return nil
	})
	require.NoError(t, err)
}
