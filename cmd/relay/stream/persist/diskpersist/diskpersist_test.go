package diskpersist

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/util"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func testCid() cid.Cid {
	buf := make([]byte, 32)
	c, err := cid.NewPrefixV1(cid.Raw, mh.SHA2_256).Sum(buf)
	if err != nil {
		panic(err)
	}
	return c
}

func TestInitialSequenceNumber(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()

	db, err := gorm.Open(sqlite.Open(filepath.Join(dir, "relay.sqlite")))
	require.NoError(err)

	// Open the persistence, the current sequence should be 1 (the default):
	persist, err := NewDiskPersistence(dir, "", db, DefaultDiskPersistOptions())
	require.NoError(err)
	require.Equal(int64(1), persist.curSeq)

	// Shutdown the persister:
	err = persist.Shutdown(t.Context())
	require.NoError(err)

	// Reopen the persistence, the current sequence should still be 1:
	persist, err = NewDiskPersistence(dir, "", db, DefaultDiskPersistOptions())
	require.NoError(err)
	require.Equal(int64(1), persist.curSeq)

	// Fake the DID to UID mapping:
	did := "did:example:123"
	persist.didCache.Add(did, 123)

	// Insert a dummy event:
	event := &stream.XRPCStreamEvent{
		RepoCommit: &atproto.SyncSubscribeRepos_Commit{
			Repo:   did,
			Commit: lexutil.LexLink(testCid()),
			Time:   time.Now().Format(util.ISO8601),
		},
	}
	err = persist.Persist(t.Context(), event)
	require.NoError(err)

	// Sequence number should now be 2:
	require.Equal(int64(2), persist.curSeq)

	// Shutdown the persister:
	err = persist.Shutdown(t.Context())
	require.NoError(err)

	// Reopen the persistence, the current sequence should still be 2:
	persist, err = NewDiskPersistence(dir, "", db, DefaultDiskPersistOptions())
	require.NoError(err)
	require.Equal(int64(2), persist.curSeq)
}

func newTestDiskPersistence(t *testing.T, opts *DiskPersistOptions) (*DiskPersistence, *gorm.DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := gorm.Open(sqlite.Open(filepath.Join(dir, "relay.sqlite")))
	require.NoError(t, err)

	persist, err := NewDiskPersistence(dir, "", db, opts)
	require.NoError(t, err)
	return persist, db, dir
}

func testCommitEvent(did string) *stream.XRPCStreamEvent {
	return &stream.XRPCStreamEvent{
		RepoCommit: &atproto.SyncSubscribeRepos_Commit{
			Repo:   did,
			Commit: lexutil.LexLink(testCid()),
			Time:   time.Now().Format(util.ISO8601),
		},
	}
}

func TestPersistWaitsForDurabilityBeforeBroadcastAndRestartReplay(t *testing.T) {
	persist, db, dir := newTestDiskPersistence(t, DefaultDiskPersistOptions())
	did := "did:example:durable"
	persist.didCache.Add(did, 1)

	synced := false
	persist.syncFile = func(fi *os.File) error {
		synced = true
		return fi.Sync()
	}
	broadcasts := 0
	persist.SetEventBroadcaster(func(event *stream.XRPCStreamEvent) {
		require.True(t, synced)
		require.Equal(t, int64(1), event.Sequence())
		broadcasts++
	})

	require.NoError(t, persist.Persist(t.Context(), testCommitEvent(did)))
	require.Equal(t, 1, broadcasts)
	require.NoError(t, persist.Shutdown(t.Context()))

	restarted, err := NewDiskPersistence(dir, "", db, DefaultDiskPersistOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, restarted.Shutdown(t.Context())) }()

	var replayed []*stream.XRPCStreamEvent
	require.NoError(t, restarted.Playback(t.Context(), 0, func(event *stream.XRPCStreamEvent) error {
		replayed = append(replayed, event)
		return nil
	}))
	require.Len(t, replayed, 1)
	require.Equal(t, int64(1), replayed[0].Sequence())
}

func TestPersistCompletesPartialWrites(t *testing.T) {
	persist, db, dir := newTestDiskPersistence(t, DefaultDiskPersistOptions())
	did := "did:example:partial"
	persist.didCache.Add(did, 1)
	writes := 0
	persist.writeFile = func(fi *os.File, data []byte) (int, error) {
		writes++
		if writes == 1 && len(data) > 7 {
			data = data[:7]
			n, err := fi.Write(data)
			return n, errors.Join(err, io.ErrShortWrite)
		}
		return fi.Write(data)
	}

	require.NoError(t, persist.Persist(t.Context(), testCommitEvent(did)))
	require.NoError(t, persist.Shutdown(t.Context()))

	restarted, err := NewDiskPersistence(dir, "", db, DefaultDiskPersistOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, restarted.Shutdown(t.Context())) }()

	count := 0
	require.NoError(t, restarted.Playback(t.Context(), 0, func(*stream.XRPCStreamEvent) error {
		count++
		return nil
	}))
	require.Equal(t, 1, count)
}

func TestPersistRotatesAfterRestart(t *testing.T) {
	opts := DefaultDiskPersistOptions()
	opts.EventsPerFile = 1
	persist, db, dir := newTestDiskPersistence(t, opts)
	did := "did:example:restart-rotation"
	persist.didCache.Add(did, 1)
	require.NoError(t, persist.Persist(t.Context(), testCommitEvent(did)))
	require.NoError(t, persist.Shutdown(t.Context()))

	restarted, err := NewDiskPersistence(dir, "", db, opts)
	require.NoError(t, err)
	restarted.didCache.Add(did, 1)
	require.NoError(t, restarted.Persist(t.Context(), testCommitEvent(did)))
	require.NoError(t, restarted.Shutdown(t.Context()))

	var refs []LogFileRef
	require.NoError(t, db.Order("seq_start asc").Find(&refs).Error)
	require.Len(t, refs, 2)

	replayed, err := NewDiskPersistence(dir, "", db, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, replayed.Shutdown(t.Context())) }()

	count := 0
	require.NoError(t, replayed.Playback(t.Context(), 0, func(*stream.XRPCStreamEvent) error {
		count++
		return nil
	}))
	require.Equal(t, 2, count)
}

func TestPersistWriteFailurePreventsLaterAcknowledgement(t *testing.T) {
	persist, _, _ := newTestDiskPersistence(t, DefaultDiskPersistOptions())
	did := "did:example:write-failure"
	persist.didCache.Add(did, 1)
	writeErr := errors.New("write failed")
	broadcasts := 0
	persist.SetEventBroadcaster(func(*stream.XRPCStreamEvent) { broadcasts++ })
	persist.writeFile = func(*os.File, []byte) (int, error) {
		return 0, writeErr
	}

	require.ErrorIs(t, persist.Persist(t.Context(), testCommitEvent(did)), writeErr)
	persist.writeFile = func(fi *os.File, data []byte) (int, error) { return fi.Write(data) }
	require.ErrorIs(t, persist.Persist(t.Context(), testCommitEvent(did)), writeErr)
	require.Equal(t, int64(1), persist.curSeq)
	require.Equal(t, 0, broadcasts)
	require.ErrorIs(t, persist.Shutdown(t.Context()), writeErr)
}

func TestRestartRecoversIncompleteTrailingWrite(t *testing.T) {
	for _, prefix := range []int{7, headerSize + 7} {
		t.Run(fmt.Sprint(prefix), func(t *testing.T) {
			persist, db, dir := newTestDiskPersistence(t, DefaultDiskPersistOptions())
			did := "did:example:incomplete"
			persist.didCache.Add(did, 1)
			require.NoError(t, persist.Persist(t.Context(), testCommitEvent(did)))
			written := false
			persist.writeFile = func(fi *os.File, data []byte) (int, error) {
				if written {
					return 0, io.ErrShortWrite
				}
				written = true
				n, err := fi.Write(data[:prefix])
				return n, errors.Join(err, io.ErrShortWrite)
			}
			require.Error(t, persist.Persist(t.Context(), testCommitEvent(did)))
			require.Error(t, persist.Shutdown(t.Context()))
			restarted, err := NewDiskPersistence(dir, "", db, DefaultDiskPersistOptions())
			require.NoError(t, err)
			defer func() { require.NoError(t, restarted.Shutdown(t.Context())) }()
			require.Equal(t, int64(2), restarted.curSeq)
			restarted.didCache.Add(did, 1)
			require.NoError(t, restarted.Persist(t.Context(), testCommitEvent(did)))
			var seqs []int64
			require.NoError(t, restarted.Playback(t.Context(), 0, func(evt *stream.XRPCStreamEvent) error {
				seqs = append(seqs, evt.Sequence())
				return nil
			}))
			require.Equal(t, []int64{1, 2}, seqs)
		})
	}
}

func TestPersistSyncFailurePreventsLaterAcknowledgement(t *testing.T) {
	persist, db, dir := newTestDiskPersistence(t, DefaultDiskPersistOptions())
	did := "did:example:sync-failure"
	persist.didCache.Add(did, 1)
	syncErr := errors.New("sync failed")
	broadcasts := 0
	persist.SetEventBroadcaster(func(*stream.XRPCStreamEvent) { broadcasts++ })
	persist.syncFile = func(*os.File) error { return syncErr }

	require.ErrorIs(t, persist.Persist(t.Context(), testCommitEvent(did)), syncErr)
	persist.syncFile = func(fi *os.File) error { return fi.Sync() }
	require.ErrorIs(t, persist.Persist(t.Context(), testCommitEvent(did)), syncErr)
	require.Equal(t, int64(1), persist.curSeq)
	require.Equal(t, 0, broadcasts)
	require.ErrorIs(t, persist.Shutdown(t.Context()), syncErr)

	restarted, err := NewDiskPersistence(dir, "", db, DefaultDiskPersistOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, restarted.Shutdown(t.Context())) }()

	count := 0
	require.NoError(t, restarted.Playback(t.Context(), 0, func(*stream.XRPCStreamEvent) error {
		count++
		return nil
	}))
	require.Equal(t, 1, count)
}

func TestPersistRotationMetadataFailurePreventsLaterAcknowledgement(t *testing.T) {
	opts := DefaultDiskPersistOptions()
	opts.EventsPerFile = 1
	persist, _, _ := newTestDiskPersistence(t, opts)
	did := "did:example:rotation"
	persist.didCache.Add(did, 1)
	metadataErr := errors.New("directory sync failed")
	broadcasts := 0
	persist.SetEventBroadcaster(func(*stream.XRPCStreamEvent) { broadcasts++ })

	require.NoError(t, persist.Persist(t.Context(), testCommitEvent(did)))
	persist.syncDir = func(string) error { return metadataErr }
	require.ErrorIs(t, persist.Persist(t.Context(), testCommitEvent(did)), metadataErr)
	persist.syncDir = syncDirectory
	require.ErrorIs(t, persist.Persist(t.Context(), testCommitEvent(did)), metadataErr)
	require.Equal(t, 1, broadcasts)
	require.ErrorIs(t, persist.Shutdown(t.Context()), metadataErr)
}
