package segment

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanActiveVisitorFailureDiscardsSequenceEnvelope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active.jss")
	writer, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	for _, seq := range []uint64{9, 3} {
		_, err = writer.Append(Event{Seq: seq, Kind: KindCreate, DID: "did:plc:scan"})
		require.NoError(t, err)
		require.NoError(t, writer.Flush())
	}
	require.NoError(t, writer.Close())
	injected := errors.New("visitor stopped")
	var visited []uint64
	maxSeq, found, err := extractScanActiveFS(nil, path, func(events []Event) error {
		visited = append(visited, events[0].Seq)
		if events[0].Seq == 3 {
			return injected
		}
		return nil
	})
	require.ErrorIs(t, err, injected)
	require.Zero(t, maxSeq)
	require.False(t, found)
	require.Equal(t, []uint64{9, 3}, visited)
	maxSeq, found, err = extractScanActiveFS(nil, path, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 9, maxSeq)
}

func TestScanActiveCompleteCorruptFrameIsNotTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active.jss")
	writer, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	_, err = writer.Append(Event{Seq: 7, Kind: KindCreate, DID: "did:plc:scan"})
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	require.NoError(t, writer.Close())
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	var prefix [8]byte
	binary.LittleEndian.PutUint64(prefix[:], 3)
	_, err = file.Write(append(prefix[:], 1, 2, 3))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	maxSeq, found, err := extractScanActiveFS(nil, path, nil)
	require.ErrorContains(t, err, "segment: scan decode block at")
	require.Zero(t, maxSeq)
	require.False(t, found)
}
