package jetstreamd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(b.buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		out = append(out, rec)
	}
	return out
}

func newTestVerifyFailureLog() (*verifyFailureLog, *syncBuffer, *time.Time) {
	buf := &syncBuffer{}
	l := newVerifyFailureLog(slog.New(slog.NewJSONHandler(buf, nil)))
	now := time.Date(2026, 11, 12, 11, 17, 24, 0, time.UTC)
	l.now = func() time.Time { return now }
	return l, buf, &now
}

func TestVerifyFailureLog_SummarisesRepeatsPerDID(t *testing.T) {
	t.Parallel()
	l, buf, now := newTestVerifyFailureLog()

	for i := range 180 {
		l.observe("did:plc:a", "chain", errors.New("chain break "+string(rune('a'+i%26))))
	}
	l.observe("did:plc:b", "chain", errors.New("chain break b"))
	l.observe("did:plc:a", "signature", errors.New("bad signature"))

	recs := buf.records(t)
	require.Len(t, recs, 3, "only the first failure per DID and class is logged immediately")
	for _, r := range recs {
		require.Equal(t, "verification failure", r["msg"])
	}

	l.flush(false)
	require.Len(t, buf.records(t), 3, "nothing is summarised before the window elapses")

	*now = now.Add(verifyFailureLogWindow)
	l.flush(false)
	recs = buf.records(t)
	require.Len(t, recs, 4, "one summary for the DID with repeats; none for single failures")
	summary := recs[3]
	require.Equal(t, "verification failures repeated", summary["msg"])
	require.Equal(t, "did:plc:a", summary["did"])
	require.Equal(t, "chain", summary["class"])
	require.EqualValues(t, 179, summary["suppressed"])
	require.Contains(t, summary["last_err"], "chain break")

	// The window restarts: the next failure is logged in full again.
	l.observe("did:plc:a", "chain", errors.New("chain break again"))
	recs = buf.records(t)
	require.Len(t, recs, 5)
	require.Equal(t, "verification failure", recs[4]["msg"])
}

func TestVerifyFailureLog_BoundsTrackedDIDs(t *testing.T) {
	t.Parallel()
	l, buf, _ := newTestVerifyFailureLog()
	l.maxKeys = 2

	l.observe("did:plc:a", "chain", errors.New("x"))
	l.observe("did:plc:b", "chain", errors.New("x"))
	l.observe("did:plc:c", "chain", errors.New("x"))
	l.observe("did:plc:d", "chain", errors.New("x"))
	require.Len(t, buf.records(t), 2)

	l.flush(true)
	recs := buf.records(t)
	require.Len(t, recs, 3)
	require.Equal(t, "verification failures not logged per DID", recs[2]["msg"])
	require.EqualValues(t, 2, recs[2]["count"])
	require.Empty(t, l.entries, "a forced flush clears all state")
}

func TestVerifyFailureLog_RunFlushesOnShutdown(t *testing.T) {
	t.Parallel()
	l, buf, _ := newTestVerifyFailureLog()
	l.observe("did:plc:a", "chain", errors.New("x"))
	l.observe("did:plc:a", "chain", errors.New("y"))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- l.run(ctx) }()
	cancel()
	require.NoError(t, <-done)

	recs := buf.records(t)
	require.Len(t, recs, 2)
	require.Equal(t, "verification failures repeated", recs[1]["msg"])
	require.EqualValues(t, 1, recs[1]["suppressed"])
}
