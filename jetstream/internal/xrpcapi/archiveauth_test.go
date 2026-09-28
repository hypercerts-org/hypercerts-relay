package xrpcapi

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bluesky-social/jetstream/internal/hypercerts/archivekeys"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestArchiveQuotaOnlySpendsSuccessfulResponses(t *testing.T) {
	dir := t.TempDir()
	writeSealedSegment(t, dir, 0, 1)
	mft, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	keys, err := archivekeys.Open(db)
	require.NoError(t, err)
	_, segmentToken, err := keys.Create("segment", "team", 1, 1)
	require.NoError(t, err)
	_, blockToken, err := keys.Create("block", "team", 1, 1)
	require.NoError(t, err)
	_, planToken, err := keys.Create("plan", "team", 1, 1)
	require.NoError(t, err)
	ready := false
	h := New(Config{Src: mft, ArchiveKeys: keys, Ready: func(context.Context) error {
		if !ready {
			return errors.New("warming up")
		}
		return nil
	}}).Handler()
	segmentPath := "/xrpc/network.bsky.jetstream.getSegment?name=" + ingest.SegmentFilename(0)
	blockPath := "/xrpc/network.bsky.jetstream.getBlock?segment=" + ingest.SegmentFilename(0) + "&blockIndex=0"
	planPath := "/xrpc/network.bsky.jetstream.planSnapshot"
	require.Equal(t, http.StatusServiceUnavailable, archiveRequest(h, "GET", segmentPath, segmentToken, nil, nil).Code)
	require.Equal(t, http.StatusServiceUnavailable, archiveRequest(h, "GET", blockPath, blockToken, nil, nil).Code)
	require.Equal(t, http.StatusServiceUnavailable, archiveRequest(h, "POST", planPath, planToken, []byte(`{}`), nil).Code)
	ready = true
	require.Equal(t, http.StatusBadRequest, archiveRequest(h, "GET", "/xrpc/network.bsky.jetstream.getSegment", segmentToken, nil, nil).Code)
	require.Equal(t, http.StatusNotFound, archiveRequest(h, "GET", "/xrpc/network.bsky.jetstream.getSegment?name="+ingest.SegmentFilename(999), segmentToken, nil, nil).Code)
	require.Equal(t, http.StatusBadRequest, archiveRequest(h, "GET", "/xrpc/network.bsky.jetstream.getBlock?segment="+ingest.SegmentFilename(0)+"&blockIndex=nope", blockToken, nil, nil).Code)
	require.Equal(t, http.StatusNotFound, archiveRequest(h, "GET", "/xrpc/network.bsky.jetstream.getBlock?segment="+ingest.SegmentFilename(999)+"&blockIndex=0", blockToken, nil, nil).Code)
	require.Equal(t, http.StatusBadRequest, archiveRequest(h, "POST", planPath, planToken, []byte(`{`), nil).Code)
	require.Equal(t, http.StatusOK, archiveRequest(h, "GET", segmentPath, segmentToken, nil, nil).Code)
	require.Equal(t, http.StatusOK, archiveRequest(h, "GET", blockPath, blockToken, nil, nil).Code)
	plan := archiveRequest(h, "POST", planPath, planToken, []byte(`{}`), nil)
	require.Equal(t, http.StatusOK, plan.Code)
	require.Contains(t, plan.Body.String(), "plannedThroughSeq")
	segmentLimited := archiveRequest(h, "GET", segmentPath, segmentToken, nil, nil)
	require.Equal(t, http.StatusTooManyRequests, segmentLimited.Code)
	require.NotEmpty(t, segmentLimited.Header().Get("Retry-After"))
	blockLimited := archiveRequest(h, "GET", blockPath, blockToken, nil, nil)
	require.Equal(t, http.StatusTooManyRequests, blockLimited.Code)
	planLimited := archiveRequest(h, "POST", planPath, planToken, []byte(`{}`), nil)
	require.Equal(t, http.StatusTooManyRequests, planLimited.Code)
	require.NotEmpty(t, planLimited.Header().Get("Retry-After"))
}

func archiveRequest(h http.Handler, method, path, token string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestArchiveKeyFeatureGateAndQuotas(t *testing.T) {
	dir := t.TempDir()
	path := writeSealedSegment(t, dir, 0, 1)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(100))
	mft, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	name := ingest.SegmentFilename(0)
	segmentPath := "/xrpc/network.bsky.jetstream.getSegment?name=" + name
	blockPath := "/xrpc/network.bsky.jetstream.getBlock?segment=" + name + "&blockIndex=0"
	planPath := "/xrpc/network.bsky.jetstream.planSnapshot"

	// A nil key manager is the default-off feature state.
	open := New(Config{Src: mft})
	require.Equal(t, http.StatusOK, archiveRequest(open.Handler(), "GET", segmentPath, "", nil, nil).Code)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	keys, err := archivekeys.Open(db)
	require.NoError(t, err)
	key, token, err := keys.Create("test consumer", "team", 5, 1)
	require.NoError(t, err)
	protected := New(Config{Src: mft, ArchiveKeys: keys})
	missing := archiveRequest(protected.Handler(), "GET", segmentPath, "", nil, nil)
	require.Equal(t, http.StatusUnauthorized, missing.Code)
	require.NotEmpty(t, missing.Header().Get("WWW-Authenticate"))
	invalid := archiveRequest(protected.Handler(), "GET", segmentPath, "wrong", nil, nil)
	require.Equal(t, http.StatusUnauthorized, invalid.Code)
	require.NotEmpty(t, invalid.Header().Get("WWW-Authenticate"))
	// Archive admission applies to block and planning endpoints too.
	require.Equal(t, http.StatusUnauthorized, archiveRequest(protected.Handler(), "GET", blockPath, "", nil, nil).Code)
	require.Equal(t, http.StatusUnauthorized, archiveRequest(protected.Handler(), "POST", planPath, "", []byte(`{}`), nil).Code)
	require.Equal(t, http.StatusOK, archiveRequest(protected.Handler(), "GET", segmentPath, token, nil, nil).Code)
	require.Equal(t, http.StatusOK, archiveRequest(protected.Handler(), "GET", blockPath, token, nil, nil).Code)
	// listSegments remains public under the feature flag.
	require.Equal(t, http.StatusOK, archiveRequest(protected.Handler(), "GET", "/xrpc/network.bsky.jetstream.listSegments", "", nil, nil).Code)
	require.NoError(t, keys.Revoke(key.ID))
	require.Equal(t, http.StatusUnauthorized, archiveRequest(protected.Handler(), "GET", segmentPath, token, nil, nil).Code)
}

func TestArchiveRequestAndByteLimitHTTP(t *testing.T) {
	dir := t.TempDir()
	path := writeSealedSegment(t, dir, 0, 1)
	info, err := os.Stat(path)
	require.NoError(t, err)
	mft, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	keys, err := archivekeys.Open(db)
	require.NoError(t, err)
	key, token, err := keys.Create("test consumer", "team", 2, 1)
	require.NoError(t, err)
	h := New(Config{Src: mft, ArchiveKeys: keys}).Handler()
	segmentPath := "/xrpc/network.bsky.jetstream.getSegment?name=" + ingest.SegmentFilename(0)
	// Reserve all but 5 bytes. The full download must be rejected before any
	// segment bytes are sent; a five-byte Range can still be admitted.
	require.Less(t, info.Size(), int64(1_000_000))
	require.NoError(t, keys.AdmitResponse(key.ID, 1_000_000-5))
	full := archiveRequest(h, "GET", segmentPath, token, nil, nil)
	require.Equal(t, http.StatusTooManyRequests, full.Code)
	require.NotEmpty(t, full.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"RateLimitExceeded","message":"archive byte limit exceeded"}`, full.Body.String())
	ranged := archiveRequest(h, "GET", segmentPath, token, nil, map[string]string{"Range": "bytes=0-4"})
	require.Equal(t, http.StatusPartialContent, ranged.Code)
	require.Len(t, ranged.Body.Bytes(), 5)
	// The rejected full request and accepted range both spend requests.
	limited := archiveRequest(h, "GET", segmentPath, token, nil, nil)
	require.Equal(t, http.StatusTooManyRequests, limited.Code)
	require.NotEmpty(t, limited.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"RateLimitExceeded","message":"archive request limit exceeded"}`, limited.Body.String())

	blockKey, blockToken, err := keys.Create("block consumer", "team", 2, 1)
	require.NoError(t, err)
	require.NoError(t, keys.AdmitResponse(blockKey.ID, 999_999))
	blockPath := "/xrpc/network.bsky.jetstream.getBlock?segment=" + ingest.SegmentFilename(0) + "&blockIndex=0"
	block := archiveRequest(h, "GET", blockPath, blockToken, nil, nil)
	require.Equal(t, http.StatusTooManyRequests, block.Code)
	require.NotEmpty(t, block.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"RateLimitExceeded","message":"archive byte limit exceeded"}`, block.Body.String())

	conditionalKey, conditionalToken, err := keys.Create("conditional consumer", "team", 3, 1)
	require.NoError(t, err)
	first := archiveRequest(h, "GET", segmentPath, conditionalToken, nil, nil)
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "private, no-store", first.Header().Get("Cache-Control"))
	require.NoError(t, keys.AdmitResponse(conditionalKey.ID, 1_000_000-info.Size()))
	unchanged := archiveRequest(h, "GET", segmentPath, conditionalToken, nil, map[string]string{"If-None-Match": first.Header().Get("ETag")})
	require.Equal(t, http.StatusNotModified, unchanged.Code)
	require.Equal(t, "private, no-store", unchanged.Header().Get("Cache-Control"))
	require.NoError(t, keys.AdmitResponse(conditionalKey.ID, 0), "304 must not spend a request")
}
