package xrpcapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func getSegURL(base, name string) string {
	return fmt.Sprintf("%s/xrpc/network.bsky.jetstream.getSegment?name=%s", base, name)
}

func TestGetSegment_WholeFile(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	resp := doGet(t, getSegURL(ts.URL, name))
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	require.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"))
	require.Equal(t, "public, no-cache", resp.Header.Get("Cache-Control"))
	require.NotEmpty(t, resp.Header.Get("ETag"))
	require.Empty(t, resp.Header.Get("Content-Encoding"))

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	want := rawFile(t, dir+"/"+name)
	require.Equal(t, want, got, "body must be byte-identical to the on-disk file")
	require.Equal(t, int64(len(want)), resp.ContentLength)
}

func TestGetSegment_CacheMaxAge(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	cached := New(Config{Src: s.src, Logger: s.logger, CacheMaxAge: time.Hour})
	ts := httptest.NewServer(cached.Handler())
	defer ts.Close()

	resp := doGet(t, getSegURL(ts.URL, ingest.SegmentFilename(0)))
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "public, max-age=3600", resp.Header.Get("Cache-Control"))
}

func TestCacheControlHeader(t *testing.T) {
	t.Parallel()
	require.Equal(t, "public, no-cache", cacheControlHeader(0))
	require.Equal(t, "public, no-cache", cacheControlHeader(-time.Second))
	require.Equal(t, "public, max-age=1", cacheControlHeader(500*time.Millisecond))
	require.Equal(t, "public, max-age=3600", cacheControlHeader(time.Hour))
}

func TestGetSegment_RangeRequest(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	resp := doGetWith(t, getSegURL(ts.URL, name), func(req *http.Request) {
		req.Header.Set("Range", "bytes=0-99")
	})
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Len(t, body, 100)
	want := rawFile(t, dir+"/"+name)
	require.Equal(t, want[:100], body)
}

func TestGetSegment_ConditionalNotModified(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	first := doGet(t, getSegURL(ts.URL, name))
	_ = first.Body.Close()
	etag := first.Header.Get("ETag")
	require.NotEmpty(t, etag)

	resp := doGetWith(t, getSegURL(ts.URL, name), func(req *http.Request) {
		req.Header.Set("If-None-Match", etag)
	})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
}

func TestGetSegment_Errors(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	cases := []struct {
		name string
		want int
	}{
		{"", http.StatusBadRequest},
		{"..%2Fetc%2Fpasswd", http.StatusBadRequest},
		{"seg_x.jss", http.StatusBadRequest},
		{"seg_0000000000.txt", http.StatusBadRequest},
		{"seg_00000000zz.jss", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doGet(t, getSegURL(ts.URL, tc.name))
			_ = resp.Body.Close()
			require.Equal(t, tc.want, resp.StatusCode)
		})
	}
}

func TestGetSegment_NotFoundErrorName(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// A well-formed but absent segment must return the lexicon's declared
	// error name, not the generic NotFound, so clients can match on it.
	resp := doGet(t, getSegURL(ts.URL, "seg_00000000zz.jss"))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "SegmentNotFound", body.Error)
}

func TestGetSegment_DeletedFileRace(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	require.NoError(t, os.Remove(dir+"/"+name))

	resp := doGet(t, getSegURL(ts.URL, name))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestRepresentationValidatorsAfterPatch(t *testing.T) {
	ts, path := newBlockTestServer(t, 2, 1)
	_, err := segment.Patch(path, func(event *segment.Event) bool {
		event.IndexedAt = 1000000000000001
		return true
	}, segment.PatchOptions{})
	require.NoError(t, err)
	name := ingest.SegmentFilename(0)
	urls := []string{getSegURL(ts.URL, name), blockURL(ts.URL, name, 0)}
	oldTags := make([]string, len(urls))
	oldBodies := make([][]byte, len(urls))
	for i, url := range urls {
		resp := doGet(t, url)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		oldTags[i] = resp.Header.Get("ETag")
		var err error
		oldBodies[i], err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	oldFile, err := os.Open(path)
	require.NoError(t, err)
	defer oldFile.Close()
	oldInfo, err := oldFile.Stat()
	require.NoError(t, err)
	oldHeader, err := segment.ReadSealedHeader(oldFile)
	require.NoError(t, err)
	var result segment.PatchResult
	for timestamp := int64(1000000000000002); timestamp <= 1000000000000256; timestamp++ {
		require.NoError(t, os.WriteFile(path+".fixture", oldBodies[0], 0600))
		require.NoError(t, os.Rename(path+".fixture", path))
		result, err = segment.Patch(path, func(event *segment.Event) bool {
			event.IndexedAt = timestamp
			return true
		}, segment.PatchOptions{})
		require.NoError(t, err)
		if result.NewChecksum == oldHeader.Checksum {
			break
		}
	}
	require.True(t, result.Patched)
	require.Equal(t, oldHeader.Checksum, result.NewChecksum)
	require.NoError(t, os.Chtimes(path, oldInfo.ModTime(), oldInfo.ModTime()))
	newInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, oldInfo.ModTime(), newInfo.ModTime())
	pinned, err := io.ReadAll(io.NewSectionReader(oldFile, 0, oldInfo.Size()))
	require.NoError(t, err)
	require.Equal(t, oldBodies[0], pinned)

	for i, url := range urls {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			resp := doGet(t, url)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Empty(t, resp.Header.Get("Last-Modified"))
			require.NotEqual(t, oldBodies[i], body)
			digest := sha256.Sum256(body)
			etag := fmt.Sprintf("\"sha256-%x-%d\"", digest, len(body))
			require.Equal(t, etag, resp.Header.Get("ETag"))
			require.NotEqual(t, oldTags[i], etag)
			date := time.Now().Add(24 * time.Hour).UTC().Format(http.TimeFormat)
			cases := []struct {
				name, header, value string
				ranged              bool
				status              int
				want                []byte
			}{
				{"old none match", "If-None-Match", oldTags[i], false, 200, body},
				{"old if range", "If-Range", oldTags[i], true, 200, body},
				{"current none match", "If-None-Match", etag, false, 304, nil},
				{"current if range", "If-Range", etag, true, 206, body[:4]},
				{"date modified", "If-Modified-Since", date, false, 200, body},
				{"date if range", "If-Range", date, true, 200, body},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got := doGetWith(t, url, func(req *http.Request) {
						req.Header.Set(tc.header, tc.value)
						if tc.ranged {
							req.Header.Set("Range", "bytes=0-3")
						}
					})
					defer got.Body.Close()
					actual, err := io.ReadAll(got.Body)
					require.NoError(t, err)
					require.Equal(t, tc.status, got.StatusCode)
					require.Empty(t, got.Header.Get("Last-Modified"))
					require.True(t, bytes.Equal(tc.want, actual))
				})
			}
		})
	}
}

type failingReaderAt struct{}

func (failingReaderAt) ReadAt(p []byte, off int64) (int, error) { return 0, errors.New("read failure") }

func TestRepresentationETagReadFailures(t *testing.T) {
	_, err := representationETag(bytes.NewReader([]byte("short")), 10)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = representationETag(failingReaderAt{}, 10)
	require.EqualError(t, err, "read failure")
}

type renameOnHeader struct {
	http.ResponseWriter
	replace func()
}

func (w *renameOnHeader) WriteHeader(status int) {
	w.replace()
	w.ResponseWriter.WriteHeader(status)
}

func TestGetSegmentPinsDescriptorAcrossRename(t *testing.T) {
	s, dir := newTestServer(t, 1)
	path := dir + "/" + ingest.SegmentFilename(0)
	original := rawFile(t, path)
	replacement := append([]byte(nil), original...)
	replacement[len(replacement)-1] ^= 1
	require.NoError(t, os.WriteFile(path+".replacement", replacement, 0600))
	handler := s.Handler()
	recorder := httptest.NewRecorder()
	writer := &renameOnHeader{ResponseWriter: recorder, replace: func() { require.NoError(t, os.Rename(path+".replacement", path)) }}
	request := httptest.NewRequest(http.MethodGet, getSegURL("", ingest.SegmentFilename(0)), nil)
	handler.ServeHTTP(writer, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, original, recorder.Body.Bytes())
	digest := sha256.Sum256(original)
	require.Equal(t, fmt.Sprintf("\"sha256-%x-%d\"", digest, len(original)), recorder.Header().Get("ETag"))
	require.Equal(t, replacement, rawFile(t, path))
}
