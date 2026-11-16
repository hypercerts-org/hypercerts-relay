package xrpcapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
)

// getSegmentHandler serves a whole sealed segment file. It implements
// xrpcserver.Handler directly (rather than xrpcserver.RawQuery) because it
// needs the underlying *http.Request to drive http.ServeContent's Range and
// conditional-request handling.
type getSegmentHandler struct {
	src         SegmentSource
	logger      *slog.Logger
	cacheMaxAge time.Duration
}

func (h *getSegmentHandler) ServeXRPC(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
	name, err := r.Params.String("name")
	if err != nil {
		return err // InvalidRequest (400) for missing param
	}

	idx, ok := ingest.ParseSegmentIndex(name)
	if !ok {
		return xrpcserver.InvalidRequest("malformed segment name")
	}

	ref, ok := h.src.SegmentByIdx(idx)
	if !ok {
		// Error name must match the lexicon's declared SegmentNotFound, not
		// the generic NotFound, so clients matching on the published name work.
		return &xrpc.Error{StatusCode: http.StatusNotFound, Name: "SegmentNotFound", Message: "segment not found"}
	}

	// Open and stat BEFORE writing anything, so failures become XRPC error
	// envelopes rather than a corrupt partial 200 response.
	f, err := os.Open(ref.Path)
	if err != nil {
		// Manifest believes this segment exists but we cannot open it: a
		// real inconsistency (rotation/deletion race). Surface it loudly.
		h.logger.Error("getSegment: open sealed file failed",
			slog.String("name", name), slog.String("path", ref.Path),
			slog.Any("err", err))
		return xrpcserver.InternalError("failed to open segment")
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		h.logger.Error("getSegment: stat sealed file failed",
			slog.String("name", name), slog.String("path", ref.Path),
			slog.Any("err", err))
		return xrpcserver.InternalError("failed to stat segment")
	}
	// Download validators come from the fd we actually serve, never the
	// manifest: during a compaction rename→refresh window the manifest
	// ETag is stale, and an If-Range match against it would let a
	// resuming client splice two file generations together.
	// ReadSealedHeader validates magic/version, so a corrupt or
	// foreign file errors instead of serving a confident strong ETag.
	_, err = segment.ReadSealedHeader(f)
	if err != nil {
		h.logger.Error("getSegment: read sealed header failed",
			slog.String("name", name), slog.String("path", ref.Path),
			slog.Any("err", err))
		return xrpcserver.InternalError("failed to read segment header")
	}

	// hypercerts: Hash the exact representation on the pinned descriptor, not
	// the metadata checksum, which excludes stored row bytes.
	etag, err := representationETag(f, info.Size())
	if err != nil {
		h.logger.Error("getSegment: hash sealed file failed", slog.String("name", name), slog.Any("err", err))
		return xrpcserver.InternalError("failed to hash segment")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		h.logger.Error("getSegment: seek sealed file failed", slog.String("name", name), slog.Any("err", err))
		return xrpcserver.InternalError("failed to seek segment")
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", cacheControlHeader(h.cacheMaxAge))

	// ServeContent handles Range, Accept-Ranges, Content-Length,
	// If-None-Match->304, and If-Range, and triggers sendfile(2) via the
	// statusRecorder.ReadFrom delegation. Per the xrpcserver.Handler contract
	// we MUST return nil after this point: the response may already be
	// partially written, so an error envelope is no longer possible.
	http.ServeContent(w, r.HTTPReq, name, time.Time{}, f)
	return nil
}

func cacheControlHeader(maxAge time.Duration) string {
	if maxAge <= 0 {
		return "public, no-cache"
	}
	seconds := int64(maxAge / time.Second)
	if maxAge%time.Second != 0 {
		seconds++
	}
	return "public, max-age=" + strconv.FormatInt(seconds, 10)
}
