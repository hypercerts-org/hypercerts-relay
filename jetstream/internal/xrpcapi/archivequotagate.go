package xrpcapi

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/archivekeys"
)

// archiveQuotaGate admits a successful archive response before its first byte.
// ServeContent calculates Content-Length after Range and conditional headers,
// so 206 responses spend their selected range and 304 responses spend nothing.
// Plan JSON responses spend one request but no archive bytes.
type archiveQuotaGate struct {
	http.ResponseWriter
	keys        *archivekeys.Manager
	keyID       string
	chargeBytes bool
	wrote       bool
	denied      bool
	status      int
}

var errArchiveQuotaDenied = errors.New("archive quota denied")

func (g *archiveQuotaGate) WriteHeader(status int) {
	if g.wrote {
		return
	}
	g.wrote = true
	g.Header().Set("Cache-Control", "private, no-store")
	if status != http.StatusOK && status != http.StatusPartialContent {
		g.ResponseWriter.WriteHeader(status)
		return
	}
	var length int64
	if g.chargeBytes {
		var err error
		length, err = strconv.ParseInt(g.Header().Get("Content-Length"), 10, 64)
		if err != nil || length < 0 {
			g.reject(http.StatusInternalServerError, "ArchiveResponseLengthUnavailable", "archive response length unavailable", false)
			return
		}
	}
	if err := g.keys.AdmitResponse(g.keyID, length); err != nil {
		switch {
		case errors.Is(err, archivekeys.ErrRequestLimited):
			g.reject(http.StatusTooManyRequests, "RateLimitExceeded", "archive request limit exceeded", true)
		case errors.Is(err, archivekeys.ErrByteLimited):
			g.reject(http.StatusTooManyRequests, "RateLimitExceeded", "archive byte limit exceeded", true)
		default:
			g.reject(http.StatusUnauthorized, "AuthRequired", "archive API key revoked", false)
		}
		return
	}
	g.ResponseWriter.WriteHeader(status)
}

func (g *archiveQuotaGate) reject(status int, name, message string, retry bool) {
	g.denied = true
	g.status = status
	g.Header().Del("Content-Length")
	g.Header().Del("Content-Range")
	g.Header().Del("ETag")
	g.Header().Set("Cache-Control", "no-store")
	g.Header().Set("Content-Type", "application/json")
	if retry {
		g.Header().Set("Retry-After", strconv.Itoa(archivekeys.RetryAfter(time.Now())))
	} else if status == http.StatusUnauthorized {
		g.Header().Set("WWW-Authenticate", `Bearer realm="jetstream-archive"`)
	}
	g.ResponseWriter.WriteHeader(status)
	_, _ = io.WriteString(g.ResponseWriter, `{"error":"`+name+`","message":"`+message+`"}`)
}

func (g *archiveQuotaGate) Write(p []byte) (int, error) {
	if !g.wrote {
		g.WriteHeader(http.StatusOK)
	}
	if g.denied {
		return 0, errArchiveQuotaDenied
	}
	return g.ResponseWriter.Write(p)
}

func (g *archiveQuotaGate) ReadFrom(src io.Reader) (int64, error) {
	if !g.wrote {
		g.WriteHeader(http.StatusOK)
	}
	if g.denied {
		return 0, errArchiveQuotaDenied
	}
	if rf, ok := g.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	return io.Copy(struct{ io.Writer }{g}, src)
}
