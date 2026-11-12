package jetstreamd

// hypercerts: The atmos verifier reports a verification failure for every
// commit it buffers while a DID resyncs, each against the same stale chain
// state. One gap after a catch-up overflow produced hundreds of identical
// warnings per second and Railway discarded thousands of log lines
// (TECH-883). This summariser logs the first failure per DID and class in full,
// counts repeats, and reports them once per window. Metrics stay exact; only
// log volume is reduced.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
)

const (
	// verifyFailureLogWindow is how long repeats of one DID and failure
	// class are counted before they are summarised.
	verifyFailureLogWindow = time.Minute
	// verifyFailureLogMaxKeys bounds the per-DID state. Failures for further
	// DIDs in a window are counted in aggregate only.
	verifyFailureLogMaxKeys = 10_000
)

type verifyFailureKey struct {
	did   string
	class string
}

type verifyFailureEntry struct {
	first      time.Time
	suppressed uint64
	lastErr    error
}

type verifyFailureLog struct {
	logger  *slog.Logger
	window  time.Duration
	maxKeys int
	now     func() time.Time

	mu       sync.Mutex
	entries  map[verifyFailureKey]*verifyFailureEntry
	overflow uint64
}

func newVerifyFailureLog(logger *slog.Logger) *verifyFailureLog {
	return &verifyFailureLog{
		logger:  logger,
		window:  verifyFailureLogWindow,
		maxKeys: verifyFailureLogMaxKeys,
		now:     time.Now,
		entries: map[verifyFailureKey]*verifyFailureEntry{},
	}
}

// observe records one failure. It never blocks on I/O while holding the lock.
func (l *verifyFailureLog) observe(did string, class string, err error) {
	key := verifyFailureKey{did: did, class: class}
	l.mu.Lock()
	if e, ok := l.entries[key]; ok {
		e.suppressed++
		e.lastErr = err
		l.mu.Unlock()
		return
	}
	if len(l.entries) >= l.maxKeys {
		l.overflow++
		l.mu.Unlock()
		return
	}
	l.entries[key] = &verifyFailureEntry{first: l.now()}
	l.mu.Unlock()
	l.logger.Warn("verification failure", "did", did, "class", class, "err", err)
}

type verifyFailureSummary struct {
	key     verifyFailureKey
	count   uint64
	lastErr error
}

// flush summarises and clears entries whose window has elapsed, or all
// entries when force is set. Logging happens after the lock is released.
func (l *verifyFailureLog) flush(force bool) {
	now := l.now()
	var summaries []verifyFailureSummary
	l.mu.Lock()
	for key, e := range l.entries {
		if !force && now.Sub(e.first) < l.window {
			continue
		}
		if e.suppressed > 0 {
			summaries = append(summaries, verifyFailureSummary{key: key, count: e.suppressed, lastErr: e.lastErr})
		}
		delete(l.entries, key)
	}
	overflow := l.overflow
	l.overflow = 0
	l.mu.Unlock()

	for _, s := range summaries {
		l.logger.Warn("verification failures repeated",
			"did", s.key.did,
			"class", s.key.class,
			"suppressed", s.count,
			"window", l.window,
			"last_err", s.lastErr,
		)
	}
	if overflow > 0 {
		l.logger.Warn("verification failures not logged per DID",
			"count", overflow,
			"tracked_dids_limit", l.maxKeys,
		)
	}
}

// run flushes elapsed windows until ctx ends, then flushes everything.
func (l *verifyFailureLog) run(ctx context.Context) error {
	interval := l.window / 4
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			l.flush(true)
			return nil
		case <-ticker.C:
			l.flush(false)
		}
	}
}

// classifyVerifyFailure is the metric class used for the log key, so log
// summaries line up with jetstream_verifier_failures_total labels.
func classifyVerifyFailure(err error) string {
	return obs.Classify(err)
}
