package main

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func newTestUpstream(t *testing.T, d *Discoverer) *Upstream {
	t.Helper()
	u, err := NewUpstream("wss://jetstream.example.com/subscribe?compress=false&wantedCollections=stale", 10*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), d)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestSubscribeURL(t *testing.T) {
	u := newTestUpstream(t, nil)
	before := time.Now().Add(-10 * time.Minute).UnixMicro()
	raw, cursor, err := u.subscribeURL([]string{"a.b.c", "a.b.*"})
	if err != nil {
		t.Fatal(err)
	}
	q := mustQuery(t, raw)
	if got := q["wantedCollections"]; len(got) != 2 || got[0] != "a.b.c" || got[1] != "a.b.*" {
		t.Fatalf("wantedCollections = %v", got)
	}
	if q.Get("compress") != "false" {
		t.Fatal("existing query parameters were dropped")
	}
	if cursor < before || q.Get("cursor") != strconv.FormatInt(cursor, 10) {
		t.Fatalf("cold-start cursor %d not within the lookback", cursor)
	}

	u.cursor.Store(42)
	raw, cursor, _ = u.subscribeURL([]string{"a.b.c"})
	if cursor != 42 || mustQuery(t, raw).Get("cursor") != "42" {
		t.Fatalf("resume cursor = %d (%s)", cursor, raw)
	}
}

func mustQuery(t *testing.T, raw string) url.Values {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Query()
}

func TestHandleMessageFiltersOperationsAndAdvancesCursor(t *testing.T) {
	h := newHarness(t, Config{})
	u := newTestUpstream(t, h.d)
	ctx := context.Background()
	did := h.account("https://pds.example.com")
	for _, msg := range []string{
		`{"did":"` + did + `","time_us":10,"kind":"commit","commit":{"operation":"delete","collection":"` + collection + `"}}`,
		`{"did":"` + did + `","time_us":20,"kind":"identity"}`,
		`not json`,
	} {
		u.handleMessage(ctx, []byte(msg))
	}
	if len(h.admin.enabled) != 0 {
		t.Fatalf("enabled on a non-record event: %v", h.admin.enabled)
	}
	if u.cursor.Load() != 20 {
		t.Fatalf("cursor = %d, want 20", u.cursor.Load())
	}
	u.handleMessage(ctx, []byte(`{"did":"`+did+`","time_us":15,"kind":"commit","commit":{"operation":"create","collection":"`+collection+`"}}`))
	if len(h.admin.enabled) != 1 {
		t.Fatalf("create did not enable the PDS: %v", h.admin.enabled)
	}
	if u.cursor.Load() != 20 {
		t.Fatal("an older event moved the cursor backwards")
	}
}
