package live

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/history"
	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
)

func TestOriginalCaptureKeepsSourceOrdinalAndDeduplicates(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	journal, err := history.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	capture := OriginalCapture{Journal: journal, Domain: "relay-a", Epoch: "boot-7", Policy: func() selection.Policy {
		return selection.Policy{Revision: 4, Collections: []string{"org.hypercerts.claim"}}
	}}
	src := SourceEvent{Cursor: 91, Raw: []byte("wire event"), Rows: []OriginalOperation{
		{Ordinal: 2, Path: "org.hypercerts.claim/a", DID: "did:plc:a", Collection: "org.hypercerts.claim", Revision: "r2", Kind: "update", ObservedAt: 123, Payload: []byte("payload")},
		{Ordinal: 5, Path: "org.hypercerts.unselected/b", Collection: "org.hypercerts.unselected", Kind: "create"},
	}}
	first, err := capture.CaptureOriginal(src)
	if err != nil || len(first) != 1 {
		t.Fatalf("capture = %v, %v", first, err)
	}
	if first[0].Sequence != 1 || first[0].Duplicate {
		t.Fatalf("first receipt = %+v", first[0])
	}
	second, err := capture.CaptureOriginal(src)
	if err != nil || len(second) != 1 {
		t.Fatalf("replay = %v, %v", second, err)
	}
	if second[0].Sequence != first[0].Sequence || !second[0].Duplicate {
		t.Fatalf("replay receipt = %+v", second[0])
	}
	page, err := journal.Read(0, 0, 10)
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("page = %+v, %v", page, err)
	}
	got := page.Events[0]
	if got.Source.Domain != "relay-a" || got.Source.Epoch != "boot-7" || got.Source.Cursor != 91 || got.Source.Operation != 2 {
		t.Fatalf("source = %+v", got.Source)
	}
	if got.Time != 123 || got.Provenance != "upstream-original" || got.Policy.Revision != 4 {
		t.Fatalf("event metadata = %+v", got)
	}
}

func TestOriginalCaptureConflictingReplayFailsClosed(t *testing.T) {
	db, err := store.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	journal, err := history.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	capture := OriginalCapture{Journal: journal, Domain: "relay", Policy: func() selection.Policy {
		return selection.Policy{Revision: 1, Collections: []string{"org.hypercerts.claim"}}
	}}
	src := SourceEvent{Cursor: 7, Raw: []byte("one"), Rows: []OriginalOperation{{Ordinal: 0, Path: "org.hypercerts.claim/x", Collection: "org.hypercerts.claim", Kind: "delete"}}}
	if _, err := capture.CaptureOriginal(src); err != nil {
		t.Fatal(err)
	}
	src.Raw = []byte("two")
	if _, err := capture.CaptureOriginal(src); err != history.ErrConflict {
		t.Fatalf("conflict error = %v", err)
	}
}
