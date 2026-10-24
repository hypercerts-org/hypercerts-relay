package policygate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/hypercerts/selection"
	"github.com/bluesky-social/jetstream/internal/store"
)

func testPolicy(t *testing.T) *selection.Manager {
	t.Helper()
	db, err := store.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	manager, err := selection.Open(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

type testDrainer struct {
	mu       sync.Mutex
	next     uint64
	drainErr error
	drained  chan struct{}
	release  chan struct{}
}

func (d *testDrainer) DrainDurability(ctx context.Context) error {
	if d.drained != nil {
		select {
		case d.drained <- struct{}{}:
		default:
		}
	}
	if d.release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.release:
		}
	}
	return d.drainErr
}
func (d *testDrainer) NextSeq() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.next
}

func TestAdmitSerializesWithAdoption(t *testing.T) {
	d := &testDrainer{next: 8, drained: make(chan struct{}, 1), release: make(chan struct{})}
	g := New(testPolicy(t), d, "domain")
	adopted := make(chan struct{})
	go func() {
		_ = g.Adopt(context.Background(), "domain", func(boundary *uint64) error {
			if boundary == nil || *boundary != 7 {
				t.Errorf("boundary = %d, want 7", boundary)
			}
			close(adopted)
			return nil
		})
	}()
	<-d.drained
	admitted := make(chan struct{})
	go func() {
		_ = g.Admit(context.Background(), func(selection.Policy) error { close(admitted); return nil })
	}()
	select {
	case <-admitted:
		t.Fatal("admission passed adoption before its drain completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(d.release)
	select {
	case <-adopted:
	case <-time.After(time.Second):
		t.Fatal("adoption did not complete")
	}
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("admission did not resume after adoption")
	}
}

func TestAdoptBoundaryAndDrainFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		next uint64
		want uint64
	}{{"empty", 1, 0}, {"existing", 12, 11}} {
		t.Run(tc.name, func(t *testing.T) {
			d := &testDrainer{next: tc.next}
			called := false
			err := New(testPolicy(t), d, "domain").Adopt(context.Background(), "domain", func(boundary *uint64) error {
				called = true
				if tc.next <= 1 {
					if boundary != nil {
						t.Fatalf("boundary = %d, want nil", *boundary)
					}
					return nil
				}
				if boundary == nil || *boundary != tc.want {
					t.Fatalf("boundary = %d, want %d", boundary, tc.want)
				}
				return nil
			})
			if err != nil || !called {
				t.Fatalf("Adopt() = %v, callback called %v", err, called)
			}
		})
	}
	drainErr := errors.New("drain failed")
	called := false
	err := New(testPolicy(t), &testDrainer{drainErr: drainErr}, "domain").Adopt(context.Background(), "domain", func(*uint64) error { called = true; return nil })
	if !errors.Is(err, drainErr) || called {
		t.Fatalf("Adopt() = %v, callback called %v", err, called)
	}
}

func TestCanceledContextDoesNotAcquire(t *testing.T) {
	g := New(testPolicy(t), &testDrainer{}, "domain")
	started, release := make(chan struct{}), make(chan struct{})
	go func() {
		_ = g.Admit(context.Background(), func(selection.Policy) error { close(started); <-release; return nil })
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Admit(ctx, func(selection.Policy) error { t.Error("canceled admission callback ran"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Admit() = %v, want context.Canceled", err)
	}
	close(release)
}

func TestAdoptRejectsWrongDomain(t *testing.T) {
	called := false
	err := New(testPolicy(t), &testDrainer{}, "one").Adopt(context.Background(), "two", func(*uint64) error { called = true; return nil })
	if !errors.Is(err, ErrDomainRequired) || called {
		t.Fatalf("Adopt() = %v, callback called %v", err, called)
	}
}
