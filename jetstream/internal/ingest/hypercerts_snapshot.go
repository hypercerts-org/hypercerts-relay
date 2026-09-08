package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
)

// Snapshot is a verified full-PDS repository projected to the job's collections.
// It never emits a DID-wide tombstone: missing paths become scoped deletes.
type Snapshot struct {
	DID         string
	Rev         string
	Collections []string
	Records     []segment.Event
}

type snapshotBoundary struct {
	Revisions map[string]string `json:"revisions"`
}

func snapshotKey(did string) []byte { return []byte("hypercerts/snapshot/" + did) }

// snapshotSupersedes keeps delayed live/retry input below a durable snapshot
// from overwriting newer materialized state. Verification still processes it.
func (w *Writer) snapshotSupersedes(ev *segment.Event) (bool, error) {
	if w.cfg.CollectionPolicy == nil || ev.Rev == "" {
		return false, nil
	}
	b, closer, err := w.cfg.Store.Get(snapshotKey(ev.DID))
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	var boundary snapshotBoundary
	if err := json.Unmarshal(b, &boundary); err != nil {
		return false, err
	}
	if ev.Kind.IsCommit() {
		return ev.Rev <= boundary.Revisions[ev.Collection], nil
	}
	if ev.Kind == segment.KindSync {
		return false, errors.New("whole-repo sync requires scoped reconciliation after a snapshot")
	}
	return false, nil
}

// ReconcileSnapshot serializes reconciliation with live appends. The caller
// also holds the orchestrator rewrite lock so the archive scan is stable.
// Completion follows archive fsync and a durable per-DID snapshot boundary.
func (w *Writer) ReconcileSnapshot(ctx context.Context, snapshot Snapshot) error {
	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.async != nil {
		return errors.New("snapshot requires the synchronous steady-state writer")
	}
	if err := w.flushAndRotateLocked(ctx); err != nil {
		return err
	}
	latest := map[string]segment.Event{}
	var syncRev string
	var accountSeq uint64
	var accountUnavailable bool
	files, err := SegmentFilesFS(w.cfg.FS, w.cfg.SegmentsDir)
	if err != nil {
		return err
	}
	visit := func(rows []segment.Event) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, row := range rows {
			if row.DID != snapshot.DID {
				continue
			}
			if row.Kind == segment.KindAccount && row.Seq > accountSeq {
				var account comatproto.SyncSubscribeRepos_Account
				if err := account.UnmarshalCBOR(row.Payload); err != nil {
					return err
				}
				accountSeq = row.Seq
				accountUnavailable = !account.Active
			}
			if row.Kind == segment.KindSync && row.Rev > syncRev {
				syncRev = row.Rev
			}
			if !row.Kind.IsCommit() || !slices.Contains(snapshot.Collections, row.Collection) {
				continue
			}
			key := row.Collection + "/" + row.Rkey
			old, ok := latest[key]
			if !ok || row.Rev > old.Rev || row.Rev == old.Rev && row.Seq > old.Seq {
				row.Payload = nil
				latest[key] = row
			}
		}
		return nil
	}
	for _, file := range files {
		r, err := segment.Open(segment.ReaderConfig{Path: file.Path, FS: w.cfg.FS})
		if errors.Is(err, segment.ErrActiveSegment) {
			err = segment.WalkActiveFS(w.cfg.FS, file.Path, visit)
		} else if err == nil {
			for i := range r.Blocks() {
				rows, e := r.DecodeBlock(i)
				if e != nil {
					err = e
					break
				}
				if e = visit(rows); e != nil {
					err = e
					break
				}
			}
			closeErr := r.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			return fmt.Errorf("snapshot archive scan: %w", err)
		}
	}
	if accountUnavailable {
		return ErrAccountUnavailable
	}
	if syncRev > snapshot.Rev {
		return nil
	} // A newer whole-repo replacement already won.
	keys := map[string]bool{}
	var rows []segment.Event
	for _, row := range snapshot.Records {
		if row.DID != snapshot.DID || row.Rev != snapshot.Rev || !slices.Contains(snapshot.Collections, row.Collection) || !row.Kind.IsMaterialization() {
			return errors.New("snapshot record outside verified scope")
		}
		key := row.Collection + "/" + row.Rkey
		if keys[key] {
			return errors.New("duplicate snapshot record")
		}
		keys[key] = true
		if old, ok := latest[key]; ok && old.Rev >= row.Rev {
			continue
		}
		row.Kind = segment.KindUpdate // Scoped replacement invalidates older versions of this key.
		rows = append(rows, row)
	}
	for key, old := range latest {
		if keys[key] || old.Kind == segment.KindDelete || old.Rev >= snapshot.Rev || old.Rev < syncRev {
			continue
		}
		rows = append(rows, segment.Event{Kind: segment.KindDelete, DID: snapshot.DID, Rev: snapshot.Rev, Collection: old.Collection, Rkey: old.Rkey, WitnessedAt: time.Now().UnixMicro()})
	}
	slices.SortFunc(rows, func(a, b segment.Event) int {
		if a.Collection != b.Collection {
			if a.Collection < b.Collection {
				return -1
			}
			return 1
		}
		if a.Rkey < b.Rkey {
			return -1
		}
		if a.Rkey > b.Rkey {
			return 1
		}
		return 0
	})
	for _, row := range rows {
		if err := segment.ValidateEvent(row); err != nil {
			return err
		}
	}
	for i := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := w.appendLocked(ctx, &rows[i]); err != nil {
			return err
		}
	}
	if err := w.flushAndRotateLocked(ctx); err != nil {
		return err
	}
	previous := snapshotBoundary{Revisions: map[string]string{}}
	if b, closer, err := w.cfg.Store.Get(snapshotKey(snapshot.DID)); err == nil {
		err = json.Unmarshal(b, &previous)
		closer.Close()
		if err != nil {
			return err
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if previous.Revisions == nil {
		previous.Revisions = map[string]string{}
	}
	for _, collection := range snapshot.Collections {
		if snapshot.Rev > previous.Revisions[collection] {
			previous.Revisions[collection] = snapshot.Rev
		}
	}
	boundary, err := json.Marshal(previous)
	if err != nil {
		return err
	}
	return w.cfg.Store.Set(snapshotKey(snapshot.DID), boundary, store.SyncWrites)
}

// ErrAccountUnavailable prevents snapshots from resurrecting a concurrently
// deleted or deactivated account. A subsequent active account event permits retry.
var ErrAccountUnavailable = errors.New("snapshot account unavailable")

// NeedsScopedSync reports whether this DID has collection-specific snapshot
// boundaries. Whole-repo tombstones are unsafe after that first reconciliation.
func (w *Writer) NeedsScopedSync(did string) (bool, error) {
	if w.cfg.CollectionPolicy == nil {
		return false, nil
	}
	// Managed steady-state syncs always serialize with first-snapshot publication.
	if w.cfg.ReconcileSnapshot != nil {
		return true, nil
	}
	_, closer, err := w.cfg.Store.Get(snapshotKey(did))
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// ReconcileSync projects a verified complete replacement to scoped record
// operations once direct-PDS snapshots have established mixed revision boundaries.
func (w *Writer) ReconcileSync(ctx context.Context, did, rev string, rows []segment.Event) (bool, error) {
	needed, err := w.NeedsScopedSync(did)
	if err != nil || !needed {
		return false, err
	}
	snapshot := Snapshot{DID: did, Rev: rev, Collections: w.cfg.CollectionPolicy.Current().Collections}
	for _, row := range rows {
		if row.Kind.IsMaterialization() && slices.Contains(snapshot.Collections, row.Collection) {
			snapshot.Records = append(snapshot.Records, row)
		}
	}
	reconcile := w.cfg.ReconcileSnapshot
	if reconcile == nil {
		reconcile = w.ReconcileSnapshot
	}
	err = reconcile(ctx, snapshot)
	if errors.Is(err, ErrAccountUnavailable) {
		err = nil
	} // stale sync cannot revive an inactive account.
	return true, err
}
