package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	state, err := w.scanSnapshotArchive(ctx, snapshot)
	if err != nil {
		return err
	}
	if state.accountUnavailable {
		return ErrAccountUnavailable
	}
	if state.syncRev > snapshot.Rev {
		return nil
	} // A newer whole-repo replacement already won.
	rows, err := state.planReplacement(snapshot)
	if err != nil {
		return err
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
	return w.persistSnapshotBoundary(snapshot)
}

// snapshotArchiveState retains only metadata needed to reconcile one DID.
type snapshotArchiveState struct {
	latest             map[string]segment.Event
	syncRev            string
	accountSeq         uint64
	accountUnavailable bool
}

func (s *snapshotArchiveState) observe(snapshot Snapshot, row segment.Event) error {
	if row.DID != snapshot.DID {
		return nil
	}
	if row.Kind == segment.KindAccount && row.Seq > s.accountSeq {
		var account comatproto.SyncSubscribeRepos_Account
		if err := account.UnmarshalCBOR(row.Payload); err != nil {
			return err
		}
		s.accountSeq = row.Seq
		s.accountUnavailable = !account.Active
	}
	if row.Kind == segment.KindSync && row.Rev > s.syncRev {
		s.syncRev = row.Rev
	}
	if row.Kind.IsCommit() && slices.Contains(snapshot.Collections, row.Collection) {
		s.observeRecord(row)
	}
	return nil
}

func (s *snapshotArchiveState) observeRecord(row segment.Event) {
	key := row.Collection + "/" + row.Rkey
	old, ok := s.latest[key]
	if !ok || row.Rev > old.Rev || row.Rev == old.Rev && row.Seq > old.Seq {
		row.Payload = nil
		s.latest[key] = row
	}
}

// Caller holds writer/rewrite locks throughout this scan and the later write.
func (w *Writer) scanSnapshotArchive(ctx context.Context, snapshot Snapshot) (*snapshotArchiveState, error) {
	state := &snapshotArchiveState{latest: map[string]segment.Event{}}
	files, err := SegmentFilesFS(w.cfg.FS, w.cfg.SegmentsDir)
	if err != nil {
		return nil, err
	}
	visit := func(rows []segment.Event) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, row := range rows {
			if err := state.observe(snapshot, row); err != nil {
				return err
			}
		}
		return nil
	}
	for _, file := range files {
		if err := w.walkSnapshotSegment(file.Path, visit); err != nil {
			return nil, fmt.Errorf("snapshot archive scan: %w", err)
		}
	}
	return state, nil
}

func (w *Writer) walkSnapshotSegment(path string, visit func([]segment.Event) error) (err error) {
	r, err := segment.Open(segment.ReaderConfig{Path: path, FS: w.cfg.FS})
	if errors.Is(err, segment.ErrActiveSegment) {
		return segment.WalkActiveFS(w.cfg.FS, path, visit)
	}
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := r.Close(); err == nil {
			err = closeErr
		}
	}()
	for i := range r.Blocks() {
		rows, err := r.DecodeBlock(i)
		if err != nil {
			return err
		}
		if err := visit(rows); err != nil {
			return err
		}
	}
	return nil
}

func (s *snapshotArchiveState) planReplacement(snapshot Snapshot) ([]segment.Event, error) {
	keys := map[string]bool{}
	var rows []segment.Event
	for _, row := range snapshot.Records {
		if row.DID != snapshot.DID || row.Rev != snapshot.Rev || !slices.Contains(snapshot.Collections, row.Collection) || !row.Kind.IsMaterialization() {
			return nil, errors.New("snapshot record outside verified scope")
		}
		key := row.Collection + "/" + row.Rkey
		if keys[key] {
			return nil, errors.New("duplicate snapshot record")
		}
		keys[key] = true
		if old, ok := s.latest[key]; ok && old.Rev >= row.Rev {
			continue
		}
		row.Kind = segment.KindUpdate // Scoped replacement invalidates older versions of this key.
		rows = append(rows, row)
	}
	rows = append(rows, s.missingRecordDeletes(snapshot, keys)...)
	slices.SortFunc(rows, func(a, b segment.Event) int {
		if order := strings.Compare(a.Collection, b.Collection); order != 0 {
			return order
		}
		return strings.Compare(a.Rkey, b.Rkey)
	})
	for _, row := range rows {
		if err := segment.ValidateEvent(row); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (s *snapshotArchiveState) missingRecordDeletes(snapshot Snapshot, keys map[string]bool) []segment.Event {
	var rows []segment.Event
	for key, old := range s.latest {
		if keys[key] || old.Kind == segment.KindDelete || old.Rev >= snapshot.Rev || old.Rev < s.syncRev {
			continue
		}
		rows = append(rows, segment.Event{Kind: segment.KindDelete, DID: snapshot.DID, Rev: snapshot.Rev, Collection: old.Collection, Rkey: old.Rkey, WitnessedAt: time.Now().UnixMicro()})
	}
	return rows
}

// Called only after the replacement rows have crossed the archive durability boundary.
func (w *Writer) persistSnapshotBoundary(snapshot Snapshot) error {
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
