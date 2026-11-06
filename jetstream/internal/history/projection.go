package history

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

var (
	ErrProjectionConflict = errors.New("history: conflicting projection intent")
	ErrProjectionPending  = errors.New("history: projection intent is not durable")
	ErrReservationPending = errors.New("history: an earlier projection reservation is pending")
)

type ProjectionTarget string

const (
	TargetMain          ProjectionTarget = "main"
	TargetBootstrapLive ProjectionTarget = "bootstrap-live"
)

func validTarget(target ProjectionTarget) bool {
	return target == TargetMain || target == TargetBootstrapLive
}

type ProjectionIntent struct {
	Target          ProjectionTarget `json:"target"`
	JournalSequence uint64           `json:"journalSequence"`
	SegmentSequence uint64           `json:"segmentSequence"`
	Fingerprint     string           `json:"fingerprint"`
	Disposition     Disposition      `json:"disposition"`
	PlannedEvent    *segment.Event   `json:"plannedEvent,omitempty"`
}

type ProjectionRow struct {
	SegmentSequence uint64
	Fingerprint     string
}
type RecoveryStatus string

const (
	RecoveryMatched     RecoveryStatus = "matched"
	RecoveryNeedsReplay RecoveryStatus = "needs-replay"
	RecoveryNoopReady   RecoveryStatus = "noop-ready"
	RecoveryComplete    RecoveryStatus = "complete"
)

type ProjectionRecovery struct {
	Intent ProjectionIntent
	Status RecoveryStatus
}
type PendingProjection struct {
	Intent ProjectionIntent
	Status RecoveryStatus
}

func projectionIntentKey(sequence uint64) []byte {
	return fmt.Appendf([]byte(nil), "%sprojection-intent/%020d", metaPrefix, sequence)
}
func projectionCompletionKey(sequence uint64) []byte {
	return fmt.Appendf([]byte(nil), "%s%020d", keyCompletion, sequence)
}
func targetAppliedKey(target ProjectionTarget) []byte {
	return []byte(keyTargetApplied + string(target))
}
func projectionReservationKey(target ProjectionTarget, sequence uint64) []byte {
	return fmt.Appendf([]byte(nil), "%sprojection-reservation/%s/%020d", metaPrefix, target, sequence)
}

func (s *Store) projectionIntent(sequence uint64) (ProjectionIntent, bool, error) {
	if sequence == 0 {
		return ProjectionIntent{}, false, ErrInvalid
	}
	value, closer, err := s.db.Get(projectionIntentKey(sequence))
	if errors.Is(err, store.ErrNotFound) {
		return ProjectionIntent{}, false, nil
	}
	if err != nil {
		return ProjectionIntent{}, false, err
	}
	if string(value) == "complete" {
		if err := closer.Close(); err != nil {
			return ProjectionIntent{}, false, err
		}
		value, closer, err = s.db.Get(projectionCompletionKey(sequence))
		if err != nil {
			return ProjectionIntent{}, false, err
		}
	}
	var intent ProjectionIntent
	decodeErr := json.Unmarshal(value, &intent)
	closeErr := closer.Close()
	if decodeErr != nil {
		return ProjectionIntent{}, false, decodeErr
	}
	if closeErr != nil {
		return ProjectionIntent{}, false, closeErr
	}
	if intent.JournalSequence != sequence || !validTarget(intent.Target) {
		return ProjectionIntent{}, false, ErrProjectionConflict
	}
	return intent, true, nil
}

// ProjectionIntent returns the default main-target intent.
func (s *Store) ProjectionIntent(sequence uint64) (ProjectionIntent, bool, error) {
	return s.ProjectionIntentFor(TargetMain, sequence)
}
func (s *Store) ProjectionIntentFor(target ProjectionTarget, sequence uint64) (ProjectionIntent, bool, error) {
	intent, found, err := s.projectionIntent(sequence)
	if err != nil || !found {
		return intent, found, err
	}
	if intent.Target != target {
		return ProjectionIntent{}, false, nil
	}
	return intent, true, nil
}

// PendingProjections returns pending intents owned by the default main target.
func (s *Store) PendingProjections() ([]PendingProjection, error) {
	return s.PendingProjectionsFor(TargetMain)
}
func (s *Store) PendingProjectionsFor(target ProjectionTarget) ([]PendingProjection, error) {
	if !validTarget(target) {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tip, err := s.Tip()
	if err != nil {
		return nil, err
	}
	pending := make([]PendingProjection, 0)
	for seq := uint64(1); seq <= tip; seq++ {
		intent, found, err := s.projectionIntent(seq)
		if err != nil {
			return nil, err
		}
		if found && intent.Target == target {
			status, err := s.ProjectionStatusFor(target, seq)
			if err != nil {
				return nil, err
			}
			if status != RecoveryComplete {
				pending = append(pending, PendingProjection{Intent: intent, Status: "pending"})
			}
		}
	}
	return pending, nil
}

type ProjectionCommit func(stage func(*pebble.Batch) error) error

func rangeIntersectsHistory(start, end []byte) bool {
	prefix := []byte(metaPrefix)
	upper := append(bytes.Clone(prefix), 0xff)
	return bytes.Compare(start, upper) < 0 && bytes.Compare(end, prefix) > 0
}

func (s *Store) PrepareProjection(intent ProjectionIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if intent.Target == "" {
		intent.Target = TargetMain
	}
	if !validTarget(intent.Target) || intent.JournalSequence == 0 || intent.Fingerprint == "" || (intent.Disposition != Apply && intent.Disposition != HistoricalOnly && intent.Disposition != Noop) {
		return ErrInvalid
	}
	if intent.Disposition == Apply && intent.SegmentSequence == 0 {
		return fmt.Errorf("%w: segment coordinate required", ErrInvalid)
	}
	if intent.Disposition != Apply && intent.SegmentSequence != 0 {
		return fmt.Errorf("%w: non-applied projection has no segment coordinate", ErrInvalid)
	}
	event, found, err := s.Event(intent.JournalSequence)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("history: projection journal row missing")
	}
	if event.Disposition != intent.Disposition {
		return ErrProjectionConflict
	}
	if event.Owner != intent.Target {
		return fmt.Errorf("%w: journal owner is %s", ErrProjectionConflict, event.Owner)
	}
	value, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	if old, closer, err := s.db.Get(projectionIntentKey(intent.JournalSequence)); err == nil {
		defer closer.Close()
		if string(old) == "complete" {
			complete, c, e := s.db.Get(projectionCompletionKey(intent.JournalSequence))
			if e != nil {
				return e
			}
			defer c.Close()
			if !bytes.Equal(complete, value) {
				return ErrProjectionConflict
			}
			return nil
		}
		if !bytes.Equal(old, value) {
			return ErrProjectionConflict
		}
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	key := projectionReservationKey(intent.Target, intent.SegmentSequence)
	if intent.SegmentSequence != 0 {
		if _, closer, err := s.db.Get(importReservationKey(string(intent.Target), intent.SegmentSequence)); err == nil {
			_ = closer.Close()
			return ErrProjectionConflict
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if old, closer, err := s.db.Get(key); err == nil {
			defer closer.Close()
			if !bytes.Equal(old, []byte(fmt.Sprint(intent.JournalSequence))) {
				return ErrProjectionConflict
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(projectionIntentKey(intent.JournalSequence), value, nil); err != nil {
		return err
	}
	if intent.SegmentSequence != 0 {
		if err := batch.Set(key, []byte(fmt.Sprint(intent.JournalSequence)), nil); err != nil {
			return err
		}
	}
	return s.db.Commit(batch, store.SyncWrites)
}

// CommitProjection commits sparse journal sequences for one immutable target.
func (s *Store) CommitProjection(sequences []uint64, stageOther func(*pebble.Batch) error) error {
	return s.CommitProjectionFor(TargetMain, sequences, stageOther)
}
func (s *Store) CommitProjectionFor(target ProjectionTarget, sequences []uint64, stageOther func(*pebble.Batch) error) error {
	if !validTarget(target) || stageOther == nil || len(sequences) == 0 {
		return ErrInvalid
	}
	seqs := append([]uint64(nil), sequences...)
	for i, seq := range seqs {
		if seq == 0 || i > 0 && seq <= seqs[i-1] {
			return fmt.Errorf("history: projection sequences must increase")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range seqs {
		intent, found, err := s.projectionIntent(seq)
		if err != nil {
			return err
		}
		if !found || intent.Target != target {
			return fmt.Errorf("%w: sequence %d is not owned by %s", ErrProjectionPending, seq, target)
		}
	}
	allDone := true
	for _, seq := range seqs {
		status, err := s.ProjectionStatusFor(target, seq)
		if err != nil {
			return err
		}
		if status != RecoveryComplete {
			allDone = false
		}
	}
	if allDone {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := stageOther(batch); err != nil {
		return err
	}
	reader := batch.Reader()
	for {
		kind, key, value, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if kind == pebble.InternalKeyKindRangeDelete && rangeIntersectsHistory(key, value) || kind != pebble.InternalKeyKindSet && kind != pebble.InternalKeyKindDelete && kind != pebble.InternalKeyKindRangeDelete || kind != pebble.InternalKeyKindRangeDelete && bytes.HasPrefix(key, []byte(metaPrefix)) {
			return fmt.Errorf("history: forbidden mutation in projection batch")
		}
	}
	for _, seq := range seqs {
		status, err := s.ProjectionStatusFor(target, seq)
		if err != nil {
			return err
		}
		if status == RecoveryComplete {
			continue
		}
		intentBytes, closer, err := s.db.Get(projectionIntentKey(seq))
		if err != nil {
			return err
		}
		if err := batch.Set(projectionCompletionKey(seq), bytes.Clone(intentBytes), nil); err != nil {
			_ = closer.Close()
			return err
		}
		if err := closer.Close(); err != nil {
			return err
		}
		if err := batch.Set(projectionIntentKey(seq), []byte("complete"), nil); err != nil {
			return err
		}
	}
	// The checkpoint is the greatest contiguous completed journal prefix, not the last requested sequence.
	checkpoint, err := s.AppliedThroughFor(target)
	if err != nil {
		return err
	}
	tip, err := s.Tip()
	if err != nil {
		return err
	}
	for checkpoint < tip {
		next := checkpoint + 1
		intent, found, err := s.projectionIntent(next)
		if err != nil {
			return err
		}
		if !found {
			ownerEvent, exists, err := s.Event(next)
			if err != nil {
				return err
			}
			if !exists || ownerEvent.Owner == target {
				break
			}
			checkpoint = next
			continue
		}
		if intent.Target != target {
			checkpoint = next
			continue
		}
		done := false
		for _, seq := range seqs {
			if seq == next {
				done = true
				break
			}
		}
		if !done {
			status, err := s.ProjectionStatusFor(target, next)
			if err != nil {
				return err
			}
			done = status == RecoveryComplete
		}
		if !done {
			break
		}
		checkpoint = next
	}
	if err := batch.Set(targetAppliedKey(target), store.EncodeVersionedUint64LE(byte(formatVersion), checkpoint), nil); err != nil {
		return err
	}
	aggregate, err := s.AppliedThrough()
	if err != nil {
		return err
	}
	for aggregate < tip {
		next := aggregate + 1
		intent, found, err := s.projectionIntent(next)
		if err != nil {
			return err
		}
		if !found {
			break
		}
		done := false
		for _, seq := range seqs {
			if seq == next {
				done = true
				break
			}
		}
		if !done {
			status, err := s.ProjectionStatusFor(intent.Target, next)
			if err != nil {
				return err
			}
			done = status == RecoveryComplete
		}
		if !done {
			break
		}
		aggregate = next
	}
	if err := batch.Set([]byte(keyApplied), store.EncodeVersionedUint64LE(byte(formatVersion), aggregate), nil); err != nil {
		return err
	}
	return s.db.Commit(batch, store.SyncWrites)
}
func otherTarget(t ProjectionTarget) ProjectionTarget {
	if t == TargetMain {
		return TargetBootstrapLive
	}
	return TargetMain
}

func (s *Store) ProjectionStatus(sequence uint64) (RecoveryStatus, error) {
	return s.ProjectionStatusFor(TargetMain, sequence)
}
func (s *Store) ProjectionStatusFor(target ProjectionTarget, sequence uint64) (RecoveryStatus, error) {
	if sequence == 0 || !validTarget(target) {
		return "", ErrInvalid
	}
	intent, found, err := s.projectionIntent(sequence)
	if err != nil {
		return "", err
	}
	if !found || intent.Target != target {
		return "", nil
	}
	if _, closer, err := s.db.Get(projectionCompletionKey(sequence)); err == nil {
		_ = closer.Close()
		return RecoveryComplete, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	return "pending", nil
}

func (s *Store) RecoverProjection(lookup func(uint64) (ProjectionRow, bool, error)) ([]ProjectionRecovery, error) {
	return s.RecoverProjectionFor(TargetMain, lookup)
}
func (s *Store) RecoverProjectionFor(target ProjectionTarget, lookup func(uint64) (ProjectionRow, bool, error)) ([]ProjectionRecovery, error) {
	if !validTarget(target) || lookup == nil {
		return nil, ErrInvalid
	}
	pending, err := s.PendingProjectionsFor(target)
	if err != nil {
		return nil, err
	}
	result := make([]ProjectionRecovery, 0, len(pending))
	for _, p := range pending {
		intent := p.Intent
		if intent.Disposition == Noop || intent.Disposition == HistoricalOnly {
			result = append(result, ProjectionRecovery{Intent: intent, Status: RecoveryNoopReady})
			continue
		}
		row, exists, err := lookup(intent.SegmentSequence)
		if err != nil {
			return nil, err
		}
		if !exists {
			result = append(result, ProjectionRecovery{Intent: intent, Status: RecoveryNeedsReplay})
			continue
		}
		if row.SegmentSequence != intent.SegmentSequence || row.Fingerprint != intent.Fingerprint {
			return nil, ErrProjectionConflict
		}
		result = append(result, ProjectionRecovery{Intent: intent, Status: RecoveryMatched})
	}
	return result, nil
}
