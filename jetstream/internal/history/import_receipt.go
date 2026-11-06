package history

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

const importPrefix = "selected-projection-import/v1/"

var (
	ErrImportConflict = errors.New("history: conflicting import receipt")
	ErrImportPending  = errors.New("history: import receipt is pending")
)

type ImportSource struct {
	Domain    string `json:"domain"`
	StreamID  string `json:"streamId"`
	Sequence  uint64 `json:"sequence"`
	Operation uint32 `json:"operation"`
}

type ImportState string

const (
	ImportPending   ImportState = "pending"
	ImportCompleted ImportState = "completed"
)

type ImportReceipt struct {
	Source                 ImportSource  `json:"source"`
	Fingerprint            string        `json:"fingerprint"`
	Target                 string        `json:"target"`
	SegmentSequence        uint64        `json:"segmentSequence"`
	PlannedEvent           segment.Event `json:"plannedEvent"`
	SourceAttribution      []byte        `json:"sourceAttribution"`
	DestinationAttribution []byte        `json:"destinationAttribution"`
	Policy                 []byte        `json:"policy"`
	State                  ImportState   `json:"state"`
}

func importSourceKey(source ImportSource) []byte {
	return []byte(fmt.Sprintf("%ssource/%x", importPrefix, []byte(fmt.Sprintf("%s\x00%s\x00%020d\x00%010d", source.Domain, source.StreamID, source.Sequence, source.Operation))))
}

func importReservationKey(target string, sequence uint64) []byte {
	return []byte(fmt.Sprintf("%sreservation/%s/%020d", importPrefix, target, sequence))
}

func (s *Store) LookupImport(source ImportSource, fingerprint string) (ImportReceipt, bool, error) {
	if source.Domain == "" || source.StreamID == "" || source.Sequence == 0 || fingerprint == "" {
		return ImportReceipt{}, false, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, closer, err := s.db.Get(importSourceKey(source))
	if errors.Is(err, store.ErrNotFound) {
		return ImportReceipt{}, false, nil
	}
	if err != nil {
		return ImportReceipt{}, false, err
	}
	defer closer.Close()
	var receipt ImportReceipt
	if json.Unmarshal(value, &receipt) != nil || receipt.Source != source || receipt.Fingerprint != fingerprint {
		return ImportReceipt{}, false, ErrImportConflict
	}
	return receipt, true, nil
}

func (s *Store) PrepareImport(receipt ImportReceipt) (ImportReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if receipt.Source.Domain == "" || receipt.Source.StreamID == "" || receipt.Source.Sequence == 0 || receipt.Fingerprint == "" || receipt.Target == "" || receipt.SegmentSequence == 0 || receipt.PlannedEvent.Seq != receipt.SegmentSequence || len(receipt.SourceAttribution) == 0 || len(receipt.DestinationAttribution) == 0 || len(receipt.Policy) == 0 {
		return ImportReceipt{}, false, ErrInvalid
	}
	key := importSourceKey(receipt.Source)
	if value, closer, err := s.db.Get(key); err == nil {
		defer closer.Close()
		var previous ImportReceipt
		if json.Unmarshal(value, &previous) != nil || previous.Source != receipt.Source || previous.Fingerprint != receipt.Fingerprint {
			return ImportReceipt{}, false, ErrImportConflict
		}
		return previous, true, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return ImportReceipt{}, false, err
	}
	canonicalReservation := projectionReservationKey(ProjectionTarget(receipt.Target), receipt.SegmentSequence)
	if _, closer, err := s.db.Get(canonicalReservation); err == nil {
		_ = closer.Close()
		return ImportReceipt{}, false, ErrImportConflict
	} else if !errors.Is(err, store.ErrNotFound) {
		return ImportReceipt{}, false, err
	}
	reservation := importReservationKey(receipt.Target, receipt.SegmentSequence)
	if old, closer, err := s.db.Get(reservation); err == nil {
		defer closer.Close()
		if !bytes.Equal(old, key) {
			return ImportReceipt{}, false, ErrImportConflict
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return ImportReceipt{}, false, err
	}
	receipt.State = ImportPending
	value, err := json.Marshal(receipt)
	if err != nil {
		return ImportReceipt{}, false, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(key, value, nil); err != nil {
		return ImportReceipt{}, false, err
	}
	if err := batch.Set(reservation, key, nil); err != nil {
		return ImportReceipt{}, false, err
	}
	if err := s.db.Commit(batch, store.SyncWrites); err != nil {
		return ImportReceipt{}, false, err
	}
	return receipt, false, nil
}

func (s *Store) CompleteImport(source ImportSource, stage func(*pebble.Batch) error) error {
	return s.CompleteImports([]ImportSource{source}, stage)
}

func (s *Store) CompleteImports(sources []ImportSource, stage func(*pebble.Batch) error) error {
	if len(sources) == 0 || stage == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	receipts := make([]ImportReceipt, len(sources))
	keys := make([][]byte, len(sources))
	for i, source := range sources {
		keys[i] = importSourceKey(source)
		value, closer, err := s.db.Get(keys[i])
		if err != nil {
			return err
		}
		if err := closer.Close(); err != nil {
			return err
		}
		if err := json.Unmarshal(value, &receipts[i]); err != nil || receipts[i].Source != source {
			return ErrImportConflict
		}
		if receipts[i].State == ImportCompleted {
			continue
		}
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := stage(batch); err != nil {
		return err
	}
	reader := batch.Reader()
	for {
		kind, stagedKey, _, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if kind == pebble.InternalKeyKindRangeDelete && bytes.Compare(stagedKey, []byte(importPrefix+"\xff")) < 0 || kind != pebble.InternalKeyKindSet && kind != pebble.InternalKeyKindDelete && kind != pebble.InternalKeyKindRangeDelete || kind != pebble.InternalKeyKindRangeDelete && bytes.HasPrefix(stagedKey, []byte(importPrefix)) || strings.HasPrefix(string(stagedKey), metaPrefix) {
			return fmt.Errorf("history: forbidden import completion batch mutation")
		}
	}
	for i := range receipts {
		receipts[i].State = ImportCompleted
		completed, err := json.Marshal(receipts[i])
		if err != nil {
			return err
		}
		if err := batch.Set(keys[i], completed, nil); err != nil {
			return err
		}
	}
	return s.db.Commit(batch, store.SyncWrites)
}

func (s *Store) ListPendingImports(target string) ([]ImportReceipt, error) {
	if target == "" {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(importPrefix + "source/"), UpperBound: []byte(importPrefix + "source0")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var receipts []ImportReceipt
	for iter.First(); iter.Valid(); iter.Next() {
		var receipt ImportReceipt
		if err := json.Unmarshal(iter.Value(), &receipt); err != nil {
			return nil, err
		}
		if receipt.Target == target && receipt.State == ImportPending {
			receipts = append(receipts, receipt)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return receipts, nil
}
