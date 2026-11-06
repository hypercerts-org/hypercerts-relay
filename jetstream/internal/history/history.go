// Package history provides the durable, immutable journal for selected events.
// It is deliberately not wired into ingestion or subscriptions in this milestone.
package history

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

const (
	formatVersion    = 1
	metaPrefix       = "selected-history/v1/"
	keyStream        = metaPrefix + "stream-id"
	keyNext          = metaPrefix + "next-seq"
	keyApplied       = metaPrefix + "projection-applied-through"
	keyCompletion    = metaPrefix + "projection-completion/"
	keyTargetApplied = metaPrefix + "projection-target-applied/"
	keyActivation    = metaPrefix + "activation"
)

var (
	ErrConflict = errors.New("history: source coordinate has conflicting content")
	ErrInvalid  = errors.New("history: invalid event")
)

type Disposition string

const (
	Apply          Disposition = "apply"
	HistoricalOnly Disposition = "historical-only"
	Noop           Disposition = "noop"
)

// Policy is the exact admission-time policy; collection removal never rewrites it.
type Policy struct {
	Revision    uint64   `json:"revision"`
	Collections []string `json:"collections"`
}

// Source identifies an operation at its upstream coordinate. Epoch and PDS
// evidence are optional because a relay cursor alone cannot establish either.
type Source struct {
	Domain      string `json:"domain,omitempty"`
	Epoch       string `json:"epoch,omitempty"`
	Cursor      uint64 `json:"cursor"`
	Operation   uint32 `json:"operation"`
	Path        string `json:"path,omitempty"`
	ContentHash string `json:"contentHash"`
	PDS         string `json:"pds,omitempty"`
}

type Attribution struct {
	StreamID   string              `json:"streamId"`
	Policy     EffectivePolicy     `json:"policy"`
	Provenance string              `json:"provenance"`
	Boundary   AttributionBoundary `json:"boundary"`
	Source     *ImportedSource     `json:"source"`
}
type EffectivePolicy struct {
	Revision          uint64   `json:"revision"`
	Collections       []string `json:"collections"`
	EffectiveAfterSeq *uint64  `json:"effectiveAfterSeq"`
}
type AttributionBoundary struct {
	Seq             *uint64 `json:"seq"`
	SnapshotID      *string `json:"snapshotId"`
	JournalSequence *uint64 `json:"journalSequence"`
}
type ImportedSource struct {
	Domain             string              `json:"domain"`
	StreamID           string              `json:"streamId"`
	Seq                uint64              `json:"seq"`
	PolicyRev          *uint64             `json:"policyRev"`
	Provenance         string              `json:"provenance"`
	Boundary           AttributionBoundary `json:"boundary"`
	Policy             *EffectivePolicy    `json:"policy,omitempty"`
	WitnessedAt        int64               `json:"witnessedAt"`
	IndexedAt          int64               `json:"indexedAt"`
	SegmentFingerprint string              `json:"segmentFingerprint"`
}

type Event struct {
	Version     uint8            `json:"version"`
	Owner       ProjectionTarget `json:"projectionOwner"`
	Sequence    uint64           `json:"sequence"`
	Source      Source           `json:"source"`
	Kind        string           `json:"kind"`
	DID         string           `json:"did,omitempty"`
	Collection  string           `json:"collection,omitempty"`
	Revision    string           `json:"revision,omitempty"`
	Time        int64            `json:"time,omitempty"`
	InputEvent  segment.Event    `json:"inputEvent"`
	Original    []byte           `json:"original"`
	Policy      Policy           `json:"policy"`
	Provenance  string           `json:"provenance"`
	Disposition Disposition      `json:"projectionDisposition"`
	Boundary    uint64           `json:"projectionBoundary,omitempty"`
	Attribution Attribution      `json:"attribution"`
}

type Receipt struct {
	Sequence  uint64 `json:"sequence"`
	Duplicate bool   `json:"duplicate,omitempty"`
}
type Append struct {
	Owner                           ProjectionTarget
	Source                          Source
	Kind, DID, Collection, Revision string
	Time                            int64
	Original                        []byte
	InputEvent                      segment.Event
	Policy                          Policy
	Provenance                      string
	Disposition                     Disposition
	Boundary                        uint64
	Attribution                     Attribution
}

func (s *Store) LatestAppliedLifecycle(did string, kind segment.Kind, target ProjectionTarget) (sourceSequence int64, journalSequence uint64, err error) {
	if s == nil || did == "" || !validTarget(target) || (kind != segment.KindIdentity && kind != segment.KindAccount) {
		return 0, 0, ErrInvalid
	}
	tip, err := s.Tip()
	if err != nil {
		return 0, 0, err
	}
	var latestSource int64
	var latestJournal uint64
	for start := uint64(0); start < tip; {
		page, err := s.Read(start, tip, 512)
		if err != nil {
			return 0, 0, err
		}
		for _, row := range page.Events {
			if row.DID != did || row.InputEvent.Kind != kind || row.Disposition != Apply || row.Source.Cursor == 0 {
				continue
			}
			status, err := s.ProjectionStatusFor(target, row.Sequence)
			if err != nil {
				return 0, 0, err
			}
			if status != RecoveryComplete {
				continue
			}
			if row.Source.Cursor > uint64(latestSource) {
				latestSource = int64(row.Source.Cursor)
				latestJournal = row.Sequence
			}
		}
		if page.Next <= start {
			return 0, 0, fmt.Errorf("history: lifecycle scan did not advance")
		}
		start = page.Next
	}
	return latestSource, latestJournal, nil
}

func (s *Store) LatestAppliedLifecycleSourceSequence(did string, kind segment.Kind, target ProjectionTarget) (int64, error) {
	sourceSequence, _, err := s.LatestAppliedLifecycle(did, kind, target)
	return sourceSequence, err
}

type Activation struct {
	Generation         string `json:"generation"`
	ActivatedAt        int64  `json:"activatedAt"`
	JournalStart       uint64 `json:"journalStart"`
	HistoricalComplete bool   `json:"historicalComplete"`
	Retention          string `json:"retention"`
}

type Store struct {
	db     *store.Store
	mu     *sync.Mutex
	stream string
	notify chan struct{}
}

type notificationCoordinator struct {
	mu       *sync.Mutex
	notifyMu sync.Mutex
	ch       chan struct{}
}

var coordinators sync.Map // map[*store.Store]*notificationCoordinator

func coordinator(db *store.Store) *sync.Mutex { return coordinatorState(db).mu }

func coordinatorState(db *store.Store) *notificationCoordinator {
	state, _ := coordinators.LoadOrStore(db, &notificationCoordinator{mu: &sync.Mutex{}, ch: make(chan struct{})})
	return state.(*notificationCoordinator)
}

func notifyCommitted(db *store.Store) {
	state := coordinatorState(db)
	state.notifyMu.Lock()
	close(state.ch)
	state.ch = make(chan struct{})
	state.notifyMu.Unlock()
}

func coordinatorChannel(db *store.Store) chan struct{} {
	state := coordinatorState(db)
	state.notifyMu.Lock()
	defer state.notifyMu.Unlock()
	return state.ch
}

// Open initializes an independent random stream identity, or recovers it.
func Open(db *store.Store) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("history: database required")
	}
	s := &Store{db: db, mu: coordinator(db)}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, closer, err := db.Get([]byte(keyStream))
	if err == nil {
		defer closer.Close()
		if len(v) != 16 {
			return nil, fmt.Errorf("history: invalid stream identity")
		}
		s.stream = hex.EncodeToString(v)
		if _, c, err := db.Get([]byte(keyActivation)); err != nil {
			return nil, fmt.Errorf("history: activation metadata missing: %w", err)
		} else {
			c.Close()
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	} else {
		// The allocator is not authoritative for history existence: a rewound or
		// missing allocator must never allow an identity to be recreated over rows.
		tip, tipErr := scanTip(db)
		if tipErr != nil {
			return nil, tipErr
		}
		next, nextOK, nextErr := db.GetVersionedUint64LE(keyNext, byte(formatVersion))
		if nextErr != nil {
			return nil, nextErr
		}
		if tip != 0 || (nextOK && next != 1) {
			return nil, fmt.Errorf("history: missing stream identity with existing history")
		}
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return nil, err
		}
		b := db.NewBatch()
		defer b.Close()
		if err := b.Set([]byte(keyStream), id, nil); err != nil {
			return nil, err
		}
		if err := b.Set([]byte(keyNext), store.EncodeVersionedUint64LE(byte(formatVersion), 1), nil); err != nil {
			return nil, err
		}
		if err := b.Set([]byte(keyApplied), store.EncodeVersionedUint64LE(byte(formatVersion), 0), nil); err != nil {
			return nil, err
		}
		activation, err := json.Marshal(Activation{Generation: hex.EncodeToString(id), ActivatedAt: time.Now().UTC().UnixNano(), JournalStart: 1, HistoricalComplete: false, Retention: "indefinite"})
		if err != nil {
			return nil, err
		}
		if err := b.Set([]byte(keyActivation), activation, nil); err != nil {
			return nil, err
		}
		if err := db.Commit(b, store.SyncWrites); err != nil {
			return nil, err
		}
		s.stream = hex.EncodeToString(id)
	}
	s.notify = coordinatorChannel(db)
	next, ok, err := db.GetVersionedUint64LE(keyNext, byte(formatVersion))
	if err != nil {
		return nil, err
	}
	if !ok || next == 0 {
		return nil, fmt.Errorf("history: missing or invalid sequence allocator")
	}
	tip, err := readTip(db)
	if err != nil {
		return nil, err
	}
	if next != tip+1 {
		return nil, fmt.Errorf("history: sequence allocator %d does not follow tip %d", next, tip)
	}
	applied, ok, err := db.GetVersionedUint64LE(keyApplied, byte(formatVersion))
	if err != nil {
		return nil, err
	}
	if !ok || applied > tip {
		return nil, fmt.Errorf("history: invalid projection checkpoint")
	}
	for _, target := range []ProjectionTarget{TargetMain, TargetBootstrapLive} {
		if _, exists, err := db.GetVersionedUint64LE(string(targetAppliedKey(target)), byte(formatVersion)); err != nil {
			return nil, err
		} else if !exists {
			b := db.NewBatch()
			if err := b.Set(targetAppliedKey(target), store.EncodeVersionedUint64LE(byte(formatVersion), 0), nil); err != nil {
				b.Close()
				return nil, err
			}
			if err := db.Commit(b, store.SyncWrites); err != nil {
				b.Close()
				return nil, err
			}
			b.Close()
		}
	}
	s.notify = coordinatorChannel(db)
	return s, nil
}

// ValidateBinding ensures projection writes use the database that owns this journal.
func (s *Store) ValidateBinding(db *store.Store) error {
	if s == nil || db == nil || s.db != db {
		return ErrInvalid
	}
	return nil
}

func (s *Store) CaptureGeneration() string {
	return s.stream
}

func (s *Store) StreamID() string { return s.stream }

// LookupSource returns the immutable event receipt for an upstream source coordinate.
// Its content hash is deliberately not part of the lookup key so callers can
// detect conflicting redeliveries rather than treating them as new events.
func (s *Store) LookupSource(source Source) (Event, bool, error) {
	identity, err := json.Marshal(struct {
		Domain, Epoch string
		Cursor        uint64
		Operation     uint32
		Path          string
	}{source.Domain, source.Epoch, source.Cursor, source.Operation, source.Path})
	if err != nil {
		return Event{}, false, err
	}
	h := sha256.Sum256(identity)
	key := append([]byte(metaPrefix+"source/"), []byte(hex.EncodeToString(h[:]))...)
	value, closer, err := s.db.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		return Event{}, false, nil
	}
	if err != nil {
		return Event{}, false, err
	}
	defer closer.Close()
	var event Event
	if err := json.Unmarshal(value, &event); err != nil {
		return Event{}, false, fmt.Errorf("history: malformed source receipt: %w", err)
	}
	if event.Source.Domain != source.Domain || event.Source.Epoch != source.Epoch || event.Source.Cursor != source.Cursor || event.Source.Operation != source.Operation || event.Source.Path != source.Path {
		return Event{}, false, fmt.Errorf("history: source receipt identity mismatch")
	}
	if event.Source.ContentHash != source.ContentHash {
		return Event{}, false, ErrConflict
	}
	return event, true, nil
}

func scanTip(db interface {
	NewIter(*pebble.IterOptions) (*pebble.Iterator, error)
}) (uint64, error) {
	prefix := []byte(metaPrefix + "event/")
	upper := append(bytes.Clone(prefix), 0xff)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var tip uint64
	expected := uint64(1)
	for valid := it.First(); valid; valid = it.Next() {
		if len(it.Key()) != len(eventKey(expected)) {
			return 0, fmt.Errorf("history: malformed journal key")
		}
		seq := binary.BigEndian.Uint64(it.Key()[len(prefix):])
		if seq != expected {
			return 0, fmt.Errorf("history: journal gap at %d", expected)
		}
		if _, err := decodeEvent(it.Key(), it.Value(), seq); err != nil {
			return 0, err
		}
		tip, expected = seq, seq+1
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	return tip, nil
}

func decodeVersionedUint64(value []byte) (uint64, bool, error) {
	if len(value) != 9 {
		return 0, false, fmt.Errorf("history: invalid allocator length %d", len(value))
	}
	if value[0] != byte(formatVersion) {
		return 0, false, fmt.Errorf("history: unsupported allocator version %d", value[0])
	}
	return binary.LittleEndian.Uint64(value[1:]), true, nil
}

func readTip(db *store.Store) (uint64, error) {
	n, ok, err := db.GetVersionedUint64LE(keyNext, byte(formatVersion))
	if err != nil {
		return 0, err
	}
	if !ok || n == 0 {
		return 0, fmt.Errorf("history: invalid next sequence")
	}
	return n - 1, nil
}

func (s *Store) Tip() (uint64, error) { return readTip(s.db) }

// Verify checks every journal row and the allocator against one Pebble snapshot.
func (s *Store) Verify() error {
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	tip, err := scanTip(snapshot)
	if err != nil {
		return err
	}
	value, closer, err := snapshot.Get([]byte(keyNext))
	if err != nil {
		return err
	}
	next, ok, err := decodeVersionedUint64(value)
	closeErr := closer.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if !ok || next != tip+1 {
		return fmt.Errorf("history: sequence allocator %d does not follow tip %d", next, tip)
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Store) Append(a Append) (Receipt, error) {
	if a.Owner == "" {
		a.Owner = TargetMain
	}
	if !validTarget(a.Owner) {
		return Receipt{}, ErrInvalid
	}
	if _, _, err := s.LookupSource(a.Source); err != nil {
		return Receipt{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.Source.Cursor == 0 || a.Source.ContentHash == "" || a.Kind == "" || len(a.Original) == 0 || a.Policy.Revision == 0 || a.Attribution.StreamID == "" || a.Attribution.Policy.Revision != a.Policy.Revision || a.Attribution.Provenance != a.Provenance || a.Provenance == "" || a.Attribution.Policy.Collections == nil || !equalStrings(a.Attribution.Policy.Collections, a.Policy.Collections) || (a.Disposition != Noop && (a.InputEvent.Kind == 0 || a.InputEvent.DID != a.DID || a.InputEvent.Collection != a.Collection || a.InputEvent.Rev != a.Revision)) {
		return Receipt{}, ErrInvalid
	}
	actual := sha256.Sum256(a.Original)
	if hex.EncodeToString(actual[:]) != a.Source.ContentHash {
		return Receipt{}, fmt.Errorf("%w: content hash mismatch", ErrInvalid)
	}
	if a.Disposition != Apply && a.Disposition != HistoricalOnly && a.Disposition != Noop {
		return Receipt{}, ErrInvalid
	}
	identity, err := json.Marshal(struct {
		Domain, Epoch string
		Cursor        uint64
		Operation     uint32
		Path          string
	}{a.Source.Domain, a.Source.Epoch, a.Source.Cursor, a.Source.Operation, a.Source.Path})
	if err != nil {
		return Receipt{}, err
	}
	// Hash the canonical coordinate without depending on source revision alone.
	h := sha256.Sum256(identity)
	dedup := append([]byte(metaPrefix+"source/"), []byte(hex.EncodeToString(h[:]))...)
	if v, closer, err := s.db.Get(dedup); err == nil {
		defer closer.Close()
		var receipt Event
		if err := json.Unmarshal(v, &receipt); err != nil {
			return Receipt{}, fmt.Errorf("history: malformed dedup receipt: %w", err)
		}
		tip, err := readTip(s.db)
		if err != nil {
			return Receipt{}, err
		}
		if receipt.Sequence == 0 || receipt.Sequence > tip {
			return Receipt{}, fmt.Errorf("history: dedup receipt sequence outside journal tip")
		}
		row, closer, err := s.db.Get(eventKey(receipt.Sequence))
		if err != nil {
			return Receipt{}, fmt.Errorf("history: dedup receipt has no journal row: %w", err)
		}
		prior, decodeErr := decodeEvent(eventKey(receipt.Sequence), row, receipt.Sequence)
		closeErr := closer.Close()
		if decodeErr != nil {
			return Receipt{}, decodeErr
		}
		if closeErr != nil {
			return Receipt{}, closeErr
		}
		if !bytes.Equal(row, v) || receipt.Source != prior.Source || receipt.Version != prior.Version || receipt.Source.Domain != a.Source.Domain || receipt.Source.Epoch != a.Source.Epoch || receipt.Source.Cursor != a.Source.Cursor || receipt.Source.Operation != a.Source.Operation || receipt.Source.Path != a.Source.Path {
			return Receipt{}, fmt.Errorf("history: dedup receipt does not match journal row")
		}
		if prior.Source.ContentHash != a.Source.ContentHash || !bytes.Equal(prior.Original, a.Original) {
			return Receipt{}, ErrConflict
		}
		return Receipt{Sequence: prior.Sequence, Duplicate: true}, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return Receipt{}, err
	}
	next, ok, err := s.db.GetVersionedUint64LE(keyNext, byte(formatVersion))
	if err != nil {
		return Receipt{}, err
	}
	if !ok || next == 0 || next == ^uint64(0) {
		return Receipt{}, fmt.Errorf("history: sequence exhausted or uninitialized")
	}
	if a.Attribution.Boundary.JournalSequence != nil && *a.Attribution.Boundary.JournalSequence == 0 {
		a.Attribution.Boundary.JournalSequence = &next
	}
	if a.Attribution.Boundary.JournalSequence != nil {
		a.Boundary = *a.Attribution.Boundary.JournalSequence
	}
	e := Event{Version: formatVersion, Sequence: next, Owner: a.Owner, Source: a.Source, Kind: a.Kind, DID: a.DID, Collection: a.Collection, Revision: a.Revision, Time: a.Time, Original: bytes.Clone(a.Original), InputEvent: a.InputEvent, Policy: Policy{Revision: a.Policy.Revision, Collections: append([]string{}, a.Policy.Collections...)}, Provenance: a.Provenance, Disposition: a.Disposition, Boundary: a.Boundary, Attribution: a.Attribution}
	value, err := json.Marshal(e)
	if err != nil {
		return Receipt{}, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(eventKey(next), value, nil); err != nil {
		return Receipt{}, err
	}
	if err := b.Set(dedup, value, nil); err != nil {
		return Receipt{}, err
	}
	if err := b.Set([]byte(keyNext), store.EncodeVersionedUint64LE(byte(formatVersion), next+1), nil); err != nil {
		return Receipt{}, err
	}
	// The record itself is the durable pending-projection work item.
	if err := s.db.Commit(b, store.SyncWrites); err != nil {
		return Receipt{}, err
	}
	notifyCommitted(s.db)
	return Receipt{Sequence: next}, nil
}

func eventKey(seq uint64) []byte {
	prefix := metaPrefix + "event/"
	b := make([]byte, len(prefix)+8)
	copy(b, prefix)
	binary.BigEndian.PutUint64(b[len(prefix):], seq)
	return b
}

func decodeEvent(key, value []byte, expected uint64) (Event, error) {
	var e Event
	if err := json.Unmarshal(value, &e); err != nil {
		return e, fmt.Errorf("history: malformed event %d: %w", expected, err)
	}
	if len(key) != len(eventKey(expected)) || !bytes.Equal(key, eventKey(expected)) || e.Version != formatVersion || e.Sequence != expected || !validTarget(e.Owner) || e.Source.Cursor == 0 || e.Kind == "" || len(e.Original) == 0 || e.Policy.Revision == 0 || e.Attribution.StreamID == "" || e.Attribution.Policy.Revision != e.Policy.Revision || e.Attribution.Provenance != e.Provenance || e.Source.ContentHash != OriginalHash(e.Original) || (e.Disposition != Apply && e.Disposition != HistoricalOnly && e.Disposition != Noop) {
		return e, fmt.Errorf("history: invalid event row %d", expected)
	}
	return e, nil
}

type Page struct {
	Events []Event
	Tip    uint64
	Next   uint64
}

// Read returns events in (after, through], where through is fixed for the call.
func snapshotTip(snapshot *pebble.Snapshot) (uint64, error) {
	value, closer, err := snapshot.Get([]byte(keyNext))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, fmt.Errorf("history: invalid next sequence")
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	next, ok, err := decodeVersionedUint64(value)
	if err != nil {
		return 0, err
	}
	if !ok || next == 0 {
		return 0, fmt.Errorf("history: invalid next sequence")
	}
	return next - 1, nil
}

func (s *Store) Read(after, through uint64, limit int) (Page, error) {
	if limit <= 0 {
		return Page{}, fmt.Errorf("history: positive page limit required")
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	tip, err := snapshotTip(snapshot)
	if err != nil {
		return Page{}, err
	}
	if through == 0 || through > tip {
		through = tip
	}
	p := Page{Events: make([]Event, 0), Tip: through, Next: after}
	if after >= through {
		return p, nil
	}
	lo := eventKey(after + 1)
	hi := eventKey(through + 1)
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return Page{}, err
	}
	defer it.Close()
	expected := after + 1
	for valid := it.First(); valid; valid = it.Next() {
		var e Event
		e, err := decodeEvent(it.Key(), it.Value(), expected)
		if err != nil {
			return Page{}, err
		}
		p.Events = append(p.Events, e)
		p.Next = e.Sequence
		expected++
		if len(p.Events) == limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return Page{}, err
	}
	// A full page is intentionally truncated. Only reaching iterator end proves
	// that a missing sequence exists before the pinned through-coordinate.
	if len(p.Events) < limit && expected <= through {
		return Page{}, fmt.Errorf("history: missing journal event %d", expected)
	}
	return p, nil
}

func (s *Store) Event(sequence uint64) (Event, bool, error) {
	if sequence == 0 {
		return Event{}, false, ErrInvalid
	}
	page, err := s.Read(sequence-1, sequence, 1)
	if err != nil {
		return Event{}, false, err
	}
	if len(page.Events) != 1 {
		return Event{}, false, nil
	}
	return page.Events[0], true, nil
}

func (s *Store) AppliedThrough() (uint64, error) {
	n, ok, err := s.db.GetVersionedUint64LE(keyApplied, byte(formatVersion))
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("history: missing projection checkpoint")
	}
	return n, nil
}

func (s *Store) AppliedThroughFor(target ProjectionTarget) (uint64, error) {
	if !validTarget(target) {
		return 0, ErrInvalid
	}
	n, ok, err := s.db.GetVersionedUint64LE(string(targetAppliedKey(target)), byte(formatVersion))
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("history: missing projection checkpoint")
	}
	return n, nil
}

func (s *Store) Acknowledge(sequence uint64) error {
	current, err := s.AppliedThrough()
	if err != nil {
		return err
	}
	if sequence <= current {
		return nil
	}
	if sequence != current+1 {
		return fmt.Errorf("history: non-contiguous projection acknowledgment")
	}
	return fmt.Errorf("history: acknowledgment requires a durable typed projection intent")
}

func (s *Store) Notifications() <-chan struct{} { return coordinatorChannel(s.db) }

func (s *Store) Activation() (Activation, error) {
	value, closer, err := s.db.Get([]byte(keyActivation))
	if err != nil {
		return Activation{}, err
	}
	defer closer.Close()
	var activation Activation
	if err := json.Unmarshal(value, &activation); err != nil {
		return Activation{}, err
	}
	if activation.Generation != s.stream || activation.JournalStart != 1 || activation.ActivatedAt <= 0 || activation.Retention != "indefinite" || activation.HistoricalComplete {
		return Activation{}, fmt.Errorf("history: invalid activation metadata")
	}
	return activation, nil
}

// WatchRead captures a broadcast generation before reading its pinned journal tip.
// A commit racing the read closes the captured generation, so callers cannot miss it.
func (s *Store) WatchRead(after uint64, limit int) (Page, <-chan struct{}, error) {
	wake := coordinatorChannel(s.db)
	tip, err := s.Tip()
	if err != nil {
		return Page{}, wake, err
	}
	page, err := s.Read(after, tip, limit)
	return page, wake, err
}

// OriginalHash computes the stable content digest used in Source.
func OriginalHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
