// Package attribution stores versioned provenance metadata for selected-stream events.
package attribution

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

const prefix = "hypercerts/attribution/v1/"
const valueVersion byte = 2
const legacyValueVersion byte = 1

var (
	ErrNotFound      = errors.New("attribution not found")
	ErrLegacyUnknown = errors.New("legacy event attribution is unknown")
	ErrFingerprint   = errors.New("prepared event fingerprint mismatch")
	ErrInvalid       = errors.New("invalid attribution")
	ErrConflict      = errors.New("conflicting attribution coordinate")
)

type Provenance string

const (
	ProvenanceLive           Provenance = "live"
	ProvenanceSnapshot       Provenance = "snapshot"
	ProvenanceReconciliation Provenance = "reconciliation"
	ProvenanceLegacyUnknown  Provenance = "legacy-unknown"
)

type Boundary struct {
	Seq        *uint64 `json:"seq"`
	SnapshotID *string `json:"snapshotId"`
}
type Event struct {
	StreamID   string          `json:"streamId"`
	Seq        uint64          `json:"seq"`
	PolicyRev  *uint64         `json:"policyRev"`
	Provenance Provenance      `json:"provenance"`
	Boundary   Boundary        `json:"boundary"`
	Payload    json.RawMessage `json:"payload"`
	Source     *ImportedSource `json:"source,omitempty"`
}
type ImportedSource struct {
	Domain             string     `json:"domain"`
	StreamID           string     `json:"streamId"`
	Seq                uint64     `json:"seq"`
	PolicyRev          *uint64    `json:"policyRev"`
	Provenance         Provenance `json:"provenance"`
	Boundary           Boundary   `json:"boundary"`
	Policy             *Policy    `json:"policy,omitempty"`
	WitnessedAt        int64      `json:"witnessedAt"`
	IndexedAt          int64      `json:"indexedAt"`
	SegmentFingerprint string     `json:"segmentFingerprint"`
}
type Prepared struct {
	Event              Event   `json:"event"`
	Policy             *Policy `json:"policy,omitempty"`
	Fingerprint        string  `json:"fingerprint"`
	SegmentFingerprint string  `json:"segmentFingerprint,omitempty"`
}
type Policy struct {
	Revision          uint64   `json:"revision"`
	Collections       []string `json:"collections"`
	EffectiveAfterSeq *uint64  `json:"effectiveAfterSeq,omitempty"`
}
type DurableEvent struct {
	Fingerprint        string
	SegmentFingerprint string
	Exists             bool
}

var managerLocks sync.Map

type storeDomain struct {
	db     *store.Store
	domain string
}
type Manager struct {
	mu       *sync.Mutex
	db       *store.Store
	streamID string
	domain   string
}

func Open(db *store.Store, persistentCryptoID, domain string) (*Manager, error) {
	if db == nil || persistentCryptoID == "" || domain == "" {
		return nil, fmt.Errorf("%w: store, stream identity and domain required", ErrInvalid)
	}
	namespace := base64.RawURLEncoding.EncodeToString([]byte(domain))
	lock, _ := managerLocks.LoadOrStore(storeDomain{db: db, domain: namespace}, &sync.Mutex{})
	m := &Manager{mu: lock.(*sync.Mutex), db: db, streamID: persistentCryptoID, domain: namespace}
	v, c, err := db.Get(m.key("activation"))
	if err == nil {
		defer c.Close()
		var state struct {
			StreamID           string `json:"streamId"`
			FirstAttributedSeq uint64 `json:"firstAttributedSeq"`
		}
		if err = decode(v, &state); err != nil {
			return nil, err
		}
		if state.StreamID != persistentCryptoID || state.FirstAttributedSeq == 0 {
			return nil, ErrInvalid
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, err
	}
	return m, nil
}
func (m *Manager) key(parts ...string) []byte { return []byte(prefix + m.domain + "/" + join(parts)) }

// ValidateBinding ensures recovery is using the exact store and namespace
// that own this manager's attribution coordinates.
func (m *Manager) ValidateBinding(db *store.Store, domain string) error {
	if db != m.db || domain == "" || base64.RawURLEncoding.EncodeToString([]byte(domain)) != m.domain {
		return ErrInvalid
	}
	return nil
}

// PreparedSeqs returns the strictly validated sequence coordinates pending recovery.
func (m *Manager) PreparedSeqs() ([]uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lo := m.key("prepared")
	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: store.PrefixUpperBound(lo)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var seqs []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		seq := seqFromKey(key)
		if seq == 0 || len(key) != len(lo)+17 || string(key[len(lo)]) != "/" || seqHex(seq) != string(key[len(lo)+1:]) {
			return nil, ErrInvalid
		}
		var p Prepared
		if err := decode(iter.Value(), &p); err != nil {
			return nil, err
		}
		if p.Event.Seq != seq || p.Event.StreamID != m.streamID || Validate(p.Event) != nil || p.Fingerprint != fingerprint(p.Event) {
			return nil, ErrInvalid
		}
		seqs = append(seqs, seq)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// ActivationSeq returns the validated first sequence governed by attribution.
func (m *Manager) ActivationSeq() (uint64, error) {
	v, c, err := m.db.Get(m.key("activation"))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer c.Close()
	var state struct {
		StreamID           string `json:"streamId"`
		FirstAttributedSeq uint64 `json:"firstAttributedSeq"`
	}
	if err := decode(v, &state); err != nil {
		return 0, err
	}
	if state.StreamID != m.streamID || state.FirstAttributedSeq == 0 {
		return 0, ErrInvalid
	}
	return state.FirstAttributedSeq, nil
}
func join(parts []string) string {
	return string(bytes.Join(func() [][]byte {
		out := make([][]byte, len(parts))
		for i, p := range parts {
			out[i] = []byte(p)
		}
		return out
	}(), []byte("/")))
}
func seqHex(n uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return fmt.Sprintf("%x", b[:])
}
func (m *Manager) eventKey(seq uint64) []byte    { return m.key("event", seqHex(seq)) }
func (m *Manager) preparedKey(seq uint64) []byte { return m.key("prepared", seqHex(seq)) }
func (m *Manager) policyKey(rev uint64) []byte   { return m.key("policy", seqHex(rev)) }
func encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append([]byte{valueVersion}, b...), nil
}
func decode(b []byte, v any) error {
	if len(b) < 2 || (b[0] != valueVersion && b[0] != legacyValueVersion) {
		return fmt.Errorf("%w: unsupported or truncated value", ErrInvalid)
	}
	d := json.NewDecoder(bytes.NewReader(b[1:]))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("%w: trailing JSON data", ErrInvalid)
	}
	return nil
}

func FingerprintSegment(e segment.Event) string {
	b, _ := json.Marshal(struct {
		Seq         uint64       `json:"seq"`
		WitnessedAt int64        `json:"witnessedAt"`
		IndexedAt   int64        `json:"indexedAt"`
		Kind        segment.Kind `json:"kind"`
		Collection  string       `json:"collection"`
		DID         string       `json:"did"`
		Rkey        string       `json:"rkey"`
		Rev         string       `json:"rev"`
		Payload     []byte       `json:"payload"`
	}{e.Seq, e.WitnessedAt, e.IndexedAt, e.Kind, e.Collection, e.DID, e.Rkey, e.Rev, e.Payload})
	h := sha256.Sum256(b)
	return base64.RawURLEncoding.EncodeToString(h[:])
}
func fingerprint(e Event) string {
	b, _ := json.Marshal(e)
	h := sha256.Sum256(b)
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// Validate validates event metadata before it is persisted or assigned a sequence.
func Validate(e Event) error {
	if e.StreamID == "" || e.Seq == 0 || (e.PolicyRev != nil && *e.PolicyRev == 0) || len(e.Payload) == 0 || !json.Valid(e.Payload) {
		return ErrInvalid
	}
	switch e.Provenance {
	case ProvenanceLegacyUnknown:
		if e.PolicyRev != nil || e.Boundary.Seq != nil || e.Boundary.SnapshotID != nil {
			return ErrInvalid
		}
	case ProvenanceLive:
		if e.Boundary.Seq != nil || e.Boundary.SnapshotID != nil {
			return ErrInvalid
		}
	case ProvenanceSnapshot, ProvenanceReconciliation:
		if e.Boundary.Seq == nil || e.Boundary.SnapshotID == nil || *e.Boundary.SnapshotID == "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func uint64Ptr(v uint64) *uint64 { return &v }
func policyPtr(p Policy) *Policy { c := clonePolicy(p); return &c }
func cloneEvent(e Event) Event {
	e.Payload = bytes.Clone(e.Payload)
	if e.PolicyRev != nil {
		v := *e.PolicyRev
		e.PolicyRev = &v
	}
	if e.Source != nil {
		c := *e.Source
		if c.PolicyRev != nil {
			v := *c.PolicyRev
			c.PolicyRev = &v
		}
		if c.Policy != nil {
			c.Policy = policyPtr(*c.Policy)
		}
		e.Source = &c
	}
	return e
}

func clonePolicy(p Policy) Policy {
	p.Collections = append([]string(nil), p.Collections...)
	if p.EffectiveAfterSeq != nil {
		seq := *p.EffectiveAfterSeq
		p.EffectiveAfterSeq = &seq
	}
	return p
}
func (m *Manager) Prepare(e Event, policies ...Policy) (Prepared, error) {
	if e.StreamID != m.streamID || Validate(e) != nil || len(policies) > 1 {
		return Prepared{}, ErrInvalid
	}
	var policy *Policy
	if len(policies) == 1 {
		preparedPolicy := clonePolicy(policies[0])
		if !validPolicy(preparedPolicy) || e.PolicyRev == nil || preparedPolicy.Revision != *e.PolicyRev {
			return Prepared{}, ErrInvalid
		}
		policy = &preparedPolicy
	} else if e.PolicyRev != nil || e.Provenance != ProvenanceLegacyUnknown {
		return Prepared{}, ErrInvalid
	}
	p := Prepared{Event: cloneEvent(e), Policy: policy, Fingerprint: fingerprint(e)}
	b, err := encode(p)
	if err != nil {
		return Prepared{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return p, m.putCoordinate(m.preparedKey(e.Seq), b)
}

// ValidateSegment checks attribution metadata without persisting a prepared coordinate.
func ValidateSegment(e segment.Event, streamID string, policy Policy, provenance Provenance, boundary Boundary) error {
	if streamID == "" || e.Seq == 0 || !validPolicy(policy) {
		return ErrInvalid
	}
	payload, err := json.Marshal(struct {
		Operation string `json:"operation"`
	}{Operation: strconv.Itoa(int(e.Kind))})
	if err != nil {
		return err
	}
	if err := Validate(Event{StreamID: streamID, Seq: e.Seq, PolicyRev: uint64Ptr(policy.Revision), Provenance: provenance, Boundary: boundary, Payload: payload}); err != nil {
		return err
	}
	return nil
}

func cloneImportedSource(source *ImportedSource) *ImportedSource {
	if source == nil {
		return nil
	}
	clonedSource := *source
	if source.PolicyRev != nil {
		value := *source.PolicyRev
		clonedSource.PolicyRev = &value
	}
	if source.Policy != nil {
		policy := clonePolicy(*source.Policy)
		clonedSource.Policy = &policy
	}
	if source.Boundary.Seq != nil {
		value := *source.Boundary.Seq
		clonedSource.Boundary.Seq = &value
	}
	if source.Boundary.SnapshotID != nil {
		value := *source.Boundary.SnapshotID
		clonedSource.Boundary.SnapshotID = &value
	}
	return &clonedSource
}

func validEventPolicy(event Event, policy *Policy) bool {
	if policy == nil {
		return event.PolicyRev == nil && event.Provenance == ProvenanceLegacyUnknown
	}
	return event.PolicyRev != nil && validPolicy(*policy) && policy.Revision == *event.PolicyRev
}

func validateImportedSource(source *ImportedSource) error {
	if source.Domain == "" || source.StreamID == "" || source.Seq == 0 {
		return ErrInvalid
	}
	event := Event{StreamID: source.StreamID, Seq: source.Seq, PolicyRev: source.PolicyRev, Provenance: source.Provenance, Boundary: source.Boundary, Payload: json.RawMessage(`{"operation":"import"}`)}
	if Validate(event) != nil || source.SegmentFingerprint == "" || !validEventPolicy(event, source.Policy) {
		return ErrInvalid
	}
	return nil
}

func (m *Manager) PrepareSegment(e segment.Event, streamID string, policy Policy, provenance Provenance, boundary Boundary, sources ...*ImportedSource) (Prepared, error) {
	if len(sources) > 1 {
		return Prepared{}, ErrInvalid
	}
	var source *ImportedSource
	if len(sources) == 1 && sources[0] != nil {
		source = cloneImportedSource(sources[0])
		if err := validateImportedSource(source); err != nil {
			return Prepared{}, err
		}
	}
	payload, err := json.Marshal(struct {
		Operation string `json:"operation"`
	}{Operation: strconv.Itoa(int(e.Kind))})
	if err != nil {
		return Prepared{}, err
	}
	if streamID != m.streamID {
		return Prepared{}, ErrInvalid
	}
	event := Event{StreamID: streamID, Seq: e.Seq, PolicyRev: uint64Ptr(policy.Revision), Provenance: provenance, Boundary: boundary, Payload: payload, Source: source}
	if Validate(event) != nil || !validPolicy(policy) {
		return Prepared{}, ErrInvalid
	}
	p := Prepared{Event: event, Policy: policyPtr(policy), Fingerprint: fingerprint(event), SegmentFingerprint: FingerprintSegment(e)}
	encoded, err := encode(p)
	if err != nil {
		return Prepared{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err = m.putCoordinate(m.preparedKey(e.Seq), encoded); err != nil {
		return Prepared{}, err
	}
	return p, nil
}
func (m *Manager) putCoordinate(key, value []byte) error {
	old, c, err := m.db.Get(key)
	if err == nil {
		defer c.Close()
		if !bytes.Equal(old, value) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	return m.db.Set(key, value, store.SyncWrites)
}
func (m *Manager) Commit(p Prepared, policy Policy, commitMetadata func(*pebble.Batch) error) error {
	if p.Policy == nil || !validPolicy(policy) || p.Event.PolicyRev == nil || policy.Revision != *p.Event.PolicyRev || !bytes.Equal(mustEncode(p.Policy), mustEncode(policy)) {
		return ErrInvalid
	}
	return m.CommitBatch([]Prepared{p}, commitMetadata)
}
func mustEncode(v any) []byte { b, _ := json.Marshal(v); return b }
func (m *Manager) CommitBatch(prepared []Prepared, commitMetadata func(*pebble.Batch) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitBatchLocked(prepared, commitMetadata)
}
func (m *Manager) commitBatchLocked(prepared []Prepared, commitMetadata func(*pebble.Batch) error) error {
	if len(prepared) == 0 {
		return ErrInvalid
	}
	batch := m.db.NewBatch()
	defer batch.Close()
	minimumSeq, err := minimumPreparedSeq(prepared)
	if err != nil {
		return err
	}
	seenEvents, seenPolicies := make(map[uint64][]byte), make(map[uint64][]byte)
	for _, p := range prepared {
		if err := m.stagePreparedEntry(batch, p, seenEvents, seenPolicies); err != nil {
			return err
		}
	}
	if err := m.stageActivation(batch, minimumSeq); err != nil {
		return err
	}
	if commitMetadata != nil {
		if err := commitMetadata(batch); err != nil {
			return err
		}
	}
	return m.db.Commit(batch, store.SyncWrites)
}

func minimumPreparedSeq(prepared []Prepared) (uint64, error) {
	var minimum uint64
	for i, p := range prepared {
		if p.Event.Seq == 0 {
			return 0, ErrInvalid
		}
		if i == 0 || p.Event.Seq < minimum {
			minimum = p.Event.Seq
		}
	}
	return minimum, nil
}

func (m *Manager) encodePreparedEntry(p Prepared) ([]byte, []byte, error) {
	if p.Event.StreamID != m.streamID || Validate(p.Event) != nil || p.Fingerprint != fingerprint(p.Event) || !validEventPolicy(p.Event, p.Policy) {
		return nil, nil, ErrInvalid
	}
	event, err := encode(p.Event)
	if err != nil {
		return nil, nil, err
	}
	var policy []byte
	if p.Policy != nil {
		policy, err = encode(p.Policy)
		if err != nil {
			return nil, nil, err
		}
	}
	return event, policy, nil
}

func (m *Manager) stagePreparedEntry(batch *pebble.Batch, p Prepared, seenEvents, seenPolicies map[uint64][]byte) error {
	event, policy, err := m.encodePreparedEntry(p)
	if err != nil {
		return err
	}
	if prior, ok := seenEvents[p.Event.Seq]; ok && !bytes.Equal(prior, event) {
		return ErrConflict
	}
	seenEvents[p.Event.Seq] = event
	if p.Policy != nil {
		if prior, ok := seenPolicies[p.Policy.Revision]; ok && !bytes.Equal(prior, policy) {
			return ErrConflict
		}
		seenPolicies[p.Policy.Revision] = policy
	}
	if err := m.checkCoordinate(m.eventKey(p.Event.Seq), event); err != nil {
		return err
	}
	if p.Policy != nil {
		if err := m.checkCoordinate(m.policyKey(p.Policy.Revision), policy); err != nil {
			return err
		}
	}
	return m.stagePreparedCoordinates(batch, p, event, policy)
}

func (m *Manager) stagePreparedCoordinates(batch *pebble.Batch, p Prepared, event, policy []byte) error {
	if err := batch.Set(m.eventKey(p.Event.Seq), event, nil); err != nil {
		return err
	}
	if p.Policy != nil {
		if err := batch.Set(m.policyKey(p.Policy.Revision), policy, nil); err != nil {
			return err
		}
	}
	return batch.Delete(m.preparedKey(p.Event.Seq), nil)
}

func (m *Manager) stageActivation(batch *pebble.Batch, first uint64) error {
	if existing, closer, err := m.db.Get(m.key("activation")); err == nil {
		var state struct {
			StreamID           string `json:"streamId"`
			FirstAttributedSeq uint64 `json:"firstAttributedSeq"`
		}
		err = decode(existing, &state)
		_ = closer.Close()
		if err != nil {
			return err
		}
		if state.StreamID != m.streamID {
			return ErrInvalid
		}
		if state.FirstAttributedSeq < first {
			first = state.FirstAttributedSeq
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	activation, err := encode(struct {
		StreamID           string `json:"streamId"`
		FirstAttributedSeq uint64 `json:"firstAttributedSeq"`
	}{m.streamID, first})
	if err != nil {
		return err
	}
	return batch.Set(m.key("activation"), activation, nil)
}
func (m *Manager) atomicCommit(p Prepared, policy Policy, commitMetadata func(*pebble.Batch) error) error {
	if p.Policy == nil || p.Event.PolicyRev == nil || !validPolicy(policy) || policy.Revision != *p.Event.PolicyRev || !bytes.Equal(mustEncode(p.Policy), mustEncode(policy)) {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitLocked(p, policy, commitMetadata)
}
func (m *Manager) commitLocked(p Prepared, policy Policy, commitMetadata func(*pebble.Batch) error) error {
	if p.Policy == nil {
		if p.Event.PolicyRev != nil || p.Event.Provenance != ProvenanceLegacyUnknown {
			return ErrInvalid
		}
		return m.commitBatchLocked([]Prepared{p}, commitMetadata)
	}
	if !validPolicy(policy) || p.Event.PolicyRev == nil || policy.Revision != *p.Event.PolicyRev || !bytes.Equal(mustEncode(policy), mustEncode(p.Policy)) {
		return ErrInvalid
	}
	return m.commitBatchLocked([]Prepared{p}, commitMetadata)
}
func validPolicy(p Policy) bool {
	if p.Revision == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, s := range p.Collections {
		if s == "" || seen[s] {
			return false
		}
		seen[s] = true
	}
	return true
}
func (m *Manager) checkCoordinate(key, value []byte) error {
	old, c, err := m.db.Get(key)
	if err == nil {
		defer c.Close()
		if !bytes.Equal(old, value) {
			return ErrConflict
		}
		return nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	return err
}

type recoveryEntry struct {
	key      []byte
	prepared Prepared
}

func (m *Manager) Recover(durableLookup func(uint64) (DurableEvent, error), policyFor func(uint64) (Policy, error)) error {
	if durableLookup == nil {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entries, err := m.readRecoveryEntries()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := m.recoverEntry(entry, durableLookup, policyFor); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) readRecoveryEntries() ([]recoveryEntry, error) {
	lo := m.key("prepared")
	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: store.PrefixUpperBound(lo)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var entries []recoveryEntry
	for iter.First(); iter.Valid(); iter.Next() {
		var p Prepared
		if err := decode(iter.Value(), &p); err != nil {
			return nil, err
		}
		entries = append(entries, recoveryEntry{key: bytes.Clone(iter.Key()), prepared: p})
	}
	return entries, iter.Error()
}

func (m *Manager) recoverEntry(entry recoveryEntry, durableLookup func(uint64) (DurableEvent, error), policyFor func(uint64) (Policy, error)) error {
	p := entry.prepared
	if p.Event.StreamID != m.streamID || p.Event.Seq != seqFromKey(entry.key) || Validate(p.Event) != nil {
		return ErrInvalid
	}
	if p.Fingerprint != fingerprint(p.Event) {
		return ErrFingerprint
	}
	found, err := durableLookup(p.Event.Seq)
	if err != nil {
		return err
	}
	if !found.Exists {
		return m.db.Delete(entry.key, store.SyncWrites)
	}
	if err := validateDurableFingerprint(p, found); err != nil {
		return err
	}
	if p.Event.PolicyRev == nil && p.Policy == nil && p.Event.Provenance == ProvenanceLegacyUnknown {
		return m.commitLocked(p, Policy{}, nil)
	}
	policy, err := recoveryPolicy(&p, policyFor)
	if err != nil {
		return err
	}
	return m.commitLocked(p, policy, nil)
}

func validateDurableFingerprint(p Prepared, found DurableEvent) error {
	if p.SegmentFingerprint != "" {
		if found.SegmentFingerprint == "" || found.SegmentFingerprint != p.SegmentFingerprint {
			return ErrFingerprint
		}
	} else if found.Fingerprint != p.Fingerprint {
		return ErrFingerprint
	}
	return nil
}

func recoveryPolicy(p *Prepared, policyFor func(uint64) (Policy, error)) (Policy, error) {
	if p.Event.PolicyRev == nil {
		return Policy{}, ErrInvalid
	}
	var policy Policy
	if p.Policy != nil {
		policy = clonePolicy(*p.Policy)
	}
	if validPolicy(policy) && policy.Revision == *p.Event.PolicyRev {
		return policy, nil
	}
	if len(policy.Collections) != 0 || policy.Revision != 0 || policyFor == nil {
		return Policy{}, ErrInvalid
	}
	policy, err := policyFor(*p.Event.PolicyRev)
	if err != nil {
		return Policy{}, err
	}
	if !validPolicy(policy) || policy.Revision != *p.Event.PolicyRev {
		return Policy{}, ErrInvalid
	}
	p.Policy = policyPtr(policy)
	return policy, nil
}
func seqFromKey(key []byte) uint64 {
	const n = 16
	if len(key) < n {
		return 0
	}
	v, err := strconv.ParseUint(string(key[len(key)-n:]), 16, 64)
	if err != nil {
		return 0
	}
	return v
}

// LookupPolicy returns a defensive copy of the immutable policy recorded for revision.
func (m *Manager) LookupPolicy(revision uint64) (Policy, error) {
	if revision == 0 {
		return Policy{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, c, err := m.db.Get(m.policyKey(revision))
	if errors.Is(err, pebble.ErrNotFound) {
		return Policy{}, fmt.Errorf("%w: policy revision %d not found", ErrInvalid, revision)
	}
	if err != nil {
		return Policy{}, err
	}
	defer c.Close()
	var policy Policy
	if err := decode(v, &policy); err != nil {
		return Policy{}, err
	}
	if !validPolicy(policy) || policy.Revision != revision {
		return Policy{}, ErrInvalid
	}
	return clonePolicy(policy), nil
}

func (m *Manager) Lookup(seq uint64) (Event, error) {
	v, c, err := m.db.Get(m.eventKey(seq))
	if errors.Is(err, pebble.ErrNotFound) {
		a, ac, ae := m.db.Get(m.key("activation"))
		if errors.Is(ae, pebble.ErrNotFound) {
			return Event{StreamID: m.streamID, Seq: seq, Provenance: ProvenanceLegacyUnknown}, ErrLegacyUnknown
		}
		if ae != nil {
			return Event{}, ae
		}
		defer ac.Close()
		var state struct {
			StreamID           string `json:"streamId"`
			FirstAttributedSeq uint64 `json:"firstAttributedSeq"`
		}
		if ae = decode(a, &state); ae != nil {
			return Event{}, ae
		}
		if seq < state.FirstAttributedSeq {
			return Event{StreamID: m.streamID, Seq: seq, Provenance: ProvenanceLegacyUnknown}, ErrLegacyUnknown
		}
		return Event{}, fmt.Errorf("%w: sequence %d after activation boundary", ErrInvalid, seq)
	}
	if err != nil {
		return Event{}, err
	}
	defer c.Close()
	var e Event
	if err = decode(v, &e); err != nil {
		return Event{}, err
	}
	if e.StreamID != m.streamID || e.Seq != seq || Validate(e) != nil {
		return Event{}, ErrInvalid
	}
	return e, nil
}

// EnsureInitialPolicyHistory atomically records the current policy without
// activating event attribution. Existing matching history is preserved.
func (m *Manager) EnsureInitialPolicyHistory(current Policy, boundary *uint64) error {
	if !validPolicy(current) {
		return ErrInvalid
	}
	current = clonePolicy(current)
	current.EffectiveAfterSeq = clonePolicy(Policy{EffectiveAfterSeq: boundary}).EffectiveAfterSeq
	if prior, err := m.LookupPolicy(current.Revision); err == nil {
		if len(prior.Collections) != len(current.Collections) {
			return ErrConflict
		}
		for i := range current.Collections {
			if prior.Collections[i] != current.Collections[i] {
				return ErrConflict
			}
		}
		return nil
	} else if !errors.Is(err, ErrInvalid) {
		return err
	}
	return m.WithPolicyCommit(current, func(stage func(*pebble.Batch) error) error {
		batch := m.db.NewBatch()
		defer batch.Close()
		if err := stage(batch); err != nil {
			return err
		}
		return m.db.Commit(batch, store.SyncWrites)
	})
}

// WithPolicyCommit holds the attribution manager lock across the selection
// update and its durable commit, and binds policy staging to that commit batch.
func (m *Manager) WithPolicyCommit(policy Policy, fn func(func(*pebble.Batch) error) error) error {
	if fn == nil || !validPolicy(policy) {
		return ErrInvalid
	}
	policy = clonePolicy(policy)
	sort.Strings(policy.Collections)
	if !validPolicy(policy) {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	staged, active := false, true
	stage := func(batch *pebble.Batch) error {
		if !active || staged || batch == nil {
			return ErrInvalid
		}
		staged = true
		active = false
		return m.stagePolicy(batch, policy)
	}
	if err := fn(stage); err != nil {
		return err
	}
	if !staged {
		return ErrInvalid
	}
	return nil
}

func checkPolicyBatchConflict(batch *pebble.Batch, target []byte) error {
	reader := batch.Reader()
	for {
		kind, key, value, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if bytes.Equal(key, target) || (kind == pebble.InternalKeyKindRangeDelete && bytes.Compare(key, target) <= 0 && bytes.Compare(target, value) < 0) {
			return ErrConflict
		}
	}
}

func (m *Manager) stagePolicy(batch *pebble.Batch, policy Policy) error {
	target := m.policyKey(policy.Revision)
	if err := checkPolicyBatchConflict(batch, target); err != nil {
		return err
	}
	existing, closer, err := m.db.Get(target)
	if err == nil {
		defer closer.Close()
		var prior Policy
		if err := decode(existing, &prior); err != nil {
			return err
		}
		if !validPolicy(prior) {
			return ErrInvalid
		}
		if !policiesEqual(prior, policy) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	encoded, err := encode(policy)
	if err != nil {
		return err
	}
	return batch.Set(target, encoded, nil)
}

func optionalSeqEqual(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func policiesEqual(a, b Policy) bool {
	if a.Revision != b.Revision || len(a.Collections) != len(b.Collections) || !optionalSeqEqual(a.EffectiveAfterSeq, b.EffectiveAfterSeq) {
		return false
	}
	for i := range a.Collections {
		if a.Collections[i] != b.Collections[i] {
			return false
		}
	}
	return true
}

func (m *Manager) ReadPolicyHistory() ([]Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lo := m.key("policy")
	iter, err := m.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: store.PrefixUpperBound(lo)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []Policy
	for iter.First(); iter.Valid(); iter.Next() {
		var p Policy
		if err = decode(iter.Value(), &p); err != nil {
			return nil, err
		}
		if !validPolicy(p) || !bytes.Equal(iter.Key(), m.policyKey(p.Revision)) {
			return nil, ErrInvalid
		}
		out = append(out, p)
	}
	return out, iter.Error()
}
