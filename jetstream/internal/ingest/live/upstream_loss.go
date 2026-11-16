package live

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

const upstreamLossKey = "hypercerts/selected/upstream-loss/v1"

// ErrUpstreamLoss identifies an upstream continuity loss for callers that surface it.
var ErrUpstreamLoss = errors.New("upstream continuity loss")

// UpstreamLoss describes the first detected loss of upstream continuity.
type UpstreamLoss struct {
	Version    int    `json:"version"`
	Reason     string `json:"reason"`
	CursorKey  string `json:"cursorKey"`
	SafeCursor int64  `json:"safeCursor"`
	Expected   int64  `json:"expected,omitempty"`
	Got        int64  `json:"got,omitempty"`
}

// upstreamLossMu serializes all marker read-modify-writes in this process.
// Pebble holds an exclusive lock on the underlying database across processes.
var upstreamLossMu sync.Mutex

func validateUpstreamLoss(loss UpstreamLoss) error {
	if loss.Version != 1 || loss.CursorKey == "" || loss.SafeCursor < 0 {
		return errors.New("invalid upstream loss fields")
	}
	switch loss.Reason {
	case "outdated_cursor":
	case "sequence_gap":
		if loss.Expected < 0 || loss.Got <= loss.Expected {
			return errors.New("invalid upstream loss fields")
		}
	default:
		return errors.New("invalid upstream loss reason")
	}
	return nil
}

// ReadUpstreamLoss reads the durable marker, returning nil when absent.
func ReadUpstreamLoss(db *store.Store) (*UpstreamLoss, error) {
	value, closer, err := db.Get([]byte(upstreamLossKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read upstream loss: %w", err)
	}
	defer func() { _ = closer.Close() }()

	decoder := json.NewDecoder(bytes.NewReader(value))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("invalid upstream loss JSON")
	}
	var loss UpstreamLoss
	seen := make(map[string]bool)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, errors.New("invalid upstream loss JSON")
		}
		name, ok := token.(string)
		if !ok || seen[name] {
			return nil, errors.New("invalid upstream loss field")
		}
		seen[name] = true
		var field any
		switch name {
		case "version":
			field = &loss.Version
		case "reason":
			field = &loss.Reason
		case "cursorKey":
			field = &loss.CursorKey
		case "safeCursor":
			field = &loss.SafeCursor
		case "expected":
			field = &loss.Expected
		case "got":
			field = &loss.Got
		default:
			return nil, errors.New("invalid upstream loss field")
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, field) != nil {
			return nil, errors.New("invalid upstream loss field")
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("invalid upstream loss JSON")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, errors.New("invalid upstream loss JSON")
	}
	if !seen["version"] || !seen["reason"] || !seen["cursorKey"] || !seen["safeCursor"] || (loss.Reason == "sequence_gap" && !seen["got"]) {
		return nil, errors.New("missing upstream loss field")
	}
	if err := validateUpstreamLoss(loss); err != nil {
		return nil, err
	}
	return &loss, nil
}

// RecordUpstreamLoss durably records a marker, preserving the first one.
func RecordUpstreamLoss(db *store.Store, loss UpstreamLoss) error {
	if err := validateUpstreamLoss(loss); err != nil {
		return err
	}
	_, err := writeUpstreamLoss(db, loss)
	return err
}

// ErrUpstreamLossPending is returned instead of a marker when a loss was
// already recorded, including by an earlier process.
var ErrUpstreamLossPending = errors.New("upstream continuity loss already recorded")

// RecordUpstreamLossOnce records the first marker, returning nil for it, and
// reports any marker that already existed so a restart cannot resume ingestion
// past a loss recorded by an earlier process.
func RecordUpstreamLossOnce(db *store.Store, loss UpstreamLoss) error {
	if err := validateUpstreamLoss(loss); err != nil {
		return err
	}
	previous, err := writeUpstreamLoss(db, loss)
	if err != nil {
		return err
	}
	if previous != nil {
		return fmt.Errorf("%w: %w: %s", ErrUpstreamLoss, ErrUpstreamLossPending, previous.Reason)
	}
	return nil
}

func writeUpstreamLoss(db *store.Store, loss UpstreamLoss) (*UpstreamLoss, error) {
	upstreamLossMu.Lock()
	defer upstreamLossMu.Unlock()
	previous, err := ReadUpstreamLoss(db)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		return previous, nil
	}
	encoded, err := json.Marshal(loss)
	if err != nil {
		return nil, fmt.Errorf("encode upstream loss: %w", err)
	}
	if err := db.Set([]byte(upstreamLossKey), encoded, store.SyncWrites); err != nil {
		return nil, fmt.Errorf("write upstream loss: %w", err)
	}
	return nil, nil
}
