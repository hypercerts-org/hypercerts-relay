package tombstone

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
)

const scopedSyncKey = "hypercertsSelectedSync"

// ErrScopedSyncEncode wraps every EncodeScopedSync failure. The #sync payload
// is upstream-controlled: a payload the scoped marker cannot represent (for
// example blocks above the CBOR codec's byte-string limit) is unrepresentable
// input, and callers at the live-event boundary drop the whole event instead
// of failing the process.
var ErrScopedSyncEncode = errors.New("tombstone: scoped sync marker could not be encoded")

type SyncScope struct {
	Collections []string
	Paths       []string
}

func EncodeScopedSync(payload []byte, scope SyncScope) ([]byte, error) {
	// The collections come from the local collection policy. If they cannot
	// be represented on their own, that is a local fault and must not be
	// reported as unrepresentable upstream input. The paths come from the
	// upstream replacement rows and are covered by the checks below.
	if err := checkScopeRepresentable(SyncScope{Collections: scope.Collections}); err != nil {
		return nil, err
	}
	decoded, err := cbor.Unmarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScopedSyncEncode, err)
	}
	fields, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: invalid sync payload", ErrScopedSyncEncode)
	}
	collections := make([]any, len(scope.Collections))
	for i, collection := range scope.Collections {
		collections[i] = collection
	}
	paths := make([]any, len(scope.Paths))
	for i, path := range scope.Paths {
		paths[i] = path
	}
	fields[scopedSyncKey] = map[string]any{"collections": collections, "paths": paths}
	out, err := cbor.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScopedSyncEncode, err)
	}
	// The marker is only useful if every reader can recover the scope from it.
	decodedScope, err := DecodeScopedSync(&segment.Event{Kind: segment.KindSync, Payload: out})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScopedSyncEncode, err)
	}
	if decodedScope == nil || len(decodedScope.Collections) != len(scope.Collections) || len(decodedScope.Paths) != len(scope.Paths) {
		return nil, fmt.Errorf("%w: scope does not round-trip", ErrScopedSyncEncode)
	}
	return out, nil
}

func scopeFields(scope SyncScope) map[string]any {
	collections := make([]any, len(scope.Collections))
	for i, collection := range scope.Collections {
		collections[i] = collection
	}
	paths := make([]any, len(scope.Paths))
	for i, path := range scope.Paths {
		paths[i] = path
	}
	return map[string]any{"collections": collections, "paths": paths}
}

func checkScopeRepresentable(scope SyncScope) error {
	out, err := cbor.Marshal(map[string]any{scopedSyncKey: scopeFields(scope)})
	if err != nil {
		return fmt.Errorf("tombstone: sync scope cannot be encoded: %w", err)
	}
	decoded, err := DecodeScopedSync(&segment.Event{Kind: segment.KindSync, Payload: out})
	if err != nil {
		return fmt.Errorf("tombstone: sync scope cannot be decoded: %w", err)
	}
	if decoded == nil || len(decoded.Collections) != len(scope.Collections) || len(decoded.Paths) != len(scope.Paths) {
		return errors.New("tombstone: sync scope does not round-trip")
	}
	return nil
}

func DecodeScopedSync(event *segment.Event) (*SyncScope, error) {
	if event.Kind != segment.KindSync || len(event.Payload) == 0 {
		return nil, nil
	}
	decoded, err := cbor.Unmarshal(event.Payload)
	if err != nil {
		return nil, err
	}
	fields, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid sync payload")
	}
	raw, exists := fields[scopedSyncKey]
	if !exists {
		return nil, nil
	}
	scopeFields, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid selected sync scope")
	}
	scope := &SyncScope{}
	for name, target := range map[string]*[]string{"collections": &scope.Collections, "paths": &scope.Paths} {
		values, ok := scopeFields[name].([]any)
		if !ok {
			return nil, fmt.Errorf("invalid selected sync %s", name)
		}
		for _, value := range values {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("invalid selected sync %s", name)
			}
			*target = append(*target, text)
		}
	}
	return scope, nil
}
