package tombstone

import (
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
)

const scopedSyncKey = "hypercertsSelectedSync"

type SyncScope struct {
	Collections []string
	Paths       []string
}

func EncodeScopedSync(payload []byte, scope SyncScope) ([]byte, error) {
	decoded, err := cbor.Unmarshal(payload)
	if err != nil {
		return nil, err
	}
	fields, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid sync payload")
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
	return cbor.Marshal(fields)
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
