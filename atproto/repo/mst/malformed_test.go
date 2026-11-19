package mst

// hypercerts: regression tests for structurally malformed MST nodes from
// untrusted CAR data (TECH-932 F2). Empty-key entries and child pointers
// without a preceding value must be rejected with ErrInvalidTree, never
// panic.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// An entry with an empty key is neither a value nor a child once loaded; it
// must be rejected when decoded, before later re-encoding would panic.
func TestNodeEmptyKeyRejected(t *testing.T) {
	assert := assert.New(t)

	nd := NodeData{
		Entries: []EntryData{{
			PrefixLen: 0,
			KeySuffix: []byte{},
			Value:     testEntryCid(t),
		}},
	}
	_, err := nd.Node(nil)
	assert.ErrorIs(err, ErrInvalidTree)
}

// A child pointer with no preceding key/value entry must return an error from
// encoding, not panic("malformed tree node").
func TestNodeDataChildWithoutValueIsError(t *testing.T) {
	assert := assert.New(t)

	c := testEntryCid(t)
	n := Node{
		Entries: []NodeEntry{{ChildCID: &c}, {ChildCID: &c}},
	}
	_, err := n.NodeData()
	assert.ErrorIs(err, ErrInvalidTree)
}
