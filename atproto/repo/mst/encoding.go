package mst

import (
	"bytes"
	"context"
	"fmt"
	"io"

	blockstore "github.com/ipfs/boxo/blockstore" // hypercerts: replace incompatible legacy go-ipfs-blockstore dependency
	bf "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
)

// CBOR serialization struct for a MST tree node. MST tree node as gets serialized to CBOR. Note that the CBOR fields are all single-character.
type NodeData struct {
	Left    *cid.Cid    `cborgen:"l"` // [nullable] pointer to lower-level subtree to the "left" of this path/key
	Entries []EntryData `cborgen:"e"` // ordered list of entries at this node
}

// CBOR serialization struct for a single entry within a `NodeData` entry list.
type EntryData struct {
	PrefixLen int64    `cborgen:"p"` // count of characters shared with previous path/key in tree
	KeySuffix []byte   `cborgen:"k"` // remaining part of path/key (appended to "previous key")
	Value     cid.Cid  `cborgen:"v"` // CID pointer at this path/key
	Right     *cid.Cid `cborgen:"t"` // [nullable] pointer to lower-level subtree to the "right" of this path/key entry
}

// Encodes a single `NodeData` struct as CBOR bytes. Does not recursively encode or update children.
func (d *NodeData) Bytes() ([]byte, *cid.Cid, error) {
	buf := new(bytes.Buffer)
	if err := d.MarshalCBOR(buf); err != nil {
		return nil, nil, err
	}
	b := buf.Bytes()
	builder := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256)
	c, err := builder.Sum(b)
	if err != nil {
		return nil, nil, err
	}
	return b, &c, nil
}

// Parses CBOR bytes in to `NodeData` struct
func NodeDataFromCBOR(r io.Reader) (*NodeData, error) {
	var nd NodeData
	if err := nd.UnmarshalCBOR(r); err != nil {
		return nil, err
	}
	// hypercerts: reject weak or truncated hashes in node links before any lookup or traversal.
	if nd.Left != nil {
		if err := validateLinkCID(*nd.Left); err != nil {
			return nil, err
		}
	}
	for _, e := range nd.Entries {
		if err := validateLinkCID(e.Value); err != nil {
			return nil, err
		}
		if e.Right != nil {
			if err := validateLinkCID(*e.Right); err != nil {
				return nil, err
			}
		}
	}
	// TODO: verify "non-empty" here?
	return &nd, nil
}

// hypercerts: validateLinkCID requires the 32-byte sha2-256 multihash that atproto repos use for CID links in decoded MST nodes. Mirrors validateRepoCID in the parent repo package.
func validateLinkCID(c cid.Cid) error {
	p := c.Prefix()
	if p.MhType != multihash.SHA2_256 || p.MhLength != 32 {
		return fmt.Errorf("%w: unsupported CID hash (must be 32-byte sha2-256): %s", ErrInvalidTree, c)
	}
	return nil
}

// Transforms `Node` struct to `NodeData`, which is the format used for encoding to CBOR.
//
// Child entries must have their CID computed first.
//
// hypercerts: returns a wrapped [ErrInvalidTree] if the entry list is malformed (a child pointer which does not follow a key/value entry), instead of panicking on untrusted tree shapes.
func (n *Node) NodeData() (NodeData, error) {
	d := NodeData{
		Left:    nil,
		Entries: []EntryData{}, // TODO perf: pre-allocate an array
	}

	prevKey := []byte{}
	for i, e := range n.Entries {
		if i == 0 && e.IsChild() {
			d.Left = e.ChildCID
			continue
		}
		if e.IsChild() {
			if len(d.Entries) == 0 {
				return NodeData{}, fmt.Errorf("%w: malformed tree node", ErrInvalidTree)
			}
			d.Entries[len(d.Entries)-1].Right = e.ChildCID
		}
		if e.IsValue() {
			idx := int64(CountPrefixLen(prevKey, e.Key))
			d.Entries = append(d.Entries, EntryData{
				PrefixLen: idx,
				KeySuffix: e.Key[idx:],
				Value:     *e.Value,
				Right:     nil,
			})
			prevKey = e.Key
		}
	}
	return d, nil
}

// Transforms an encoded [NodeData] to [Node] data structure format.
//
// Returns a wrapped [ErrInvalidTree] if the data is invalid. For example, entries with negative or out-of-bound prefix lenths ('p' field).
//
// c: optional CID argument for the CID of the CBOR representation of the NodeData
func (d *NodeData) Node(c *cid.Cid) (Node, error) {
	height := -1
	n := Node{
		CID:     c,
		Dirty:   c == nil,
		Entries: []NodeEntry{}, // TODO: pre-allocate
	}

	if d.Left != nil {
		n.Entries = append(n.Entries, NodeEntry{ChildCID: d.Left})
	}

	var prevKey []byte
	for _, e := range d.Entries {
		if e.PrefixLen < 0 || e.PrefixLen > int64(len(prevKey)) {
			return n, fmt.Errorf("%w: node entry with out of range prefix length (%d)", ErrInvalidTree, e.PrefixLen)
		}
		// TODO perf: pre-allocate
		key := []byte{}
		key = append(key, prevKey[:e.PrefixLen]...)
		key = append(key, e.KeySuffix...)
		// hypercerts: an empty key is neither a value nor a child once loaded, and later re-encoding panics; reject it at decode time.
		if len(key) == 0 {
			return n, fmt.Errorf("%w: node entry with empty key", ErrInvalidTree)
		}
		n.Entries = append(n.Entries, NodeEntry{
			Key:   key,
			Value: &e.Value,
		})
		prevKey = key
		if height < 0 {
			height = HeightForKey(key)
		}

		if e.Right != nil {
			n.Entries = append(n.Entries, NodeEntry{
				ChildCID: e.Right,
			})
		}
	}

	// TODO: height doesn't get set properly if this is an intermediate node; we rely on `EnsureHeights` getting called to fix that
	n.Height = height
	return n, nil
}

// TODO: this feels like a hack, and easy to forget
func (n *Node) ensureHeights() {
	if n.Height <= 0 {
		return
	}
	for _, e := range n.Entries {
		if e.Child != nil {
			if n.Height > 0 && e.Child.Height < 0 {
				e.Child.Height = n.Height - 1
			}
			e.Child.ensureHeights()
		}
	}
}

// Recursively encodes sub-tree, optionally writing to blockstore. Returns root CID.
//
// This method will not error if tree is partial.
//
// bs: is an optional blockstore; if it is nil, blocks will not be written.
// onlyDirty: is an optional blockstore; if it is nil, blocks will not be written.
func (n *Node) writeBlocks(ctx context.Context, bs blockstore.Blockstore, onlyDirty bool) (*cid.Cid, error) {
	if n == nil || n.Stub {
		return nil, fmt.Errorf("%w: nil tree node", ErrInvalidTree)
	}
	if onlyDirty && !n.Dirty && n.CID != nil {
		return n.CID, nil
	}

	// walk all children first
	for i, e := range n.Entries {
		if e.IsValue() && e.Dirty {
			// TODO: should we actually clear this here?
			e.Dirty = false
		}
		if !e.IsChild() {
			continue
		}
		if e.Child != nil && (e.Dirty || e.Child.Dirty || !onlyDirty) {
			cc, err := e.Child.writeBlocks(ctx, bs, onlyDirty)
			if err != nil {
				return nil, err
			}
			n.Entries[i].ChildCID = cc
			n.Entries[i].Dirty = false
		}
	}

	// compute this block
	// hypercerts: NodeData errors on malformed entry lists instead of panicking (untrusted tree shapes).
	nd, err := n.NodeData()
	if err != nil {
		return nil, err
	}
	b, c, err := nd.Bytes()
	if err != nil {
		return nil, err
	}

	n.CID = c
	n.Dirty = false

	if bs != nil {
		blk, err := bf.NewBlockWithCid(b, *c)
		if err != nil {
			return nil, err
		}
		if err := bs.Put(ctx, blk); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// hypercerts: key heights are at most 128 (the count of leading zero bit-pairs in a SHA-256 hash, see HeightForKey), and each child node is exactly one level below its parent, so a well-formed tree has at most 129 levels of nodes.
const maxTreeLevels = 129

// Recursively loads the sub-tree rooted at `ref`.
//
// hypercerts: depth and seen bound the work done on untrusted blocks.
//
// depth: level of `ref` in the tree being loaded, starting at 1 for the root
// seen:CIDs of every node referenced so far while loading this tree. A well-formed tree never references the same node twice (it would contain duplicate keys), and refusing repeats keeps the work bounded by the number of distinct blocks.
func loadNodeFromStore(ctx context.Context, bs MSTBlockSource, ref cid.Cid, depth int, seen map[string]struct{}) (*Node, error) {
	if depth > maxTreeLevels {
		return nil, fmt.Errorf("%w: tree is deeper than %d levels", ErrInvalidTree, maxTreeLevels)
	}
	key := ref.KeyString()
	if _, ok := seen[key]; ok {
		return nil, fmt.Errorf("%w: node referenced more than once: %s", ErrInvalidTree, ref)
	}
	seen[key] = struct{}{}

	block, err := bs.Get(ctx, ref)
	if err != nil {
		return nil, err
	}

	nd, err := NodeDataFromCBOR(bytes.NewReader(block.RawData()))
	if err != nil {
		return nil, err
	}

	n, err := nd.Node(&ref)
	if err != nil {
		return nil, err
	}

	for i, e := range n.Entries {
		if e.IsChild() {
			child, err := loadNodeFromStore(ctx, bs, *e.ChildCID, depth+1, seen)
			if err != nil && ipld.IsNotFound(err) {
				// allow "partial" trees
				continue
			}
			if err != nil {
				return nil, err
			}
			n.Entries[i].Child = child
			// NOTE: this is kind of a hack
			if n.Height == -1 && child.Height >= 0 {
				n.Height = child.Height + 1
			}
		}
	}

	return &n, nil
}
