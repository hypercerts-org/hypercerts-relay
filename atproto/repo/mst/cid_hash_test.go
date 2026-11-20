package mst

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// hypercerts: ValidateCIDHash checks the hash only, for either CID version.
func TestValidateCIDHash(t *testing.T) {
	sum := func(code uint64, length int) multihash.Multihash {
		h, err := multihash.Sum([]byte("data"), code, length)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	sha256 := sum(multihash.SHA2_256, -1)

	for name, c := range map[string]cid.Cid{
		"v1 dag-cbor": cid.NewCidV1(cid.DagCBOR, sha256),
		"v1 raw":      cid.NewCidV1(cid.Raw, sha256),
		"v0":          cid.NewCidV0(sha256),
	} {
		if err := ValidateCIDHash(c); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, c := range map[string]cid.Cid{
		"sha2-512":         cid.NewCidV1(cid.DagCBOR, sum(multihash.SHA2_512, -1)),
		"truncated digest": cid.NewCidV1(cid.DagCBOR, sum(multihash.SHA2_256, 16)),
	} {
		if err := ValidateCIDHash(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
