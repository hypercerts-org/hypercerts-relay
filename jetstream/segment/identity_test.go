package segment

import (
	"encoding/binary"
	"errors"
	"github.com/cockroachdb/pebble/vfs"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestSealedAuditIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment")
	w, err := New(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSealedAuditIdentity(nil, path); !errors.Is(err, ErrActiveSegment) {
		t.Fatalf("active: %v", err)
	}
	if _, err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	id, err := ReadSealedAuditIdentity(nil, path)
	if err != nil || !id.HasEpoch || id.Size < ReservedHeaderBytes {
		t.Fatalf("sealed identity: %+v %v", id, err)
	}
	if err := os.Remove(path + ".audit-epoch"); err != nil {
		t.Fatal(err)
	}
	id, err = ReadSealedAuditIdentity(nil, path)
	if err != nil || id.HasEpoch {
		t.Fatalf("legacy identity: %+v %v", id, err)
	}
}

type identitySpyFS struct {
	vfs.FS
	path                     string
	segmentBytes, epochBytes int
	maxOffset                int64
}
type identitySpyFile struct {
	vfs.File
	spy   *identitySpyFS
	epoch bool
}

func (fs *identitySpyFS) Open(path string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(path, opts...)
	if err != nil {
		return nil, err
	}
	return identitySpyFile{f, fs, path != fs.path}, nil
}
func (f identitySpyFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if f.epoch {
		f.spy.epochBytes += n
	} else {
		f.spy.segmentBytes += n
	}
	return n, err
}
func (f identitySpyFile) ReadAt(p []byte, offset int64) (int, error) {
	n, err := f.File.ReadAt(p, offset)
	if f.epoch {
		f.spy.epochBytes += n
	} else {
		f.spy.segmentBytes += n
		if end := offset + int64(n); end > f.spy.maxOffset {
			f.spy.maxOffset = end
		}
	}
	return n, err
}
func TestSealedAuditIdentityMalformedSegmentBoundedReads(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]byte) []byte
		want   error
	}{
		{"short header", func(raw []byte) []byte { return raw[:ReservedHeaderBytes-1] }, ErrCorruptSegment},
		{"bad magic", func(raw []byte) []byte { raw[0] ^= 0xff; return raw }, ErrCorruptSegment},
		{"bad version", func(raw []byte) []byte { binary.LittleEndian.PutUint16(raw[12:14], 0); return raw }, ErrInvalidFooter},
		{"footer before header", func(raw []byte) []byte { binary.LittleEndian.PutUint64(raw[58:66], 0); return raw }, ErrInvalidFooter},
		{"footer beyond file", func(raw []byte) []byte { binary.LittleEndian.PutUint64(raw[58:66], uint64(len(raw))+1); return raw }, ErrInvalidFooter},
		{"mismatched block index", func(raw []byte) []byte { binary.LittleEndian.PutUint64(raw[90:98], 0); return raw }, ErrInvalidFooter},
		{"out of order sections", func(raw []byte) []byte { binary.LittleEndian.PutUint64(raw[66:74], 0); return raw }, ErrInvalidFooter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := sealedSegmentForReader(t, t.TempDir(), patchFixtureEvents(), 2)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, tc.mutate(raw), 0600); err != nil {
				t.Fatal(err)
			}
			spy := &identitySpyFS{FS: vfs.Default, path: path}
			if _, err := ReadSealedAuditIdentity(spy, path); !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if spy.segmentBytes > ReservedHeaderBytes || spy.maxOffset > ReservedHeaderBytes || spy.epochBytes != 0 {
				t.Fatalf("unbounded reads: segment=%d end=%d epoch=%d", spy.segmentBytes, spy.maxOffset, spy.epochBytes)
			}
		})
	}
}

func TestSealedAuditIdentityBoundedReadsAndMalformedEpoch(t *testing.T) {
	path := sealedSegmentForReader(t, t.TempDir(), patchFixtureEvents(), 2)
	spy := &identitySpyFS{FS: vfs.Default, path: path}
	if _, err := ReadSealedAuditIdentity(spy, path); err != nil {
		t.Fatal(err)
	}
	if spy.segmentBytes != ReservedHeaderBytes || spy.maxOffset != ReservedHeaderBytes || spy.epochBytes != 36 {
		t.Fatalf("unexpected reads: segment=%d end=%d epoch=%d", spy.segmentBytes, spy.maxOffset, spy.epochBytes)
	}
	if err := os.WriteFile(path+".audit-epoch", make([]byte, 1000), 0600); err != nil {
		t.Fatal(err)
	}
	spy.segmentBytes, spy.epochBytes = 0, 0
	if _, err := ReadSealedAuditIdentity(spy, path); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("malformed epoch: %v", err)
	}
	if spy.epochBytes != 37 {
		t.Fatalf("unbounded epoch read: %d", spy.epochBytes)
	}
}
