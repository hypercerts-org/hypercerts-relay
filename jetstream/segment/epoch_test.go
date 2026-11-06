package segment

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"

	"github.com/cockroachdb/pebble/vfs"
	"os"
	"path/filepath"
	"testing"
)

func TestAuditEpochEncoding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment")
	if _, ok, err := ReadAuditEpoch(nil, path); err != nil || ok {
		t.Fatalf("missing epoch: %v %v", ok, err)
	}
	first, err := BeginAuditMutation(nil, path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path + ".audit-epoch")
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 36 || binary.BigEndian.Uint32(raw[:4]) != 1 {
		t.Fatalf("invalid encoding: %x", raw)
	}
	got, ok, err := ReadAuditEpoch(nil, path)
	if err != nil || !ok || got != first {
		t.Fatalf("read: %v %v %v", got, ok, err)
	}
	second, err := BeginAuditMutation(nil, path)
	if err != nil || second == first {
		t.Fatalf("replacement: %v %v", second, err)
	}
	for _, malformed := range [][]byte{nil, raw[:35], append(append([]byte{}, raw...), 0), make([]byte, 36)} {
		if err := os.WriteFile(path+".audit-epoch", malformed, 0600); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := ReadAuditEpoch(nil, path); err == nil || ok {
			t.Fatalf("accepted malformed epoch %x", malformed)
		}
	}
}

var errEpochFault = errors.New("epoch fault")

type epochFaultFS struct {
	vfs.FS
	fault string
}

type epochFaultFile struct {
	vfs.File
	fault string
}

func (f epochFaultFile) Write(p []byte) (int, error) {
	if f.fault == "short write" {
		return len(p) - 1, nil
	}
	return f.File.Write(p)
}
func (f epochFaultFile) Sync() error {
	if f.fault == "file sync" || f.fault == "dir sync" {
		return errEpochFault
	}
	return f.File.Sync()
}
func (f epochFaultFile) Close() error {
	err := f.File.Close()
	if f.fault == "close" {
		return errEpochFault
	}
	return err
}
func (fs epochFaultFS) Create(path string) (vfs.File, error) {
	f, err := fs.FS.Create(path)
	if err != nil {
		return nil, err
	}
	if strings.Contains(path, ".audit-epoch.") && fs.fault != "dir sync" {
		return epochFaultFile{f, fs.fault}, nil
	}
	return f, nil
}
func (fs epochFaultFS) Rename(oldPath, newPath string) error {
	if strings.HasSuffix(newPath, ".audit-epoch") && fs.fault == "rename" {
		if err := fs.FS.Rename(oldPath, newPath); err != nil {
			return err
		}
		return errEpochFault
	}
	return fs.FS.Rename(oldPath, newPath)
}
func (fs epochFaultFS) OpenDir(path string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(path)
	if err != nil {
		return nil, err
	}
	if fs.fault == "dir sync" {
		return epochFaultFile{f, fs.fault}, nil
	}
	return f, nil
}

func TestAuditEpochPersistenceFailures(t *testing.T) {
	for _, fault := range []string{"short write", "file sync", "close", "rename", "dir sync"} {
		t.Run(fault, func(t *testing.T) {
			base := vfs.NewMem()
			first, err := BeginAuditMutation(base, "segment")
			if err != nil {
				t.Fatal(err)
			}
			_, err = BeginAuditMutation(epochFaultFS{base, fault}, "segment")
			want := errEpochFault
			if fault == "short write" {
				want = io.ErrShortWrite
			}
			if !errors.Is(err, want) {
				t.Fatalf("error = %v, want %v", err, want)
			}
			var persistence *ReplacementPersistenceError
			ambiguous := fault == "rename" || fault == "dir sync"
			if errors.As(err, &persistence) != ambiguous {
				t.Fatalf("unexpected persistence error: %v", err)
			}
			if ambiguous && (!persistence.MayHaveChanged || persistence.Path != "segment.audit-epoch") {
				t.Fatalf("bad persistence error: %+v", persistence)
			}
			after, ok, readErr := ReadAuditEpoch(base, "segment")
			if readErr != nil || !ok {
				t.Fatalf("read epoch: %v", readErr)
			}
			if (after != first) != ambiguous {
				t.Fatalf("epoch changed = %v, want %v", after != first, ambiguous)
			}
		})
	}
}
