package segment

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestAuditEpochWriterLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment")
	w, err := New(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	first, ok, err := ReadAuditEpoch(nil, path)
	if err != nil || !ok {
		t.Fatalf("create epoch: %v %v", ok, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = New(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := ReadAuditEpoch(nil, path)
	if err != nil || first == second {
		t.Fatalf("reopen epoch: %v", err)
	}
	if _, err := w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	beforeSeal, _, err := ReadAuditEpoch(nil, path)
	if err != nil || beforeSeal != second {
		t.Fatalf("append/flush changed epoch: %v", err)
	}
	if _, err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Path: path}); !errors.Is(err, ErrSegmentSealed) {
		t.Fatalf("sealed probe: %v", err)
	}
	after, _, err := ReadAuditEpoch(nil, path)
	if err != nil || after != second {
		t.Fatalf("sealed probe changed epoch: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	w, err = New(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	third, _, err := ReadAuditEpoch(nil, path)
	if err != nil || third == second {
		t.Fatalf("recreate epoch: %v", err)
	}
}

type epochRenameFailFS struct{ vfs.FS }

func (fs epochRenameFailFS) Rename(oldPath, newPath string) error {
	return errors.New("injected rename failure")
}

func TestAuditEpochFailureBeforeCreate(t *testing.T) {
	fs := epochRenameFailFS{vfs.NewMem()}
	path := "segment"
	if _, err := New(Config{Path: path, FS: fs}); err == nil {
		t.Fatal("expected epoch failure")
	}
	if _, err := fs.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("authoritative file created: %v", err)
	}
}

func TestAuditEpochReplacementAndNoops(t *testing.T) {
	path := sealedSegmentForReader(t, t.TempDir(), patchFixtureEvents(), 2)
	initial, _, err := ReadAuditEpoch(nil, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Patch(path, func(*Event) bool { return false }, PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Rewrite(path, func(*Event) RowDecision { return RowKeep }, RewriteOptions{}); err != nil {
		t.Fatal(err)
	}
	after, _, err := ReadAuditEpoch(nil, path)
	if err != nil || after != initial {
		t.Fatalf("no-op epoch: %v", err)
	}
	if _, err := Patch(path, func(ev *Event) bool { ev.IndexedAt++; return true }, PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	patched, _, err := ReadAuditEpoch(nil, path)
	if err != nil || patched == after {
		t.Fatalf("patch epoch: %v", err)
	}
	if _, err := Rewrite(path, func(*Event) RowDecision { return RowDrop }, RewriteOptions{}); err != nil {
		t.Fatal(err)
	}
	rewritten, _, err := ReadAuditEpoch(nil, path)
	if err != nil || rewritten == patched {
		t.Fatalf("rewrite epoch: %v", err)
	}
}

func TestEpochFailurePreventsAuthoritativeMutation(t *testing.T) {
	for _, fault := range []string{"short write", "file sync", "close", "rename", "dir sync"} {
		for _, operation := range []string{"create", "resume", "patch", "rewrite"} {
			t.Run(fault+"/"+operation, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "segment")
				fs := epochFaultFS{vfs.Default, fault}
				if operation == "create" {
					_, err := New(Config{Path: path, FS: fs})
					want := errEpochFault
					if fault == "short write" {
						want = io.ErrShortWrite
					}
					if !errors.Is(err, want) {
						t.Fatalf("error = %v, want %v", err, want)
					}
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Fatalf("authoritative segment should be absent: %v", err)
					}
					return
				}
				w, err := New(Config{Path: path})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}); err != nil {
					t.Fatal(err)
				}
				if operation == "resume" {
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.Write([]byte{1, 2, 3}); err != nil {
						t.Fatal(err)
					}
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := w.Seal(); err != nil {
						t.Fatal(err)
					}
				}
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				switch operation {
				case "resume":
					_, err = New(Config{Path: path, FS: fs})
				case "patch":
					_, err = Patch(path, func(e *Event) bool { e.IndexedAt++; return true }, PatchOptions{FS: fs})
				case "rewrite":
					_, err = Rewrite(path, func(*Event) RowDecision { return RowDrop }, RewriteOptions{FS: fs})
				}
				if err == nil {
					t.Fatal("expected epoch failure")
				}
				after, readErr := os.ReadFile(path)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("authoritative segment changed")
				}
			})
		}
	}
}

type epochCrashInjector struct {
	point string
	err   error
}

func (i epochCrashInjector) SimulateCrash(_ context.Context, point string) error {
	if point == i.point {
		return i.err
	}
	return nil
}
func TestReplacementCrashErrors(t *testing.T) {
	for _, point := range []string{CrashPointPatchRenamed, CrashPointPatchDirSynced, CrashPointRewriteRenamed, CrashPointRewriteDirSynced} {
		t.Run(point, func(t *testing.T) {
			path := sealedSegmentForReader(t, t.TempDir(), patchFixtureEvents(), 2)
			cause := errors.New("crash")
			injector := epochCrashInjector{point, cause}
			var err error
			switch point {
			case CrashPointPatchRenamed, CrashPointPatchDirSynced:
				_, err = Patch(path, func(e *Event) bool { e.IndexedAt++; return true }, PatchOptions{CrashInjector: injector})
			default:
				_, err = Rewrite(path, func(*Event) RowDecision { return RowDrop }, RewriteOptions{CrashInjector: injector})
			}
			var persistence *ReplacementPersistenceError
			if !errors.Is(err, cause) || !errors.As(err, &persistence) || persistence.Path != path || !persistence.MayHaveChanged {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
