package segment

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

// AuditEpoch identifies an interval of authoritative segment mutation.
type AuditEpoch struct {
	Version uint32
	Nonce   [32]byte
}

// ReplacementPersistenceError reports an uncertain replacement outcome.
type ReplacementPersistenceError struct {
	Path           string
	MayHaveChanged bool
	Err            error
}

func (e *ReplacementPersistenceError) Error() string {
	return fmt.Sprintf("segment: replacement %s (may have changed: %t): %v", e.Path, e.MayHaveChanged, e.Err)
}
func (e *ReplacementPersistenceError) Unwrap() error { return e.Err }

// ReadAuditEpoch reads the sidecar, reporting absence only for a missing file.
func ReadAuditEpoch(fs vfs.FS, path string) (AuditEpoch, bool, error) {
	f, err := openSegmentReadOnly(fs, path+".audit-epoch")
	if err != nil {
		if isSegmentNotExist(err) {
			return AuditEpoch{}, false, nil
		}
		return AuditEpoch{}, false, err
	}
	defer f.Close()
	buf, err := io.ReadAll(io.LimitReader(f, 37))
	if err != nil {
		return AuditEpoch{}, false, err
	}
	if len(buf) != 36 {
		return AuditEpoch{}, false, fmt.Errorf("segment: invalid audit epoch length %d", len(buf))
	}
	epoch := AuditEpoch{Version: binary.BigEndian.Uint32(buf[:4])}
	if epoch.Version != 1 {
		return AuditEpoch{}, false, fmt.Errorf("segment: invalid audit epoch version %d", epoch.Version)
	}
	copy(epoch.Nonce[:], buf[4:])
	return epoch, true, nil
}

// BeginAuditMutation durably replaces the epoch before authoritative writes.
func BeginAuditMutation(fs vfs.FS, path string) (AuditEpoch, error) {
	epoch := AuditEpoch{Version: 1}
	if _, err := rand.Read(epoch.Nonce[:]); err != nil {
		return AuditEpoch{}, err
	}
	var buf [36]byte
	binary.BigEndian.PutUint32(buf[:4], epoch.Version)
	copy(buf[4:], epoch.Nonce[:])
	tmp := fmt.Sprintf("%s.audit-epoch.%x.tmp", path, epoch.Nonce)
	f, err := createSegmentFileExclusive(fs, tmp)
	if err != nil {
		return AuditEpoch{}, err
	}
	defer removeSegmentFile(fs, tmp)
	n, err := f.Write(buf[:])
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		_ = f.Close()
		return AuditEpoch{}, err
	}
	if err = syncSegmentFile(fs, f); err != nil {
		_ = f.Close()
		return AuditEpoch{}, err
	}
	if err = f.Close(); err != nil {
		return AuditEpoch{}, err
	}
	sidecar := path + ".audit-epoch"
	if err = renameSegmentFile(fs, tmp, sidecar); err != nil {
		return AuditEpoch{}, &ReplacementPersistenceError{Path: sidecar, MayHaveChanged: true, Err: err}
	}
	if err = syncParentDirFS(fs, sidecar, nil); err != nil {
		return AuditEpoch{}, &ReplacementPersistenceError{Path: sidecar, MayHaveChanged: true, Err: err}
	}
	return epoch, nil
}
