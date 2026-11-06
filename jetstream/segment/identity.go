package segment

import (
	"fmt"

	"github.com/cockroachdb/pebble/vfs"
)

// SealedAuditIdentity is the fixed-header identity of a sealed segment.
type SealedAuditIdentity struct {
	Epoch       AuditEpoch
	HasEpoch    bool
	Size        int64
	HeaderBytes [ReservedHeaderBytes]byte
}

// ReadSealedAuditIdentity reads only the fixed header, file size and epoch.
func ReadSealedAuditIdentity(fs vfs.FS, path string) (SealedAuditIdentity, error) {
	var identity SealedAuditIdentity
	f, err := openSegmentReadOnly(fs, path)
	if err != nil {
		return identity, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return identity, err
	}
	identity.Size = info.Size()
	if identity.Size < ReservedHeaderBytes {
		return identity, fmt.Errorf("%w: %s is %d bytes", ErrCorruptSegment, path, identity.Size)
	}
	if _, err = f.ReadAt(identity.HeaderBytes[:], 0); err != nil {
		return identity, err
	}
	header, err := decodeHeader(identity.HeaderBytes[:])
	if err != nil {
		return identity, err
	}
	if err = validateHeaderOffsets(header, uint64(identity.Size)); err != nil {
		return identity, err
	}
	if header.BlockCount > maxBlockCountLimit {
		return identity, fmt.Errorf("%w: block count exceeds limit", ErrInvalidFooter)
	}
	identity.Epoch, identity.HasEpoch, err = ReadAuditEpoch(fs, path)
	return identity, err
}
