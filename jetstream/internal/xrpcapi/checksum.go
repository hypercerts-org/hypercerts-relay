package xrpcapi

import (
	"crypto/sha256"
	"fmt"
	"io"
)

// checksumHex renders a segment's xxh3 checksum as a fixed 16-char hex
// string for the listSegments metadata field. It does not cover row bytes
// and is not a representation validator or a unique generation identifier.
func checksumHex(checksum uint64) string {
	return fmt.Sprintf("%016x", checksum)
}

func representationETag(reader io.ReaderAt, size int64) (string, error) {
	hash := sha256.New()
	count, err := io.Copy(hash, io.NewSectionReader(reader, 0, size))
	if err != nil {
		return "", err
	}
	if count != size {
		return "", io.ErrUnexpectedEOF
	}
	return fmt.Sprintf("\"sha256-%x-%d\"", hash.Sum(nil), size), nil
}
