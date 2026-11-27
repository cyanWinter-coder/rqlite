package verify

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// VerifyFile verifies an object stored at path without loading it into
// memory: the fixed-size tail is read to parse the envelope, then the payload
// region is streamed through the checksum. It returns the envelope and the
// payload's byte length.
//
// As with VerifyBytes the verdict is deterministic and depends only on the
// file contents and the metadata carried inside it.
func VerifyFile(path string) (*Envelope, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	total := fi.Size()
	if total < int64(trailerLen) {
		return nil, 0, ErrNotVerifiable
	}

	tail := make([]byte, trailerLen)
	if _, err := f.ReadAt(tail, total-int64(trailerLen)); err != nil {
		return nil, 0, fmt.Errorf("reading verification trailer: %w", err)
	}
	if tail[envelopeSizeLen] != '\n' {
		return nil, 0, ErrNotVerifiable
	}
	envSize, err := parseEnvelopeSize(tail[:envelopeSizeLen])
	if err != nil {
		return nil, 0, err
	}
	if int64(envSize) > total-int64(trailerLen) {
		return nil, 0, fmt.Errorf("%w: malformed envelope length", ErrNotVerifiable)
	}
	payloadSize := total - int64(trailerLen) - int64(envSize)

	envBytes := make([]byte, envSize)
	if _, err := f.ReadAt(envBytes, payloadSize); err != nil {
		return nil, 0, fmt.Errorf("reading verification envelope: %w", err)
	}
	e := &Envelope{}
	if err := unmarshalEnvelope(envBytes, e); err != nil {
		return nil, 0, err
	}

	if payloadSize != e.PayloadSize {
		return e, payloadSize, fmt.Errorf("%w: recorded %d bytes, read %d bytes",
			ErrSizeMismatch, e.PayloadSize, payloadSize)
	}

	h, err := newHash(e.Algorithm)
	if err != nil {
		return e, payloadSize, err
	}
	if _, err := io.CopyN(h, f, payloadSize); err != nil {
		return e, payloadSize, fmt.Errorf("computing checksum: %w", err)
	}
	got, err := encodeChecksum(e.Algorithm, h)
	if err != nil {
		return e, payloadSize, err
	}
	if got != e.Checksum {
		return e, payloadSize, fmt.Errorf("%w: recorded %s, computed %s (algorithm %s)",
			ErrChecksumMismatch, e.Checksum, got, e.Algorithm)
	}
	return e, payloadSize, nil
}

// PayloadReader returns a reader over the payload region of a verified object
// file together with its envelope. It assumes the object's trailer has already
// been verified successfully; callers typically use it after VerifyFile.
func PayloadReader(path string) (io.ReadCloser, *Envelope, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	total := fi.Size()
	if total < int64(trailerLen) {
		f.Close()
		return nil, nil, ErrNotVerifiable
	}
	tail := make([]byte, trailerLen)
	if _, err := f.ReadAt(tail, total-int64(trailerLen)); err != nil {
		f.Close()
		return nil, nil, err
	}
	envSize, err := parseEnvelopeSize(tail[:envelopeSizeLen])
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	payloadSize := total - int64(trailerLen) - int64(envSize)
	envBytes := make([]byte, envSize)
	if _, err := f.ReadAt(envBytes, payloadSize); err != nil {
		f.Close()
		return nil, nil, err
	}
	e := &Envelope{}
	if err := unmarshalEnvelope(envBytes, e); err != nil {
		f.Close()
		return nil, nil, err
	}
	// Rewind to the start so the limited reader walks exactly the payload.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &limitedReadCloser{
		Reader: io.LimitReader(f, payloadSize),
		Closer: f,
	}, e, nil
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}

// shared parsing helpers reused by the in-memory and file paths.

func parseEnvelopeSize(hexBytes []byte) (int, error) {
	if len(hexBytes) != envelopeSizeLen {
		return 0, ErrNotVerifiable
	}
	raw, err := hex.DecodeString(string(hexBytes))
	if err != nil || len(raw) != 4 {
		return 0, ErrNotVerifiable
	}
	size := int(raw[0])<<24 | int(raw[1])<<16 | int(raw[2])<<8 | int(raw[3])
	if size <= 0 || size > maxEnvelopeSize {
		return 0, fmt.Errorf("%w: malformed envelope length", ErrNotVerifiable)
	}
	return size, nil
}

func unmarshalEnvelope(envBytes []byte, e *Envelope) error {
	if err := json.Unmarshal(envBytes, e); err != nil {
		return fmt.Errorf("decoding verification envelope: %w", err)
	}
	if e.Algorithm == "" || e.Checksum == "" {
		return fmt.Errorf("%w: envelope missing algorithm or checksum", ErrNotVerifiable)
	}
	return nil
}
