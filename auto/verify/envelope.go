// Package verify implements the self-describing verification envelope used by
// the auto-backup upload and auto-restore download paths.
//
// A verifiable snapshot is the payload bytes (the exact bytes produced by the
// backup provider, optionally gzip-compressed) immediately followed by a
// fixed-size, human-readable trailer. The trailer records the checksum
// algorithm, the expected checksum and the payload size, so a receiver can
// verify a downloaded object using only metadata stored inside the object
// itself. No local cache of checksums is involved, and verifying the same
// object twice always yields the same result.
//
// On-disk layout:
//
//	<raw payload bytes>            payloadSize bytes
//	<JSON-encoded Envelope>        envelopeSize bytes
//	<8 hex chars: envelopeSize>
//	'\n'
//
// The final 9 bytes are fixed. The 8 hex characters immediately preceding the
// newline encode the byte length of the JSON envelope, which the reader
// unmarshals to learn the algorithm and expected checksum.
package verify

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

const (
	// AlgorithmCRC32Castagnoli is the CRC32 checksum computed with the
	// Castagnoli polynomial. It matches the checksum used elsewhere in the
	// snapshot subsystem.
	AlgorithmCRC32Castagnoli = "crc32-castagnoli"

	// envelopeSizeLen is the width, in bytes, of the hex-encoded envelope
	// length prefix at the very end of a verifiable object.
	envelopeSizeLen = 8
	// trailerLen is envelopeSizeLen plus the terminating newline.
	trailerLen = envelopeSizeLen + 1

	// maxEnvelopeSize bounds how much envelope data a reader will accept.
	maxEnvelopeSize = 64 * 1024
)

// ErrNotVerifiable is returned when a byte stream does not carry a verification
// trailer, i.e. it was written by an older uploader that did not record
// checksums.
var ErrNotVerifiable = errors.New("snapshot has no verification metadata")

// ErrChecksumMismatch is returned when the checksum computed over the payload
// does not match the checksum recorded in the envelope.
var ErrChecksumMismatch = errors.New("snapshot checksum mismatch")

// ErrSizeMismatch is returned when the payload size recorded in the envelope
// does not match the number of payload bytes actually read.
var ErrSizeMismatch = errors.New("snapshot size mismatch")

// Envelope is the JSON metadata persisted alongside every verifiable snapshot.
// It travels inside the uploaded object, so the algorithm and expected checksum
// are always available to a receiver without any out-of-band state.
type Envelope struct {
	// Algorithm names the checksum algorithm, e.g. "crc32-castagnoli".
	Algorithm string `json:"algorithm"`

	// Checksum is the lowercase hex-encoded checksum of the payload.
	Checksum string `json:"checksum"`

	// PayloadSize is the number of payload bytes covered by Checksum.
	PayloadSize int64 `json:"payload_size"`

	// BackupID is the Raft log index the payload was captured at. It is
	// informational and not required for verification.
	BackupID string `json:"backup_id,omitempty"`
}

// newHash returns the hash.Hash32 implementing the named algorithm.
func newHash(algorithm string) (hash.Hash32, error) {
	switch algorithm {
	case AlgorithmCRC32Castagnoli:
		return crc32.New(crc32.MakeTable(crc32.Castagnoli)), nil
	default:
		return nil, fmt.Errorf("unsupported checksum algorithm %q", algorithm)
	}
}

// encodeChecksum renders a checksum as lowercase hex. CRC32 values are encoded
// as exactly 8 hex characters, matching the sidecar format used by the
// snapshot package.
func encodeChecksum(algorithm string, h hash.Hash32) (string, error) {
	switch algorithm {
	case AlgorithmCRC32Castagnoli:
		return fmt.Sprintf("%08x", h.Sum32()), nil
	default:
		return "", fmt.Errorf("unsupported checksum algorithm %q", algorithm)
	}
}

// MarshalTrailer builds the verification trailer for an envelope: the JSON
// envelope followed by its 8-character hex length and a newline.
func MarshalTrailer(e *Envelope) ([]byte, error) {
	envBytes, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshaling verification envelope: %w", err)
	}
	if len(envBytes) > maxEnvelopeSize {
		return nil, fmt.Errorf("verification envelope too large: %d bytes", len(envBytes))
	}
	trailer := make([]byte, 0, len(envBytes)+trailerLen)
	trailer = append(trailer, envBytes...)
	trailer = append(trailer, []byte(fmt.Sprintf("%08x\n", len(envBytes)))...)
	return trailer, nil
}

// parseTrailer interprets the tail of a verifiable object and returns the
// envelope and the byte offset at which the payload starts.
func parseTrailer(object []byte) (*Envelope, int64, error) {
	if len(object) < trailerLen {
		return nil, 0, ErrNotVerifiable
	}
	tail := object[len(object)-trailerLen:]
	if tail[envelopeSizeLen] != '\n' {
		return nil, 0, ErrNotVerifiable
	}
	size, err := parseEnvelopeSize(tail[:envelopeSizeLen])
	if err != nil || size > len(object)-trailerLen {
		return nil, 0, fmt.Errorf("%w: malformed envelope length", ErrNotVerifiable)
	}
	envStart := int64(len(object)) - trailerLen - int64(size)
	e := &Envelope{}
	if err := unmarshalEnvelope(object[envStart:envStart+int64(size)], e); err != nil {
		return nil, 0, err
	}
	return e, envStart, nil
}

// VerifyBytes verifies a complete verifiable object held in memory. It returns
// the envelope and the payload bytes (a sub-slice of object, not a copy).
//
// The check is deterministic: identical input always produces an identical
// verdict. ErrNotVerifiable indicates the object predates checksum support;
// ErrChecksumMismatch and ErrSizeMismatch indicate a corrupt object and carry
// the recorded versus observed values in their error text.
func VerifyBytes(object []byte) (*Envelope, []byte, error) {
	e, payloadEnd, err := parseTrailer(object)
	if err != nil {
		return nil, nil, err
	}
	payload := object[:payloadEnd]
	if int64(len(payload)) != e.PayloadSize {
		return e, payload, fmt.Errorf("%w: recorded %d bytes, read %d bytes",
			ErrSizeMismatch, e.PayloadSize, len(payload))
	}
	h, err := newHash(e.Algorithm)
	if err != nil {
		return e, payload, err
	}
	if _, err := h.Write(payload); err != nil {
		return e, payload, fmt.Errorf("computing checksum: %w", err)
	}
	got, err := encodeChecksum(e.Algorithm, h)
	if err != nil {
		return e, payload, err
	}
	if got != e.Checksum {
		return e, payload, fmt.Errorf("%w: recorded %s, computed %s (algorithm %s)",
			ErrChecksumMismatch, e.Checksum, got, e.Algorithm)
	}
	return e, payload, nil
}

// Writer wraps an io.Writer, computing a running checksum over every payload
// byte written through it. Close writes the verification trailer, after which
// no more payload bytes may be written.
type Writer struct {
	w         io.Writer
	algorithm string
	h         hash.Hash32
	size      int64
	backupID  string
	closed    bool
}

// NewWriter returns a Writer that records a checksum using algorithm.
func NewWriter(w io.Writer, algorithm, backupID string) (*Writer, error) {
	h, err := newHash(algorithm)
	if err != nil {
		return nil, err
	}
	return &Writer{
		w:         w,
		algorithm: algorithm,
		h:         h,
		backupID:  backupID,
	}, nil
}

// Write writes payload bytes to the underlying writer and updates the checksum.
func (vw *Writer) Write(p []byte) (int, error) {
	if vw.closed {
		return 0, errors.New("verify: write after Close")
	}
	n, err := vw.w.Write(p)
	vw.size += int64(n)
	vw.h.Write(p[:n])
	return n, err
}

// Close writes the verification trailer. It does not close the underlying
// writer, which remains the caller's responsibility.
func (vw *Writer) Close() error {
	if vw.closed {
		return nil
	}
	sum, err := encodeChecksum(vw.algorithm, vw.h)
	if err != nil {
		return err
	}
	trailer, err := MarshalTrailer(&Envelope{
		Algorithm:   vw.algorithm,
		Checksum:    sum,
		PayloadSize: vw.size,
		BackupID:    vw.backupID,
	})
	if err != nil {
		return err
	}
	if _, err := vw.w.Write(trailer); err != nil {
		return fmt.Errorf("writing verification trailer: %w", err)
	}
	vw.closed = true
	return nil
}

// Size returns the number of payload bytes written so far.
func (vw *Writer) Size() int64 {
	return vw.size
}

// VerifyingReader wraps an io.Reader for a payload whose envelope is already
// known. It hashes every byte read; Check compares the result against the
// envelope. It is used on the download path where the trailer is parsed from
// the tail of the object before the payload is consumed.
type VerifyingReader struct {
	r io.Reader
	h hash.Hash32
	e *Envelope

	read int64
}

// NewVerifyingReader returns a VerifyingReader bound to envelope e.
func NewVerifyingReader(r io.Reader, e *Envelope) (*VerifyingReader, error) {
	h, err := newHash(e.Algorithm)
	if err != nil {
		return nil, err
	}
	return &VerifyingReader{r: r, h: h, e: e}, nil
}

// Read reads payload bytes and updates the running checksum.
func (vr *VerifyingReader) Read(p []byte) (int, error) {
	n, err := vr.r.Read(p)
	vr.h.Write(p[:n])
	vr.read += int64(n)
	return n, err
}

// Check reports whether the bytes read so far match the envelope, in both size
// and checksum.
func (vr *VerifyingReader) Check() error {
	if vr.read != vr.e.PayloadSize {
		return fmt.Errorf("%w: recorded %d bytes, read %d bytes",
			ErrSizeMismatch, vr.e.PayloadSize, vr.read)
	}
	got, err := encodeChecksum(vr.e.Algorithm, vr.h)
	if err != nil {
		return err
	}
	if got != vr.e.Checksum {
		return fmt.Errorf("%w: recorded %s, computed %s (algorithm %s)",
			ErrChecksumMismatch, vr.e.Checksum, got, vr.e.Algorithm)
	}
	return nil
}

// IsChecksumError reports whether err indicates a failed verification (a
// checksum or size mismatch, as opposed to a missing/legacy envelope or an
// I/O error).
func IsChecksumError(err error) bool {
	return errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrSizeMismatch)
}

// EnvelopeOf is a convenience helper that parses and verifies an in-memory
// object, returning just its envelope.
func EnvelopeOf(object []byte) (*Envelope, error) {
	e, _, err := VerifyBytes(object)
	return e, err
}
