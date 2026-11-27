package verify

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func Test_Writer_RoundTrip(t *testing.T) {
	payload := []byte("the quick brown fox jumps over the lazy dog")

	var buf bytes.Buffer
	w, err := NewWriter(&buf, AlgorithmCRC32Castagnoli, "42")
	if err != nil {
		t.Fatalf("NewWriter: %s", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %s", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %s", err)
	}
	if w.Size() != int64(len(payload)) {
		t.Fatalf("Size: got %d, want %d", w.Size(), len(payload))
	}

	e, gotPayload, err := VerifyBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("VerifyBytes: %s", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", gotPayload, payload)
	}
	if e.Algorithm != AlgorithmCRC32Castagnoli {
		t.Fatalf("algorithm: got %q", e.Algorithm)
	}
	if e.PayloadSize != int64(len(payload)) {
		t.Fatalf("payload size: got %d, want %d", e.PayloadSize, len(payload))
	}
	if e.BackupID != "42" {
		t.Fatalf("backup ID: got %q", e.BackupID)
	}
	if len(e.Checksum) != 8 {
		t.Fatalf("checksum width: got %d", len(e.Checksum))
	}
}

func Test_VerifyBytes_Deterministic(t *testing.T) {
	object := mustBuildObject(t, make([]byte, 4096))

	// Repeated verification of the identical object must reach the identical
	// verdict and return identical envelopes.
	var first *Envelope
	for i := 0; i < 5; i++ {
		e, _, err := VerifyBytes(object)
		if err != nil {
			t.Fatalf("iteration %d: %s", i, err)
		}
		if first == nil {
			first = e
		} else if *e != *first {
			t.Fatalf("iteration %d returned a different envelope: %+v vs %+v", i, e, first)
		}
	}
}

func Test_VerifyBytes_CorruptPayload(t *testing.T) {
	payload := make([]byte, 1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	object := mustBuildObject(t, payload)

	// Flip a payload byte without touching the trailer.
	object[10] ^= 0xff
	e, _, err := VerifyBytes(object)
	if err == nil {
		t.Fatal("expected checksum error, got nil")
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	if !IsChecksumError(err) {
		t.Fatal("IsChecksumError should be true")
	}
	if e == nil || e.Algorithm != AlgorithmCRC32Castagnoli {
		t.Fatalf("original envelope should still be returned for inspection: %+v", e)
	}
}

func Test_VerifyBytes_TruncatedPayload(t *testing.T) {
	object := mustBuildObject(t, bytes.Repeat([]byte("x"), 500))

	// Drop payload bytes but keep the trailer intact: size check must fire.
	truncated := append([]byte(nil), object[:100]...)
	truncated = append(truncated, object[500:]...)
	_, _, err := VerifyBytes(truncated)
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got %v", err)
	}
	if !IsChecksumError(err) {
		t.Fatal("IsChecksumError should be true for size mismatch")
	}
}

func Test_VerifyBytes_LegacyAndGarbage(t *testing.T) {
	tests := []struct {
		name string
		obj  []byte
	}{
		{"plain legacy object", []byte("just a sqlite database, no trailer")},
		{"empty", nil},
		{"bad envelope length hex", append([]byte("payload"), []byte("zzzzzzzz\n")...)},
		{"envelope length exceeds object", append([]byte("payload"), []byte("0000ffff\n")...)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := VerifyBytes(tt.obj)
			if !errors.Is(err, ErrNotVerifiable) {
				t.Fatalf("expected ErrNotVerifiable, got %v", err)
			}
			if IsChecksumError(err) {
				t.Fatal("legacy/garbage objects are not checksum failures")
			}
		})
	}
}

func Test_VerifyBytes_TamperedEnvelope(t *testing.T) {
	object := mustBuildObject(t, []byte("important snapshot contents"))
	e, payloadEnd, err := parseTrailer(object)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite the checksum inside the envelope and replace the whole trailer.
	e.Checksum = "deadbeef"
	trailer := mustMarshalTrailer(t, e)

	tampered := append([]byte(nil), object[:payloadEnd]...)
	tampered = append(tampered, trailer...)

	_, _, err = VerifyBytes(tampered)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch after envelope tampering, got %v", err)
	}
}

func Test_Writer_UnknownAlgorithm(t *testing.T) {
	if _, err := NewWriter(&bytes.Buffer{}, "md5", ""); err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
}

func Test_VerifyingReader_Check(t *testing.T) {
	payload := make([]byte, 2048)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	object := mustBuildObject(t, payload)
	e, payloadEnd, err := parseTrailer(object)
	if err != nil {
		t.Fatal(err)
	}
	vr, err := NewVerifyingReader(bytes.NewReader(object[:payloadEnd]), e)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, payloadEnd)
	if _, err := io.ReadFull(vr, got); err != nil {
		t.Fatal(err)
	}
	if err := vr.Check(); err != nil {
		t.Fatalf("Check: %s", err)
	}

	// Short read must fail the size check.
	vr2, _ := NewVerifyingReader(bytes.NewReader(object[:payloadEnd-1]), e)
	out2 := make([]byte, payloadEnd-1)
	if _, err := io.ReadFull(vr2, out2); err != nil {
		t.Fatal(err)
	}
	if err := vr2.Check(); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got %v", err)
	}
}

func Test_Quarantine_PreservesEvidence(t *testing.T) {
	dir := t.TempDir()
	q, err := NewQuarantine(filepath.Join(dir, "quarantine"))
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("corrupt me")
	object := mustBuildObject(t, payload)
	object[3] ^= 0x01
	e, _, verifyErr := VerifyBytes(object)
	if !errors.Is(verifyErr, ErrChecksumMismatch) {
		t.Fatalf("test setup: %v", verifyErr)
	}

	entry, err := q.Store(object, verifyErr, e, "s3://bucket/backup")
	if err != nil {
		t.Fatalf("Store: %s", err)
	}

	// The raw object and a record carrying the original envelope must exist.
	saved, err := os.ReadFile(filepath.Join(entry, "snapshot.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, object) {
		t.Fatal("quarantined object bytes were not preserved verbatim")
	}
	recBytes, err := os.ReadFile(filepath.Join(entry, quarantineRecordFile))
	if err != nil {
		t.Fatal(err)
	}
	rec := &QuarantineRecord{}
	if err := json.Unmarshal(recBytes, rec); err != nil {
		t.Fatal(err)
	}
	if rec.Envelope == nil || rec.Envelope.Checksum != e.Checksum {
		t.Fatalf("record must preserve original envelope: %+v", rec.Envelope)
	}
	if rec.Reason == "" || rec.Source != "s3://bucket/backup" {
		t.Fatalf("record missing reason/source: %+v", rec)
	}

	// A second quarantine must create a separate entry, never overwrite.
	entry2, err := q.Store(object, verifyErr, e, "s3://bucket/backup")
	if err != nil {
		t.Fatal(err)
	}
	if entry2 == entry {
		t.Fatal("second quarantine overwrote the first entry")
	}
	entries, err := q.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 preserved entries, got %d", len(entries))
	}

	// Quarantine must refuse to hide successful verifications.
	if _, err := q.Store(object, nil, e, ""); err == nil {
		t.Fatal("quarantining without a failure reason must be rejected")
	}
}

func Test_Quarantine_RejectsEmptyDir(t *testing.T) {
	if _, err := NewQuarantine(""); err == nil {
		t.Fatal("expected error for empty quarantine dir")
	}
}

func mustBuildObject(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, AlgorithmCRC32Castagnoli, "7")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func mustMarshalTrailer(t *testing.T, e *Envelope) []byte {
	t.Helper()
	b, err := MarshalTrailer(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
