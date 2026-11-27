package verify

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Test_VerifyFile_RoundTrip writes a verifiable object to disk and confirms
// VerifyFile accepts it while PayloadReader returns the exact payload.
func Test_VerifyFile_RoundTrip(t *testing.T) {
	payload := make([]byte, 256*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "object.bin")
	writeObjectFile(t, path, payload)

	e, size, err := VerifyFile(path)
	if err != nil {
		t.Fatalf("VerifyFile: %s", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("payload size: got %d, want %d", size, len(payload))
	}
	if e.Algorithm != AlgorithmCRC32Castagnoli {
		t.Fatalf("algorithm: got %q", e.Algorithm)
	}

	rc, gotEnv, err := PayloadReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("PayloadReader did not return the exact payload region")
	}
	if gotEnv.Checksum != e.Checksum {
		t.Fatal("PayloadReader envelope differs from VerifyFile envelope")
	}
}

func Test_VerifyFile_Corruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "object.bin")
	writeObjectFile(t, path, bytes.Repeat([]byte("q"), 4096))

	// Corrupt a byte in the payload region.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[100] ^= 0x01
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	e, _, err := VerifyFile(path)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	if e == nil {
		t.Fatal("the original envelope must be returned for the record")
	}
}

func Test_VerifyFile_DeterministicAcrossRuns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "object.bin")
	writeObjectFile(t, path, []byte("stable contents"))

	var first *Envelope
	for i := 0; i < 3; i++ {
		e, _, err := VerifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = e
		} else if *e != *first {
			t.Fatal("repeated file verification returned different metadata")
		}
	}
}

func Test_Quarantine_StoreFile(t *testing.T) {
	dir := t.TempDir()
	q, err := NewQuarantine(filepath.Join(dir, "q"))
	if err != nil {
		t.Fatal(err)
	}

	src := filepath.Join(dir, "snap.bin")
	object := mustBuildObject(t, []byte("quarantine via file"))
	object[4] ^= 0x02 // real corruption so verification fails
	if err := os.WriteFile(src, object, 0o644); err != nil {
		t.Fatal(err)
	}
	e, _, verifyErr := VerifyBytes(object)
	if !errors.Is(verifyErr, ErrChecksumMismatch) {
		t.Fatalf("test setup: %v", verifyErr)
	}
	entry, err := q.StoreFile(src, verifyErr, e, "gcs://bucket/snap")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("StoreFile must move the source out of its staging location")
	}
	saved, err := os.ReadFile(filepath.Join(entry, "snapshot.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, object) {
		t.Fatal("quarantined file content changed")
	}
}

func writeObjectFile(t *testing.T, path string, payload []byte) {
	t.Helper()
	var buf bytes.Buffer
	vw, err := NewWriter(&buf, AlgorithmCRC32Castagnoli, "9")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := vw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
