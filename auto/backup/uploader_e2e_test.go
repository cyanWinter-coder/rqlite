package backup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/auto/file"
	"github.com/rqlite/rqlite/v10/auto/verify"
)

// Test_UploadRestore_EndToEnd_VerifiableObject proves the upload and restore
// sides interoperate solely through metadata stored inside the object: the
// uploader appends a self-describing trailer, and a fresh verifier with no
// local cache accepts it and recovers the exact payload.
func Test_UploadRestore_EndToEnd_VerifiableObject(t *testing.T) {
	dir := t.TempDir()
	name := "snapshot.bin"
	sc, err := file.NewClient(dir, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	dp := &mockDataProvider{data: "end-to-end snapshot contents"}
	u := NewUploader(sc, dp, time.Hour)
	if err := u.upload(context.Background()); err != nil {
		t.Fatalf("upload: %s", err)
	}

	// Locate the object exactly as a restore node would, with no cached
	// checksum available.
	latest := sc.LatestFilePath(context.Background())
	if latest == "" {
		t.Fatal("no uploaded object found")
	}
	object, err := os.ReadFile(latest)
	if err != nil {
		t.Fatal(err)
	}
	e, payload, err := verify.VerifyBytes(object)
	if err != nil {
		t.Fatalf("restore-side verification failed without any local cache: %s", err)
	}
	if string(payload) != "end-to-end snapshot contents" {
		t.Fatalf("payload mismatch: %q", payload)
	}
	if e.Algorithm != verify.AlgorithmCRC32Castagnoli {
		t.Fatalf("algorithm must travel with the object, got %q", e.Algorithm)
	}

	// The same bytes, verified through the file-based path used for large
	// objects, must reach the same verdict.
	staging := filepath.Join(t.TempDir(), "download.bin")
	if err := os.WriteFile(staging, object, 0o644); err != nil {
		t.Fatal(err)
	}
	e2, _, err := verify.VerifyFile(staging)
	if err != nil {
		t.Fatalf("VerifyFile: %s", err)
	}
	if e2.Checksum != e.Checksum {
		t.Fatal("in-memory and file-based verification disagree")
	}
}

// Test_UploadRestore_EndToEnd_CorruptionRejected simulates bit rot in object
// storage and confirms a restore node rejects the object rather than booting
// from it.
func Test_UploadRestore_EndToEnd_CorruptionRejected(t *testing.T) {
	dir := t.TempDir()
	sc, err := file.NewClient(dir, "snap.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	dp := &mockDataProvider{data: "durable snapshot"}
	u := NewUploader(sc, dp, time.Hour)
	if err := u.upload(context.Background()); err != nil {
		t.Fatal(err)
	}
	latest := sc.LatestFilePath(context.Background())
	object, err := os.ReadFile(latest)
	if err != nil {
		t.Fatal(err)
	}
	object[0] ^= 0xff // bit rot in storage
	if _, _, err := verify.VerifyBytes(object); err == nil {
		t.Fatal("corrupted object must be rejected at restore time")
	} else if !bytes.Contains([]byte(err.Error()), []byte("mismatch")) {
		t.Fatalf("expected a mismatch error, got %v", err)
	}
}
