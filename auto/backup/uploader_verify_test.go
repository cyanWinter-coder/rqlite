package backup

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/auto/verify"
)

// Test_Uploader_WritesVerifiableObject checks that an uploaded object carries
// a self-describing verification trailer whose checksum matches the payload.
func Test_Uploader_WritesVerifiableObject(t *testing.T) {
	ResetStats()
	var object []byte
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			var err error
			object, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "verifiable snapshot payload"}
	u := NewUploader(sc, dp, time.Hour)
	u.retryInterval = time.Millisecond

	if err := u.upload(context.Background()); err != nil {
		t.Fatalf("upload: %s", err)
	}
	e, payload, err := verify.VerifyBytes(object)
	if err != nil {
		t.Fatalf("uploaded object is not verifiable: %s", err)
	}
	if string(payload) != "verifiable snapshot payload" {
		t.Fatalf("payload mismatch: %q", payload)
	}
	if e.Algorithm != verify.AlgorithmCRC32Castagnoli {
		t.Fatalf("algorithm not persisted in object: %q", e.Algorithm)
	}
	if e.BackupID != "1" {
		t.Fatalf("backup ID not recorded in envelope: %q", e.BackupID)
	}
}

// Test_Uploader_UploadFailureRetriedNoHalfSnapshot verifies that a failure
// mid-upload is retried with a fresh object and never surfaces a partial,
// unverifiable object as the snapshot.
func Test_Uploader_UploadFailureRetriedNoHalfSnapshot(t *testing.T) {
	ResetStats()
	var objects [][]byte
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			b, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			objects = append(objects, b)
			if len(objects) == 1 {
				return errors.New("connection reset by peer")
			}
			return nil
		},
	}
	dp := &mockDataProvider{data: "retry me"}
	u := NewUploader(sc, dp, time.Hour)
	u.nRetries = 2
	u.retryInterval = time.Millisecond

	if err := u.upload(context.Background()); err != nil {
		t.Fatalf("upload: %s", err)
	}
	if len(objects) != 2 {
		t.Fatalf("expected 2 upload attempts, got %d", len(objects))
	}
	for i, obj := range objects {
		if _, _, err := verify.VerifyBytes(obj); err != nil {
			t.Fatalf("attempt %d object must be fully verifiable, got: %s", i+1, err)
		}
	}
	if got := stats.Get(numUploadsRetried).(*expvar.Int).Value(); got != 1 {
		t.Fatalf("expected 1 recorded retry, got %d", got)
	}
}

// Test_Uploader_AllAttemptsFail tests that exhausting retries reports failure
// without advancing lastIndex, so the same snapshot is retried next cycle.
func Test_Uploader_AllAttemptsFail(t *testing.T) {
	ResetStats()
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			io.Copy(io.Discard, reader)
			return fmt.Errorf("storage unavailable")
		},
	}
	dp := &mockDataProvider{data: "doomed"}
	u := NewUploader(sc, dp, time.Hour)
	u.nRetries = 1
	u.retryInterval = time.Millisecond

	if err := u.upload(context.Background()); err == nil {
		t.Fatal("expected upload to fail after exhausting retries")
	}
	if u.lastIndex != 0 {
		t.Fatalf("lastIndex must not advance on failure, got %d", u.lastIndex)
	}
	if got := stats.Get(numUploadsFail).(*expvar.Int).Value(); got != 1 {
		t.Fatalf("expected 1 failed-upload stat, got %d", got)
	}
}

// Test_Uploader_FailedAttemptLeavesNoTempFiles checks that a failed prepare or
// upload removes its staging file. It uses the real filesystem via the
// package-level tempFD helper.
func Test_Uploader_FailedAttemptLeavesNoTempFiles(t *testing.T) {
	ResetStats()
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			return errors.New("boom")
		},
	}
	dp := &mockDataProvider{}
	dp.provideFn = func(w io.Writer) error {
		_, err := w.Write([]byte("staged then lost"))
		return err
	}
	u := NewUploader(sc, dp, time.Hour)
	u.nRetries = 0
	u.retryInterval = time.Millisecond

	before := listUploadTempFiles(t)
	if err := u.upload(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	after := listUploadTempFiles(t)
	if len(after) != len(before) {
		t.Fatalf("failed attempt left staging files behind: before=%v after=%v", before, after)
	}
}

// listUploadTempFiles lists staging files created by tempFD in the OS temp
// directory. It only inspects names, never contents.
func listUploadTempFiles(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "rqlite-upload*"))
	if err != nil {
		t.Fatalf("listing temp files: %s", err)
	}
	return matches
}
