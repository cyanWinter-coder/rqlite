package restore

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/auto/verify"
)

// mockStorageClient serves a fixed object, optionally failing the first
// failFirst attempts.
type mockStorageClient struct {
	mu        sync.Mutex
	data      []byte
	error     error
	failFirst int
	attempts  int
	stringer  string
}

func (m *mockStorageClient) Download(ctx context.Context, writer io.WriterAt) error {
	m.mu.Lock()
	m.attempts++
	attempt := m.attempts
	m.mu.Unlock()

	if m.error != nil {
		return m.error
	}
	if attempt <= m.failFirst {
		return errors.New("simulated transient download failure")
	}
	n, err := writer.WriteAt(m.data, 0)
	if n != len(m.data) || err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	return nil
}

func (m *mockStorageClient) String() string {
	if m.stringer != "" {
		return m.stringer
	}
	return "mockStorageClient"
}

// verifiableObject builds an object carrying a valid verification trailer.
func verifiableObject(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	vw, err := verify.NewWriter(&buf, verify.AlgorithmCRC32Castagnoli, "1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := vw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestDownloader_Do_Verified(t *testing.T) {
	payload := []byte("test data")
	mockClient := &mockStorageClient{data: verifiableObject(t, payload)}
	downloader := NewDownloader(mockClient)

	var out bytes.Buffer
	if err := downloader.Do(context.Background(), &out, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("expected payload %q, got %q", payload, out.Bytes())
	}
}

func TestDownloader_Do_VerifiedCompressed(t *testing.T) {
	payload := []byte("compressed test data")

	// First gzip the payload, then wrap the gzip stream in a verification
	// trailer so the checksum covers exactly the bytes stored in the object.
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, err := gw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	mockClient := &mockStorageClient{data: verifiableObject(t, gzBuf.Bytes())}

	downloader := NewDownloader(mockClient)
	var out bytes.Buffer
	if err := downloader.Do(context.Background(), &out, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("expected decompressed payload %q, got %q", payload, out.Bytes())
	}
}

func TestDownloader_Do_ChecksumMismatchQuarantinesAndRejects(t *testing.T) {
	dir := t.TempDir()
	object := verifiableObject(t, []byte("important data"))
	// Corrupt a payload byte; the trailer and its original envelope survive.
	object[5] ^= 0xff

	mockClient := &mockStorageClient{data: object, stringer: "s3://bucket/snap"}
	downloader := NewDownloader(mockClient)
	if err := downloader.SetQuarantineDir(filepath.Join(dir, "quarantine")); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	err := downloader.Do(context.Background(), &out, 5*time.Second)
	if err == nil {
		t.Fatal("corrupt snapshot must be rejected")
	}
	if !errors.Is(err, verify.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	if out.Len() != 0 {
		t.Fatal("corrupt payload must never be delivered to the caller")
	}

	// Evidence must be preserved, never auto-deleted.
	entries, err := downloader.quarantine.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 quarantine entry, got %d", len(entries))
	}
	saved, err := os.ReadFile(filepath.Join(entries[0], "snapshot.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, object) {
		t.Fatal("quarantined object must be preserved verbatim with original metadata")
	}

	// A second, identical failure must keep both copies (stable verdict,
	// no automatic deletion).
	err2 := downloader.Do(context.Background(), &out, 5*time.Second)
	if !errors.Is(err2, verify.ErrChecksumMismatch) {
		t.Fatalf("second verification must reach the same conclusion, got %v", err2)
	}
	entries, _ = downloader.quarantine.Entries()
	if len(entries) != 2 {
		t.Fatalf("failed snapshots must accumulate, got %d entries", len(entries))
	}
}

func TestDownloader_Do_TruncatedObjectIsSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	object := verifiableObject(t, bytes.Repeat([]byte("z"), 200))
	truncated := append(append([]byte(nil), object[:50]...), object[200:]...)

	mockClient := &mockStorageClient{data: truncated}
	downloader := NewDownloader(mockClient)
	if err := downloader.SetQuarantineDir(filepath.Join(dir, "q")); err != nil {
		t.Fatal(err)
	}
	err := downloader.Do(context.Background(), &bytes.Buffer{}, 5*time.Second)
	if !errors.Is(err, verify.ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got %v", err)
	}
}

func TestDownloader_Do_RetriesTransientFailure(t *testing.T) {
	mockClient := &mockStorageClient{
		data:      verifiableObject(t, []byte("eventually consistent")),
		failFirst: 2,
	}
	downloader := NewDownloader(mockClient)
	downloader.SetRetries(3, time.Millisecond)

	var out bytes.Buffer
	if err := downloader.Do(context.Background(), &out, 5*time.Second); err != nil {
		t.Fatalf("expected retry to succeed: %v", err)
	}
	if out.String() != "eventually consistent" {
		t.Fatalf("payload mismatch: %q", out.String())
	}
	if mockClient.attempts != 3 {
		t.Fatalf("expected 3 download attempts, got %d", mockClient.attempts)
	}
}

func TestDownloader_Do_RetriesExhausted(t *testing.T) {
	mockClient := &mockStorageClient{
		data:      verifiableObject(t, []byte("x")),
		failFirst: 100,
	}
	downloader := NewDownloader(mockClient)
	downloader.SetRetries(1, time.Millisecond)

	err := downloader.Do(context.Background(), &bytes.Buffer{}, 5*time.Second)
	if err == nil {
		t.Fatal("expected error after retries are exhausted")
	}
	if mockClient.attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", mockClient.attempts)
	}
}

func TestDownloader_Do_LegacyObjectRejected(t *testing.T) {
	dir := t.TempDir()
	// An object written by an uploader without checksum support.
	mockClient := &mockStorageClient{data: []byte("legacy plain sqlite bytes")}
	downloader := NewDownloader(mockClient)
	if err := downloader.SetQuarantineDir(filepath.Join(dir, "q")); err != nil {
		t.Fatal(err)
	}
	err := downloader.Do(context.Background(), &bytes.Buffer{}, 5*time.Second)
	if !errors.Is(err, verify.ErrNotVerifiable) {
		t.Fatalf("legacy object must be rejected as not verifiable, got %v", err)
	}
}

func TestDownloader_Progress(t *testing.T) {
	mockClient := &mockStorageClient{
		data:      verifiableObject(t, bytes.Repeat([]byte("p"), 1024)),
		failFirst: 1,
	}
	downloader := NewDownloader(mockClient)
	downloader.SetRetries(2, time.Millisecond)

	var states []string
	var mu sync.Mutex
	downloader.SetProgressListener(func(p Progress) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, p.State)
	})

	if err := downloader.Do(context.Background(), &bytes.Buffer{}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	joined := ""
	for _, s := range states {
		joined += s + ","
	}
	for _, want := range []string{"downloading", "verifying", "verified"} {
		if !bytes.Contains([]byte(joined), []byte(want)) {
			t.Fatalf("progress never reported %q; got %q", want, joined)
		}
	}
	// The failed first attempt must be visible as a failed state.
	sawFailed := false
	for _, s := range states {
		if s == "failed" {
			sawFailed = true
		}
	}
	if !sawFailed {
		t.Fatalf("expected a failed progress state for attempt 1, got %q", joined)
	}
}
