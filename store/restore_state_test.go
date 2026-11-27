package store

import (
	"context"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// Test_RestoreState_InitialAndPending checks the observable restore lifecycle
// up to staging.
func Test_RestoreState_InitialAndPending(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if got := s.RestoreStatus().State; got != RestoreStateNone {
		t.Fatalf("initial state: got %q, want %q", got, RestoreStateNone)
	}

	path := mustCopyFileToTempFile("testdata/load.sqlite")
	if err := s.SetRestorePath(path); err != nil {
		t.Fatalf("SetRestorePath: %s", err)
	}
	status := s.RestoreStatus()
	if status.State != RestoreStatePending {
		t.Fatalf("after staging: got %q, want %q", status.State, RestoreStatePending)
	}
	if status.Attempts != 0 || status.RestorePath != path {
		t.Fatalf("unexpected pending status: %+v", status)
	}
}

// Test_RestoreState_FailureIsRetryableAndNotReady verifies that a failed
// auto-restore leaves the node in a distinct failed state, retains the staged
// file and its metadata, and does not mark the node ready.
func Test_RestoreState_FailureIsRetryableAndNotReady(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	path := mustCopyFileToTempFile("testdata/load.sqlite")
	if err := s.SetRestorePath(path); err != nil {
		t.Fatalf("SetRestorePath: %s", err)
	}
	if err := s.Open(); err != nil {
		t.Fatalf("Open: %s", err)
	}
	defer s.Close(true)

	// The store is open but never bootstrapped, so it is not leader. An
	// auto-restore attempt must fail cleanly rather than consuming the file.
	s.selfLeaderChange(true)

	status := s.RestoreStatus()
	if status.State != RestoreStateFailed {
		t.Fatalf("expected failed state, got %q", status.State)
	}
	if status.LastError == "" {
		t.Fatal("failed state must record the error reason")
	}
	if status.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", status.Attempts)
	}
	if s.Ready() {
		t.Fatal("a failed restore must never report the node ready")
	}
	if !fileExistsPath(path) {
		t.Fatal("staged restore file must be retained for retry/manual inspection")
	}
	if s.restoreDoneCh == nil {
		t.Fatal("ready channel must remain open while the restore is failed")
	}
	select {
	case <-s.restoreDoneCh:
		t.Fatal("ready channel must not be closed on a failed restore")
	default:
	}

	// A repeated failure reaches the same conclusion deterministically and
	// increments the attempt counter.
	s.selfLeaderChange(true)
	if got := s.RestoreStatus().State; got != RestoreStateFailed {
		t.Fatalf("second failure must stay failed, got %q", got)
	}
	if s.RestoreStatus().Attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", s.RestoreStatus().Attempts)
	}
	if !fileExistsPath(path) {
		t.Fatal("staged restore file must still be retained")
	}
}

// Test_RetryRestore_RequiresFailedState checks the guard on manual retries.
func Test_RetryRestore_RequiresFailedState(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	// Nothing staged.
	if err := s.RetryRestore(); err != ErrNoRestorePending {
		t.Fatalf("expected ErrNoRestorePending, got %v", err)
	}

	path := mustCopyFileToTempFile("testdata/load.sqlite")
	if err := s.SetRestorePath(path); err != nil {
		t.Fatal(err)
	}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)

	// Pending (not yet failed) cannot be force-retried.
	if err := s.RetryRestore(); err != ErrNoRestorePending {
		t.Fatalf("pending state: expected ErrNoRestorePending, got %v", err)
	}

	// Drive it to failed, then RetryRestore must attempt and fail again but
	// stay retryable.
	s.selfLeaderChange(true)
	if err := s.RetryRestore(); err == nil {
		t.Fatal("retry without leadership must fail")
	}
	if got := s.RestoreStatus().State; got != RestoreStateFailed {
		t.Fatalf("retry failure must keep failed state, got %q", got)
	}
}

// Test_RestoreStatus_ExposedInStats ensures progress is externally observable.
func Test_RestoreStatus_ExposedInStats(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	path := mustCopyFileToTempFile("testdata/load.sqlite")
	if err := s.SetRestorePath(path); err != nil {
		t.Fatal(err)
	}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)

	stats, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	ar, ok := stats["auto_restore"].(RestoreStatus)
	if !ok {
		t.Fatalf("auto_restore missing or wrong type: %T", stats["auto_restore"])
	}
	if ar.State != RestoreStatePending {
		t.Fatalf("stats restore state: got %q", ar.State)
	}
}

func fileExistsPath(p string) bool {
	return fsutil.PathExists(p)
}

// Test_RestoreStatus_ConcurrentReaders exercises concurrent RestoreStatus
// reads against state transitions so the race detector can observe any
// unsynchronized access to the restore state or path.
func Test_RestoreStatus_ConcurrentReaders(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	path := mustCopyFileToTempFile("testdata/load.sqlite")
	if err := s.SetRestorePath(path); err != nil {
		t.Fatal(err)
	}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			_ = s.RestoreStatus()
		}
	}()
	for i := 0; i < 50; i++ {
		s.selfLeaderChange(true) // fails (not leader), stays retryable
		_ = s.RestoreStatus()
	}
	<-done
	if got := s.RestoreStatus().State; got != RestoreStateFailed {
		t.Fatalf("expected failed state, got %q", got)
	}
}

// Test_RetryRestore_SucceedsAfterFailure drives a restore into the failed
// state on a leader, then retries successfully, proving the node catches up
// and becomes ready rather than staying failed.
func Test_RetryRestore_SucceedsAfterFailure(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	// Bootstrap a single-node leader first.
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("bootstrap: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("waiting for leader: %s", err)
	}

	// Stage a restore pointing at a missing file. SetRestorePath validates
	// the file, so set the state directly to model a failed staged restore.
	goodPath := mustCopyFileToTempFile("testdata/load.sqlite")
	s.restoreMu.Lock()
	s.restorePath = goodPath + ".missing"
	s.restoreState = RestoreStateFailed
	s.restoreAttempt = 1
	s.restoreMu.Unlock()

	if err := s.RetryRestore(); err == nil {
		t.Fatal("retry against a missing file must fail but remain retryable")
	}
	if got := s.RestoreStatus().State; got != RestoreStateFailed {
		t.Fatalf("expected to stay failed, got %q", got)
	}
	// The staged path is retained so the same restore can be retried.
	if !fileExistsPath(goodPath) {
		t.Fatal("staging file must remain available for the retry")
	}

	// Make the staged file available and retry; the restore now applies.
	if err := fsutil.Rename(goodPath, goodPath+".missing"); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryRestore(); err != nil {
		t.Fatalf("RetryRestore: %s", err)
	}

	status := s.RestoreStatus()
	if status.State != RestoreStateSucceeded {
		t.Fatalf("expected succeeded state, got %q (err=%s)", status.State, status.LastError)
	}
	if fileExistsPath(goodPath + ".missing") {
		t.Fatal("staged restore file must be removed after a successful retry")
	}

	// The restored data is queryable, showing the node caught up to the
	// restored snapshot's log state.
	qr := queryRequestFromString("SELECT count(*) FROM foo", false, true, false)
	r, _, _, err := s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("query after retried restore: %s", err)
	}
	if len(r) != 1 || len(r[0].Values) != 1 {
		t.Fatalf("unexpected query result: %v", r)
	}
}
