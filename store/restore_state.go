package store

import "time"

// RestoreState is the observable state of an auto-restore. It lets operators
// tell whether a node is waiting to restore, actively restoring, has finished,
// or has stopped in a retryable failure state.
type RestoreState string

const (
	// RestoreStateNone means no auto-restore has been requested.
	RestoreStateNone RestoreState = "none"

	// RestoreStatePending means a restore file is staged and the node is
	// waiting to become leader before installing it.
	RestoreStatePending RestoreState = "pending"

	// RestoreStateInProgress means the restore is currently being applied.
	RestoreStateInProgress RestoreState = "in_progress"

	// RestoreStateSucceeded means the restore was applied and the node has
	// rejoined the Raft log.
	RestoreStateSucceeded RestoreState = "succeeded"

	// RestoreStateSkipped means another node performed the restore, so this
	// node discarded its staged file and is catching up via Raft replication.
	RestoreStateSkipped RestoreState = "skipped"

	// RestoreStateFailed means the restore failed. The node deliberately
	// stays not-ready and retains the staged file so the restore can be
	// retried; it never reports success in this state.
	RestoreStateFailed RestoreState = "failed"
)

// RestoreStatus is a point-in-time snapshot of auto-restore progress, safe to
// expose through stats or diagnostics.
type RestoreStatus struct {
	State          RestoreState `json:"state"`
	Attempts       int          `json:"attempts"`
	LastError      string       `json:"last_error,omitempty"`
	RestorePath    string       `json:"restore_path,omitempty"`
	LastUpdateTime time.Time    `json:"last_update_time,omitempty"`
}

// RestoreStatus returns the current auto-restore status. It is safe for
// concurrent use.
func (s *Store) RestoreStatus() RestoreStatus {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	status := RestoreStatus{
		State:          s.restoreState,
		Attempts:       s.restoreAttempt,
		RestorePath:    s.restorePath,
		LastUpdateTime: s.restoreUpdateTime,
	}
	if s.restoreErr != nil {
		status.LastError = s.restoreErr.Error()
	}
	return status
}

// currentRestorePath returns the staged restore path under the restore lock.
func (s *Store) currentRestorePath() string {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	return s.restorePath
}

func (s *Store) setRestoreState(state RestoreState, err error) {
	s.restoreMu.Lock()
	defer s.restoreMu.Unlock()
	s.restoreState = state
	s.restoreErr = err
	s.restoreUpdateTime = time.Now()
}

// clearRestorePath drops the staged path and marks the given state, all under
// the restore lock so concurrent RestoreStatus readers never observe a torn
// update.
func (s *Store) clearRestorePath(state RestoreState) {
	s.restoreMu.Lock()
	defer s.restoreMu.Unlock()
	s.restoreState = state
	s.restoreErr = nil
	s.restoreUpdateTime = time.Now()
	s.restorePath = ""
}

// RetryRestore re-attempts a previously failed auto-restore. It returns
// ErrNoRestorePending if there is no staged restore (including after a
// successful one, whose file is removed), allowing an operator or health check
// to distinguish "nothing to retry" from a real retry failure. On failure the
// node stays in the retryable RestoreStateFailed state; on success it becomes
// ready and catches up with the cluster Raft log.
func (s *Store) RetryRestore() error {
	s.restoreMu.Lock()
	path := s.restorePath
	state := s.restoreState
	if path == "" || state != RestoreStateFailed {
		s.restoreMu.Unlock()
		return ErrNoRestorePending
	}
	s.restoreAttempt++
	s.restoreState = RestoreStateInProgress
	s.restoreUpdateTime = time.Now()
	s.restoreMu.Unlock()

	if err := s.installRestore(); err != nil {
		s.setRestoreState(RestoreStateFailed, err)
		return err
	}
	s.completeRestore()
	return nil
}
