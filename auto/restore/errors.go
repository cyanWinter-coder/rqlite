package restore

import (
	"errors"
	"fmt"

	"github.com/rqlite/rqlite/v10/auto/verify"
)

// ErrVerificationFailed wraps a snapshot verification failure. It exists so
// callers can distinguish a corrupt (or unverifiable) snapshot from a
// transient download error: a corrupt snapshot must always block startup and
// be quarantined for manual investigation, even when the restore config asks
// the node to continue through other download failures.
var ErrVerificationFailed = errors.New("snapshot verification failed")

// IsVerificationError reports whether err is (or wraps) a snapshot
// verification failure rather than a transient transport error.
func IsVerificationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrVerificationFailed) {
		return true
	}
	return errors.Is(err, verify.ErrChecksumMismatch) ||
		errors.Is(err, verify.ErrSizeMismatch) ||
		errors.Is(err, verify.ErrNotVerifiable)
}

// verificationError wraps the underlying checksum error so it can be
// distinguished from transient download failures.
type verificationError struct {
	err           error
	quarantineDir string
}

func (e *verificationError) Error() string {
	if e.quarantineDir != "" {
		return fmt.Sprintf("%s: %v (corrupt snapshot quarantined at %s)",
			ErrVerificationFailed, e.err, e.quarantineDir)
	}
	return fmt.Sprintf("%s: %v", ErrVerificationFailed, e.err)
}

func (e *verificationError) Unwrap() error {
	return e.err
}

// Is lets errors.Is match both the verification sentinel and the underlying
// checksum error.
func (e *verificationError) Is(target error) bool {
	return target == ErrVerificationFailed
}
