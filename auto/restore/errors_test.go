package restore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rqlite/rqlite/v10/auto/verify"
)

func Test_IsVerificationError(t *testing.T) {
	ve := &verificationError{err: verify.ErrChecksumMismatch}
	if !IsVerificationError(ve) {
		t.Fatal("wrapped verification error should be recognized")
	}
	if !errors.Is(ve, ErrVerificationFailed) {
		t.Fatal("errors.Is must match the verification sentinel")
	}
	if !errors.Is(ve, verify.ErrChecksumMismatch) {
		t.Fatal("errors.Is must unwrap to the underlying checksum error")
	}
	if IsVerificationError(errors.New("connection reset")) {
		t.Fatal("a transient transport error is not a verification error")
	}
	if IsVerificationError(nil) {
		t.Fatal("nil must not be a verification error")
	}
}

func Test_VerificationError_IncludesQuarantinePath(t *testing.T) {
	ve := &verificationError{
		err:           verify.ErrSizeMismatch,
		quarantineDir: "/var/lib/rqlite/quarantine/snapshot-1",
	}
	s := ve.Error()
	if !bytes.Contains([]byte(s), []byte("/var/lib/rqlite/quarantine/snapshot-1")) {
		t.Fatalf("error must point the operator at the quarantine entry: %q", s)
	}
	if !bytes.Contains([]byte(s), []byte(ErrVerificationFailed.Error())) {
		t.Fatalf("error must carry the verification sentinel text: %q", s)
	}
}
