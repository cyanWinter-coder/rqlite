package proxy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/store"
)

func Test_WrapReadBarrier_LocalSentinelPassesThrough(t *testing.T) {
	t.Parallel()

	local := fmt.Errorf("%w: verify leader: nope", store.ErrReadBarrier)
	if got := wrapIfUnauthorized(local); !errors.Is(got, ErrReadBarrier) {
		t.Fatalf("local barrier error must keep sentinel, got %v", got)
	}

	localTimeout := fmt.Errorf("index 7: %w: %w",
		store.ErrReadBarrierTimeout, store.ErrWaitForFSMTimeout)
	if got := wrapIfUnauthorized(localTimeout); !errors.Is(got, ErrReadBarrierTimeout) {
		t.Fatalf("local barrier timeout must keep sentinel, got %v", got)
	}
}

func Test_WrapReadBarrier_RemoteStringReclassified(t *testing.T) {
	t.Parallel()

	// Remote nodes return errors as bare strings; the prefix must still be
	// recognized and re-wrapped with the identifiable sentinel.
	remoteFail := errors.New(store.ErrReadBarrier.Error() + ": verify leader: not leader")
	got := wrapIfUnauthorized(remoteFail)
	if !errors.Is(got, ErrReadBarrier) {
		t.Fatalf("remote barrier string must be re-wrapped, got %v", got)
	}
	if errors.Is(got, ErrReadBarrierTimeout) {
		t.Fatalf("a confirmation failure is not a timeout: %v", got)
	}

	remoteTimeout := errors.New("index 9: " + store.ErrReadBarrierTimeout.Error() +
		": " + store.ErrWaitForFSMTimeout.Error())
	got = wrapIfUnauthorized(remoteTimeout)
	if !errors.Is(got, ErrReadBarrierTimeout) {
		t.Fatalf("remote timeout string must be re-wrapped, got %v", got)
	}
}

func Test_WrapReadBarrier_UnrelatedErrorUntouched(t *testing.T) {
	t.Parallel()

	other := errors.New("database is locked")
	if got := wrapIfUnauthorized(other); got != other {
		t.Fatalf("unrelated error must pass through unchanged, got %v", got)
	}
}

func Test_Query_LocalBarrierErrorNotDowngraded(t *testing.T) {
	t.Parallel()

	barrierErr := fmt.Errorf("%w: stale read", store.ErrReadBarrier)
	s := &mockStore{
		queryFn: func(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error) {
			return nil, proto.ConsistencyLevel_LINEARIZABLE, 0, barrierErr
		},
	}
	p := newTestProxy(s, &mockCluster{})

	// A non-NotLeader barrier error is returned as-is and never triggers
	// forwarding or a local read.
	rows, _, _, err := p.Query(context.Background(), &proto.QueryRequest{}, nil, time.Second, 0, false)
	if !errors.Is(err, ErrReadBarrier) {
		t.Fatalf("expected ErrReadBarrier, got %v", err)
	}
	if rows != nil {
		t.Fatalf("a failed barrier must not return rows, got %v", rows)
	}
}
