package http

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/store"
)

// newBarrierService builds a Service whose local Store returns the
// provided query error, so the HTTP mapping can be exercised without a
// real cluster.
func newBarrierService(t *testing.T, queryErr error) *Service {
	t.Helper()
	m := &MockStore{
		queryFn: func(qr *proto.QueryRequest) ([]*proto.QueryRows, uint64, error) {
			return nil, 0, queryErr
		},
	}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	return s
}

func Test_Query_BarrierTimeoutMapsTo504(t *testing.T) {
	barrierErr := fmt.Errorf("index 9: %w: %w",
		store.ErrReadBarrierTimeout, store.ErrWaitForFSMTimeout)
	s := newBarrierService(t, barrierErr)

	req := httptest.NewRequest(http.MethodGet, "/db/query?level=linearizable&q=SELECT+1", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 for barrier timeout, got %d (body=%s)", w.Code, w.Body.String())
	}
}

func Test_Query_BarrierFailureMapsTo503(t *testing.T) {
	barrierErr := fmt.Errorf("%w: verify leader: %s",
		store.ErrReadBarrier, store.ErrNotLeader.Error())
	s := newBarrierService(t, barrierErr)

	req := httptest.NewRequest(http.MethodGet, "/db/query?level=linearizable&q=SELECT+1", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for unconfirmed barrier, got %d (body=%s)", w.Code, w.Body.String())
	}
}

func Test_Query_GenericErrorStill200WithBodyError(t *testing.T) {
	// An error that is neither a barrier error nor a routing error keeps
	// the legacy behavior: HTTP 200 with the error rendered in the JSON
	// body, so existing client parsing is unchanged.
	s := newBarrierService(t, errors.New("some ordinary query failure"))

	req := httptest.NewRequest(http.MethodGet, "/db/query?q=SELECT+1", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("ordinary errors must keep legacy 200 response, got %d", w.Code)
	}
}

func Test_Query_BarrierCountersIncremented(t *testing.T) {
	ResetStats()
	barrierErr := fmt.Errorf("%w: stale read", store.ErrReadBarrier)
	s := newBarrierService(t, barrierErr)

	req := httptest.NewRequest(http.MethodGet, "/db/query?level=linearizable&q=SELECT+1", nil)
	s.ServeHTTP(httptest.NewRecorder(), req)

	if got := stats.Get(numReadBarrierFailed).String(); got != "1" {
		t.Fatalf("expected read_barrier_failed=1, got %s", got)
	}

	timeoutErr := fmt.Errorf("index 3: %w: %w",
		store.ErrReadBarrierTimeout, store.ErrWaitForFSMTimeout)
	s2 := newBarrierService(t, timeoutErr)
	s2.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/db/query?level=linearizable&q=SELECT+1", nil))
	if got := stats.Get(numReadBarrierTimeout).String(); got != "1" {
		t.Fatalf("expected read_barrier_timeout=1, got %s", got)
	}
}
