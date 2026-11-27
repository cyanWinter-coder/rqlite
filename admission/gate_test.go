package admission

import (
	"context"
	"errors"
	"expvar"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestGate(t *testing.T, cfg Config) *Gate {
	t.Helper()
	ResetStats()
	return New(cfg)
}

func Test_RejectPolicyWhenFull(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 2, WaitLimit: 2})
	defer g.Close()

	t1, err := g.Acquire(context.Background(), "c1", PolicyReject)
	if err != nil {
		t.Fatalf("first acquire failed: %s", err)
	}
	t2, err := g.Acquire(context.Background(), "c1", PolicyReject)
	if err != nil {
		t.Fatalf("second acquire failed: %s", err)
	}
	if _, err := g.Acquire(context.Background(), "c2", PolicyReject); !errors.Is(err, ErrGateFull) {
		t.Fatalf("expected ErrGateFull, got %v", err)
	}
	if got, exp := g.Depth(), 2; got != exp {
		t.Fatalf("depth: exp %d, got %d", exp, got)
	}

	g.Release(t1)
	g.Release(t2)
	if got := g.Depth(); got != 0 {
		t.Fatalf("depth after release: exp 0, got %d", got)
	}
}

func Test_WaitPolicyGrantsOnRelease(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 1, WaitLimit: 4})
	defer g.Close()

	t1, err := g.Acquire(context.Background(), "c1", PolicyWait)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}

	granted := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), "c2", PolicyWait)
		if err != nil {
			t.Errorf("waiting acquire failed: %s", err)
			return
		}
		granted <- tk
	}()

	select {
	case <-granted:
		t.Fatalf("waiter was granted before a slot was released")
	case <-time.After(50 * time.Millisecond):
	}
	if got := g.Waiting(); got != 1 {
		t.Fatalf("waiting count: exp 1, got %d", got)
	}

	g.Release(t1)
	select {
	case tk := <-granted:
		g.Release(tk)
	case <-time.After(time.Second):
		t.Fatalf("waiter not granted after release")
	}
}

func Test_WaitLimitBounded(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 1, WaitLimit: 1})
	defer g.Close()

	t1, err := g.Acquire(context.Background(), "c1", PolicyWait)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	defer g.Release(t1)

	blocked := make(chan struct{})
	go func() {
		tk, err := g.Acquire(context.Background(), "c2", PolicyWait)
		if err == nil {
			g.Release(tk)
		}
		close(blocked)
	}()
	deadline := time.Now().Add(time.Second)
	for g.Waiting() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if g.Waiting() != 1 {
		t.Fatalf("expected one waiter, got %d", g.Waiting())
	}

	// The single wait slot is taken; a third caller must be rejected.
	if _, err := g.Acquire(context.Background(), "c3", PolicyWait); !errors.Is(err, ErrGateFull) {
		t.Fatalf("expected ErrGateFull, got %v", err)
	}
}

func Test_AcquireContextTimeout(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 1, WaitLimit: 2})
	defer g.Close()

	t1, err := g.Acquire(context.Background(), "c1", PolicyWait)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	defer g.Release(t1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = g.Acquire(ctx, "c2", PolicyWait)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatalf("acquire returned before context deadline")
	}
	if got := g.Waiting(); got != 0 {
		t.Fatalf("timed-out waiter should be removed, waiting=%d", got)
	}
	if got := stats.Get(numTimeout).(*expvar.Int).Value(); got != 1 {
		t.Fatalf("timeout counter: exp 1, got %d", got)
	}

	// After the timeout frees the wait slot, another waiter can join and
	// then be granted when the slot is released.
	tkCh := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), "c3", PolicyWait)
		if err != nil {
			t.Errorf("post-timeout acquire failed: %s", err)
			return
		}
		tkCh <- tk
	}()
	deadline := time.Now().Add(time.Second)
	for g.Waiting() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	g.Release(t1)
	select {
	case tk := <-tkCh:
		g.Release(tk)
	case <-time.After(time.Second):
		t.Fatalf("queued waiter not granted after release")
	}
}

func Test_CloseFailsWaitersAndWaitsForInFlight(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 1, WaitLimit: 4})

	t1, err := g.Acquire(context.Background(), "c1", PolicyWait)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := g.Acquire(context.Background(), "c2", PolicyWait)
		errCh <- err
	}()
	deadline := time.Now().Add(time.Second)
	for g.Waiting() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	closed := make(chan struct{})
	go func() {
		g.Close()
		close(closed)
	}()

	// Close must block until the in-flight ticket is released.
	select {
	case <-closed:
		t.Fatalf("Close returned while an in-flight ticket was held")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrGateClosed) {
			t.Fatalf("waiter: expected ErrGateClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("queued waiter not failed on close")
	}

	// New acquires fail with the same consistent error.
	if _, err := g.Acquire(context.Background(), "c3", PolicyReject); !errors.Is(err, ErrGateClosed) {
		t.Fatalf("post-close acquire: expected ErrGateClosed, got %v", err)
	}

	g.Release(t1)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatalf("Close did not complete after in-flight release")
	}
}

func Test_CloseIdempotent(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 1, WaitLimit: 1})
	g.Close()
	g.Close() // must not block or panic
}

func Test_FairSharePreventsMonopoly(t *testing.T) {
	// Capacity 4, fair share 2: one client may fill the gate when alone,
	// but once another client waits, the monopolist cannot take more than
	// its share while the other client has any queued request.
	g := newTestGate(t, Config{Capacity: 4, WaitLimit: 8, FairShare: 2})
	defer g.Close()

	var mono []*Ticket
	for range 4 {
		tk, err := g.Acquire(context.Background(), "mono", PolicyReject)
		if err != nil {
			t.Fatalf("monopolist acquire failed: %s", err)
		}
		mono = append(mono, tk)
	}

	otherGranted := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), "other", PolicyWait)
		if err != nil {
			t.Errorf("other client acquire failed: %s", err)
			return
		}
		otherGranted <- tk
	}()
	deadline := time.Now().Add(time.Second)
	for g.Waiting() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// With another client waiting, the monopolist is at capacity and over
	// its fair share: a direct acquire must be rejected.
	if _, err := g.Acquire(context.Background(), "mono", PolicyReject); !errors.Is(err, ErrGateFull) {
		t.Fatalf("monopolist should be rejected over fair share, got %v", err)
	}

	// Releasing two monopolist slots brings it to its fair share, at which
	// point the waiting other client is admitted.
	g.Release(mono[0])
	g.Release(mono[1])
	select {
	case tk := <-otherGranted:
		g.Release(tk)
	case <-time.After(time.Second):
		t.Fatalf("other client starved by monopolist")
	}
	for _, tk := range mono[2:] {
		g.Release(tk)
	}
}

func Test_FairShareDoesNotWasteIdleCapacity(t *testing.T) {
	// With no contention, a single client may exceed its fair share.
	g := newTestGate(t, Config{Capacity: 4, WaitLimit: 2, FairShare: 2})
	defer g.Close()

	var tickets []*Ticket
	for range 4 {
		tk, err := g.Acquire(context.Background(), "solo", PolicyReject)
		if err != nil {
			t.Fatalf("idle capacity should be usable, got %v", err)
		}
		tickets = append(tickets, tk)
	}
	for _, tk := range tickets {
		g.Release(tk)
	}
}

func Test_SetCapacityAppliesImmediately(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 2, WaitLimit: 4})
	defer g.Close()

	t1, err := g.Acquire(context.Background(), "c1", PolicyReject)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	t2, err := g.Acquire(context.Background(), "c2", PolicyReject)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	if _, err := g.Acquire(context.Background(), "c3", PolicyReject); !errors.Is(err, ErrGateFull) {
		t.Fatalf("expected full at capacity 2, got %v", err)
	}

	// Grow: the very next request observes the new capacity without restart.
	g.SetCapacity(3)
	if got := g.Capacity(); got != 3 {
		t.Fatalf("capacity: exp 3, got %d", got)
	}
	t3, err := g.Acquire(context.Background(), "c3", PolicyReject)
	if err != nil {
		t.Fatalf("acquire after grow failed: %s", err)
	}

	// Shrink below in-flight count: existing tickets keep running, new
	// requests are rejected until enough slots are released.
	g.SetCapacity(1)
	if _, err := g.Acquire(context.Background(), "c4", PolicyReject); !errors.Is(err, ErrGateFull) {
		t.Fatalf("expected full after shrink, got %v", err)
	}
	g.Release(t1)
	g.Release(t2)
	if _, err := g.Acquire(context.Background(), "c4", PolicyReject); !errors.Is(err, ErrGateFull) {
		t.Fatalf("still over shrunken capacity, got %v", err)
	}
	g.Release(t3)
	t4, err := g.Acquire(context.Background(), "c4", PolicyReject)
	if err != nil {
		t.Fatalf("acquire after draining below new capacity failed: %s", err)
	}
	g.Release(t4)
}

func Test_SetCapacityWakesWaiters(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 1, WaitLimit: 4})
	defer g.Close()

	t1, err := g.Acquire(context.Background(), "c1", PolicyWait)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}

	tkCh := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), "c2", PolicyWait)
		if err != nil {
			t.Errorf("waiting acquire failed: %s", err)
			return
		}
		tkCh <- tk
	}()
	deadline := time.Now().Add(time.Second)
	for g.Waiting() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	g.SetCapacity(2)
	select {
	case tk := <-tkCh:
		g.Release(tk)
	case <-time.After(time.Second):
		t.Fatalf("waiter not woken by capacity growth")
	}
	g.Release(t1)
}

func Test_ReleaseIdempotentAndNilSafe(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 1, WaitLimit: 1})
	defer g.Close()

	tk, err := g.Acquire(context.Background(), "c1", PolicyReject)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	var nilGate *Gate
	nilGate.Release(nil) // must not panic when admission is disabled
	g.Release(nil)
	g.Release(tk)
	g.Release(tk) // duplicate must not drive in-flight negative
	if got := g.Depth(); got != 0 {
		t.Fatalf("depth: exp 0, got %d", got)
	}
}

func Test_RetryAfterBounded(t *testing.T) {
	g := newTestGate(t, Config{
		Capacity:   1,
		WaitLimit:  1,
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
	})
	defer g.Close()

	// Without wait history the minimum backoff is suggested.
	if got := g.RetryAfter(); got != 100*time.Millisecond {
		t.Fatalf("default retry-after: exp 100ms, got %s", got)
	}

	g.observeWait(50 * time.Millisecond) // below min -> clamped up
	if got := g.RetryAfter(); got != 100*time.Millisecond {
		t.Fatalf("min-clamped retry-after: exp 100ms, got %s", got)
	}
	g.observeWait(10 * time.Second) // above max -> clamped down
	if got := g.RetryAfter(); got != 2*time.Second {
		t.Fatalf("max-clamped retry-after: exp 2s, got %s", got)
	}
}

func Test_StatsAndExpvar(t *testing.T) {
	g := newTestGate(t, Config{Capacity: 3, WaitLimit: 2})
	defer g.Close()

	tk, err := g.Acquire(context.Background(), "c1", PolicyReject)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	s := g.Stats()
	if got := s["in_flight"]; got != 1 {
		t.Fatalf("stats in_flight: exp 1, got %v", got)
	}
	if got := s["capacity"]; got != 3 {
		t.Fatalf("stats capacity: exp 3, got %v", got)
	}
	if got := stats.Get(numInFlight).(*expvar.Int).Value(); got != 1 {
		t.Fatalf("expvar in_flight: exp 1, got %d", got)
	}

	// Reject counters land in expvar, not just logs.
	t2, err := g.Acquire(context.Background(), "c2", PolicyReject)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	t3, err := g.Acquire(context.Background(), "c3", PolicyReject)
	if err != nil {
		t.Fatalf("acquire failed: %s", err)
	}
	if _, err := g.Acquire(context.Background(), "c4", PolicyReject); !errors.Is(err, ErrGateFull) {
		t.Fatalf("expected full, got %v", err)
	}
	if got := stats.Get(numRejectedFull).(*expvar.Int).Value(); got != 1 {
		t.Fatalf("rejected counter: exp 1, got %d", got)
	}
	g.Release(tk)
	g.Release(t2)
	g.Release(t3)
}

func Test_ConcurrentFairnessAndNoLeaks(t *testing.T) {
	// WaitLimit covers every concurrent request, so all 200 must eventually
	// be granted; the bounded-wait rejection path is covered separately.
	g := newTestGate(t, Config{Capacity: 16, WaitLimit: 256, FairShare: 8})
	defer g.Close()

	var wg sync.WaitGroup
	var rejected atomic.Int64
	clients := []string{"a", "b", "c", "d"}
	for _, c := range clients {
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				tk, err := g.Acquire(ctx, c, PolicyWait)
				if err != nil {
					rejected.Add(1)
					return
				}
				time.Sleep(time.Millisecond)
				g.Release(tk)
			}()
		}
	}
	wg.Wait()

	if got := g.Depth(); got != 0 {
		t.Fatalf("all tickets should be released, depth=%d", got)
	}
	if got := g.Waiting(); got != 0 {
		t.Fatalf("no waiters should remain, waiting=%d", got)
	}
	if got := rejected.Load(); got != 0 {
		t.Fatalf("no request should have failed, got %d", got)
	}
}
