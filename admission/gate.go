// Package admission provides a bounded admission gate for write requests.
//
// The gate limits the number of write requests that may be in flight at any
// time, so that a burst of concurrent writes cannot grow process memory
// without bound. When the gate has no free slots, callers are either rejected
// immediately (PolicyReject) or wait until a slot becomes free, their context
// expires, or the gate is closed (PolicyWait). Every client is restricted to a
// fair share of the capacity while other clients are waiting, so no single
// client can monopolize the gate.
//
// Slots are represented by Tickets: a successful Acquire returns a Ticket
// which the caller MUST Release once the request no longer needs its reserved
// resources. All limits may be changed at runtime; new requests observe the
// new values immediately.
package admission

import (
	"context"
	"errors"
	"expvar"
	"sync"
	"sync/atomic"
	"time"
)

// stats captures stats for the admission gate.
var stats *expvar.Map

const (
	numAccepted      = "accepted"
	numRejectedFull  = "rejected_full"
	numRejectedWait  = "rejected_wait_limit"
	numTimeout       = "wait_timeouts"
	numClosed        = "closed_failures"
	numCapacity      = "capacity"
	numWaitLimit     = "wait_limit"
	numInFlight      = "in_flight"
	numWaiting       = "waiting"
	numClients       = "clients"
	numFairShare     = "fair_share"
	lastWaitDuration = "last_wait_duration"
)

// ewmaAlpha is the weight given to the most recent wait sample when updating
// the exponential moving average that backs RetryAfter.
const ewmaAlpha = 0.5

func init() {
	stats = expvar.NewMap("admission")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	// Counters.
	stats.Add(numAccepted, 0)
	stats.Add(numRejectedFull, 0)
	stats.Add(numRejectedWait, 0)
	stats.Add(numTimeout, 0)
	stats.Add(numClosed, 0)
	// Gauges, updated via Set for the rest of the process lifetime.
	stats.Add(numCapacity, 0)
	stats.Add(numWaitLimit, 0)
	stats.Add(numInFlight, 0)
	stats.Add(numWaiting, 0)
	stats.Add(numClients, 0)
	stats.Add(numFairShare, 0)
	stats.Add(lastWaitDuration, 0)
}

// setGauge publishes the current value of a gauge-style metric. All gauge
// keys are pre-registered in ResetStats.
func setGauge(key string, v int64) {
	stats.Get(key).(*expvar.Int).Set(v)
}

// Policy determines what Acquire does when no slot is immediately available.
type Policy int

const (
	// PolicyReject rejects an Acquire immediately when no slot is available.
	PolicyReject Policy = iota

	// PolicyWait blocks the caller until a slot becomes available, the
	// caller's context expires, or the gate is closed.
	PolicyWait
)

var (
	// ErrGateFull is returned by a PolicyReject Acquire when every slot is
	// occupied, or by a PolicyWait Acquire when the bounded wait queue is full.
	ErrGateFull = errors.New("admission gate full")

	// ErrGateClosed is returned by Acquire once the gate has been closed.
	// Waiters that were queued when Close was called receive this error so
	// that all outstanding callers observe the same failure.
	ErrGateClosed = errors.New("admission gate closed")
)

// Default bounds for the backoff hint returned to rejected callers.
const (
	defaultMinBackoff = 50 * time.Millisecond
	defaultMaxBackoff = 5 * time.Second
)

// Config holds the tunable parameters of a Gate.
type Config struct {
	// Capacity is the maximum number of requests that may hold a slot at
	// the same time. A value below 1 is treated as 1.
	Capacity int

	// WaitLimit bounds the number of callers that may be blocked in Acquire
	// at the same time. It bounds the memory used by waiting requests: even
	// under PolicyWait the gate never grows beyond Capacity + WaitLimit
	// admitted callers. A value below 1 is treated as 1.
	WaitLimit int

	// FairShare is the maximum number of slots a single client may occupy
	// while other clients are waiting. A client may exceed this share when
	// nobody else is waiting, so spare capacity is never wasted. If zero,
	// the share defaults to half the capacity (at least 1).
	FairShare int

	// MinBackoff and MaxBackoff bound the RetryAfter hint returned on a
	// rejected Acquire. Zero values select the package defaults.
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

// Ticket represents a slot reserved by Acquire. It must be returned with
// Release exactly once, after which it must not be used.
type Ticket struct {
	client   string
	gate     *Gate
	released atomic.Bool
}

// Gate is a bounded, fair admission gate for write requests. It is safe for
// concurrent use.
type Gate struct {
	mu sync.Mutex

	capacity   int
	waitLimit  int
	fairShare  int
	minBackoff time.Duration
	maxBackoff time.Duration

	inFlight int
	clients  map[string]int
	waiters  []*waiter
	closed   bool

	// wg tracks every granted ticket so Close can wait for in-flight writes
	// to finish without interrupting them.
	wg sync.WaitGroup

	// EWMA of the time a granted waiter spent queued, in nanoseconds. It is
	// the basis of the RetryAfter hint and is read without the lock.
	avgWaitNS atomic.Int64
}

type waiter struct {
	client     string
	enqueuedAt time.Time
	// ch has capacity one. It is sent once and closed when the waiter is
	// either granted a slot or failed (timeout cancellation aside, which
	// closes the channel directly).
	ch chan struct{}
	// err is meaningful only after ch is closed. nil means a slot was
	// granted; otherwise the caller must return it.
	err  error
	done bool
}

// New returns a Gate configured with the given values.
func New(cfg Config) *Gate {
	g := &Gate{
		capacity:   max(cfg.Capacity, 1),
		waitLimit:  max(cfg.WaitLimit, 1),
		fairShare:  max(cfg.FairShare, 0),
		minBackoff: cfg.MinBackoff,
		maxBackoff: cfg.MaxBackoff,
		clients:    make(map[string]int),
	}
	if g.minBackoff <= 0 {
		g.minBackoff = defaultMinBackoff
	}
	if g.maxBackoff <= 0 {
		g.maxBackoff = defaultMaxBackoff
	}
	if g.maxBackoff < g.minBackoff {
		g.maxBackoff = g.minBackoff
	}
	if g.fairShare == 0 {
		g.fairShare = (g.capacity + 1) / 2
	}
	g.publishConfig()
	return g
}

// Acquire reserves a slot for the identified client. The client key is opaque
// to the gate and only needs to be stable per client; an empty key identifies
// the anonymous client. On success the returned Ticket MUST be released.
//
// With PolicyReject the call returns ErrGateFull immediately when no slot is
// available. With PolicyWait it blocks, bounded by ctx and by the configured
// WaitLimit; it returns ErrGateFull when the wait queue is full, the context
// error when ctx expires before a slot is granted, or ErrGateClosed if the
// gate was closed while the call was queued.
func (g *Gate) Acquire(ctx context.Context, client string, policy Policy) (*Ticket, error) {
	g.mu.Lock()
	if g.closed {
		stats.Add(numClosed, 1)
		g.mu.Unlock()
		return nil, ErrGateClosed
	}
	if g.canAdmit(client) {
		g.admitLocked(client)
		g.mu.Unlock()
		return &Ticket{client: client, gate: g}, nil
	}

	if policy == PolicyReject {
		stats.Add(numRejectedFull, 1)
		g.mu.Unlock()
		return nil, ErrGateFull
	}
	if len(g.waiters) >= g.waitLimit {
		stats.Add(numRejectedWait, 1)
		g.mu.Unlock()
		return nil, ErrGateFull
	}

	w := &waiter{
		client:     client,
		enqueuedAt: time.Now(),
		ch:         make(chan struct{}, 1),
	}
	g.waiters = append(g.waiters, w)
	setGauge(numWaiting, int64(len(g.waiters)))
	g.mu.Unlock()

	select {
	case <-w.ch:
		if w.err != nil {
			return nil, w.err
		}
		return &Ticket{client: client, gate: g}, nil
	case <-ctx.Done():
		return nil, g.cancelWaiter(w, ctx.Err())
	}
}

// Release returns the ticket's slot to the gate. It is a no-op on a nil gate
// (admission disabled), a nil ticket, and is safe against a duplicate call on
// the same ticket.
func (g *Gate) Release(t *Ticket) {
	if g == nil || t == nil || t.gate != g || !t.released.CompareAndSwap(false, true) {
		return
	}
	g.mu.Lock()
	g.inFlight--
	if g.clients[t.client] <= 1 {
		delete(g.clients, t.client)
	} else {
		g.clients[t.client]--
	}
	setGauge(numInFlight, int64(g.inFlight))
	setGauge(numClients, int64(len(g.clients)))
	g.mu.Unlock()

	g.wg.Done()
	g.pump()
}

// Close stops admitting new requests and fails every queued waiter with
// ErrGateClosed. It blocks until all previously granted tickets have been
// released, so in-flight writes are allowed to complete rather than being
// interrupted. A closed gate never reopens; calling Close more than once is a
// no-op.
func (g *Gate) Close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	waiters := g.drainWaitersLocked(ErrGateClosed)
	g.mu.Unlock()
	g.notify(waiters)

	// Wait for every in-flight request to release its slot. Releases remain
	// valid on a closed gate.
	g.wg.Wait()
}

// SetCapacity changes the slot limit. The new value applies immediately to
// subsequent Acquire calls and to queued waiters, without restarting the
// gate. If the new limit is below the current in-flight count, outstanding
// requests are allowed to finish; only new admissions pause until the count
// drops below the limit. A value below 1 is treated as 1.
func (g *Gate) SetCapacity(n int) {
	n = max(n, 1)
	g.mu.Lock()
	g.capacity = n
	// A fair share above the capacity is meaningless; clamp it but never
	// shrink an explicit value that still fits the new capacity.
	if g.fairShare > n {
		g.fairShare = n
	}
	g.publishConfigLocked()
	g.mu.Unlock()
	g.pump()
}

// SetWaitLimit changes the maximum number of callers that may wait at once.
// The new value applies immediately. Shrinking it does not cancel callers
// already queued; it only prevents further waiters from joining.
func (g *Gate) SetWaitLimit(n int) {
	n = max(n, 1)
	g.mu.Lock()
	g.waitLimit = n
	g.publishConfigLocked()
	g.mu.Unlock()
}

// SetFairShare changes the per-client fair-share limit. If zero, the share
// defaults to half the current capacity. The new value is observed by the
// next admission decision.
func (g *Gate) SetFairShare(n int) {
	g.mu.Lock()
	g.fairShare = max(n, 0)
	if g.fairShare == 0 {
		g.fairShare = (g.capacity + 1) / 2
	}
	g.publishConfigLocked()
	g.mu.Unlock()
	g.pump()
}

// SetMinBackoff changes the lower bound applied to the RetryAfter hint. A
// non-positive value is ignored. If the new minimum exceeds the current
// maximum, the maximum is raised to match.
func (g *Gate) SetMinBackoff(d time.Duration) {
	if d <= 0 {
		return
	}
	g.mu.Lock()
	g.minBackoff = d
	if g.maxBackoff < d {
		g.maxBackoff = d
	}
	g.mu.Unlock()
}

// SetMaxBackoff changes the upper bound applied to the RetryAfter hint. A
// non-positive value is ignored. If the new maximum is below the current
// minimum, the minimum is lowered to match.
func (g *Gate) SetMaxBackoff(d time.Duration) {
	if d <= 0 {
		return
	}
	g.mu.Lock()
	g.maxBackoff = d
	if g.minBackoff > d {
		g.minBackoff = d
	}
	g.mu.Unlock()
}

// Depth returns the number of slots currently held by in-flight requests.
func (g *Gate) Depth() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inFlight
}

// Waiting returns the number of callers currently blocked in Acquire.
func (g *Gate) Waiting() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.waiters)
}

// Capacity returns the current slot limit.
func (g *Gate) Capacity() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.capacity
}

// RetryAfter returns a suggested delay before a rejected client retries. It
// is derived from the average time queued requests take to clear, clamped to
// the configured backoff bounds.
func (g *Gate) RetryAfter() time.Duration {
	g.mu.Lock()
	min, max := g.minBackoff, g.maxBackoff
	g.mu.Unlock()

	d := time.Duration(g.avgWaitNS.Load())
	if d <= 0 {
		// No wait history yet (e.g. an immediate reject): suggest the minimum
		// backoff rather than returning zero.
		return min
	}
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

// Stats returns a snapshot of the gate's operational stats, suitable for a
// node status endpoint. The same values are published to the "admission"
// expvar map for metrics collection.
func (g *Gate) Stats() map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return map[string]any{
		"capacity":    g.capacity,
		"wait_limit":  g.waitLimit,
		"fair_share":  g.fairShare,
		"in_flight":   g.inFlight,
		"waiting":     len(g.waiters),
		"num_clients": len(g.clients),
		"retry_after": g.retryAfterLocked().String(),
	}
}

// canAdmit reports whether a request from client may take a slot now.
// Caller must hold g.mu.
func (g *Gate) canAdmit(client string) bool {
	if g.closed || g.inFlight >= g.capacity {
		return false
	}
	if g.clients[client] < g.fairShare {
		return true
	}
	// The client is at or over its fair share. It may use a spare slot only
	// when no other client is queued, which keeps a single client from
	// holding every slot while others wait.
	for _, w := range g.waiters {
		if w.client != client {
			return false
		}
	}
	return true
}

// admitLocked grants a slot and updates the in-flight counters. Caller must
// hold g.mu.
func (g *Gate) admitLocked(client string) {
	g.inFlight++
	g.clients[client]++
	g.wg.Add(1)
	stats.Add(numAccepted, 1)
	setGauge(numInFlight, int64(g.inFlight))
	setGauge(numClients, int64(len(g.clients)))
}

// pump grants slots to queued waiters, in queue order, while admission
// remains possible. An ineligible waiter retains its position and is simply
// skipped: later eligible waiters are still admitted, and every Release or
// limit change triggers another pass, so a skipped waiter is re-evaluated as
// soon as the state changes. FIFO ordering guarantees it cannot starve once
// it becomes eligible.
func (g *Gate) pump() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	var granted []*waiter
	for _, w := range g.waiters {
		if g.canAdmit(w.client) {
			g.admitLocked(w.client)
			w.done = true
			granted = append(granted, w)
		}
	}
	// Drop every granted waiter while preserving the relative FIFO order of
	// the rest.
	final := g.waiters[:0]
	for _, w := range g.waiters {
		if !w.done {
			final = append(final, w)
		}
	}
	g.waiters = final
	setGauge(numWaiting, int64(len(g.waiters)))
	g.mu.Unlock()

	for _, w := range granted {
		g.observeWait(time.Since(w.enqueuedAt))
		// Buffered channel: this never blocks, even if the caller has not
		// yet reached its select.
		w.ch <- struct{}{}
		close(w.ch)
	}
}

// cancelWaiter removes a queued waiter whose context has expired. If the
// waiter was granted concurrently, the slot is released immediately and the
// caller still receives its context error.
func (g *Gate) cancelWaiter(target *waiter, ctxErr error) error {
	g.mu.Lock()
	if target.done {
		g.mu.Unlock()
		g.Release(&Ticket{client: target.client, gate: g})
		return ctxErr
	}
	target.done = true
	final := g.waiters[:0]
	for _, w := range g.waiters {
		if w != target {
			final = append(final, w)
		}
	}
	g.waiters = final
	stats.Add(numTimeout, 1)
	setGauge(numWaiting, int64(len(g.waiters)))
	g.mu.Unlock()

	close(target.ch)
	g.pump()
	return ctxErr
}

// drainWaitersLocked fails every queued waiter with err and returns the
// affected waiters so the caller can signal them without holding the lock.
// Caller must hold g.mu.
func (g *Gate) drainWaitersLocked(err error) []*waiter {
	waiters := g.waiters
	for _, w := range waiters {
		w.done = true
		w.err = err
	}
	g.waiters = nil
	stats.Add(numClosed, int64(len(waiters)))
	setGauge(numWaiting, 0)
	return waiters
}

// notify signals every drained waiter.
func (g *Gate) notify(waiters []*waiter) {
	for _, w := range waiters {
		w.ch <- struct{}{}
		close(w.ch)
	}
}

// observeWait folds a completed wait into the EWMA and publishes the latest
// sample as a gauge. The gauge is kept in milliseconds to match the duration
// unit convention used by the OTLP bridge.
func (g *Gate) observeWait(d time.Duration) {
	prev := g.avgWaitNS.Load()
	var next int64
	if prev == 0 {
		next = int64(d)
	} else {
		next = int64(ewmaAlpha*float64(d) + (1-ewmaAlpha)*float64(prev))
	}
	g.avgWaitNS.Store(next)
	setGauge(lastWaitDuration, time.Duration(next).Milliseconds())
}

// retryAfterLocked is the lock-held variant of RetryAfter for Stats.
// Caller must hold g.mu.
func (g *Gate) retryAfterLocked() time.Duration {
	d := time.Duration(g.avgWaitNS.Load())
	if d <= 0 {
		return g.minBackoff
	}
	if d < g.minBackoff {
		return g.minBackoff
	}
	if d > g.maxBackoff {
		return g.maxBackoff
	}
	return d
}

func (g *Gate) publishConfig() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.publishConfigLocked()
}

// publishConfigLocked publishes the current limit values. Caller must hold g.mu.
func (g *Gate) publishConfigLocked() {
	setGauge(numCapacity, int64(g.capacity))
	setGauge(numWaitLimit, int64(g.waitLimit))
	setGauge(numFairShare, int64(g.fairShare))
}
