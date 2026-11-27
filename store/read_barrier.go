package store

import (
	"errors"
	"expvar"
	"sync"
	"sync/atomic"
	"time"
)

// isBarrierTimeout reports whether a barrier wait ended because its
// deadline elapsed or the enclosing request was canceled, as opposed to
// a leadership/term confirmation failure.
func isBarrierTimeout(err error) bool {
	return errors.Is(err, ErrReadBarrierTimeout)
}

// readBarrierTracker observes the read barriers performed on behalf of
// LINEARIZABLE reads. All metrics are safe for concurrent use.
//
// A single Store.Query/Store.Request call performs at most one
// *confirmed* barrier. When a barrier attempt fails for a transient
// reason the Store may run the barrier again within the same call; each
// such extra attempt increments the retry counter and gets its own
// wait-duration sample, but only the final confirmed attempt counts as
// a successful confirmation. A failed attempt is therefore never also
// counted as a confirmation, so one request can never be recorded as
// two confirmations.
type readBarrierTracker struct {
	inFlight atomic.Int64

	waits        atomic.Int64
	waitFailures atomic.Int64
	timeouts     atomic.Int64
	retries      atomic.Int64
	lastWaitMS   atomic.Int64

	mu        sync.Mutex
	durations []time.Duration
}

// barrierSampleLimit is the maximum number of per-wait durations
// retained by the tracker. Old samples are evicted once the limit is
// reached so the slice cannot grow without bound.
const barrierSampleLimit = 128

func newReadBarrierTracker() *readBarrierTracker {
	return &readBarrierTracker{
		durations: make([]time.Duration, 0, barrierSampleLimit),
	}
}

// begin marks one barrier attempt as starting. It returns a function
// that must be invoked exactly once when the attempt completes; the
// function records the outcome (nil error for a confirmed barrier) and
// the wait duration. isRetry must be true for every attempt after the
// first within the same request: retries get their own wait-duration
// sample and increment the retry counter, while a single attempt only
// ever records one outcome, so a request is never counted as two
// confirmations.
func (t *readBarrierTracker) begin(isRetry bool) func(error) {
	t.inFlight.Add(1)
	if g := stats.Get(barrierInFlight); g != nil {
		g.(*expvar.Int).Add(1)
	}
	t.waits.Add(1)
	stats.Add(numBarrierWaits, 1)
	if isRetry {
		t.retries.Add(1)
		stats.Add(numBarrierRetries, 1)
	}
	start := time.Now()
	var done atomic.Bool
	return func(err error) {
		if !done.CompareAndSwap(false, true) {
			// Guard against double-counting if a caller mistakenly
			// invokes the completion function twice.
			return
		}
		dur := time.Since(start)
		ms := dur.Milliseconds()

		t.inFlight.Add(-1)
		if g := stats.Get(barrierInFlight); g != nil {
			g.(*expvar.Int).Add(-1)
		}
		t.lastWaitMS.Store(ms)
		stats.Get(barrierWaitDuration).(*expvar.Int).Set(ms)

		t.mu.Lock()
		t.durations = append(t.durations, dur)
		if len(t.durations) > barrierSampleLimit {
			t.durations = t.durations[len(t.durations)-barrierSampleLimit:]
		}
		t.mu.Unlock()

		if err != nil {
			t.waitFailures.Add(1)
			stats.Add(numBarrierWaitFailures, 1)
			if isBarrierTimeout(err) {
				t.timeouts.Add(1)
				stats.Add(numBarrierTimeouts, 1)
			}
		}
	}
}

// InFlight returns the number of barrier waits currently queued or
// running on this node.
func (t *readBarrierTracker) InFlight() int64 {
	return t.inFlight.Load()
}

// Waits returns the total number of barrier waits started.
func (t *readBarrierTracker) Waits() int64 {
	return t.waits.Load()
}

// Failures returns the total number of barrier waits that ended in an
// error, including timeouts.
func (t *readBarrierTracker) Failures() int64 {
	return t.waitFailures.Load()
}

// Timeouts returns the total number of barrier waits that ended because
// the configured barrier timeout elapsed.
func (t *readBarrierTracker) Timeouts() int64 {
	return t.timeouts.Load()
}

// Retries returns the number of barrier attempts that were retries,
// i.e. attempts after the first within the same read request.
func (t *readBarrierTracker) Retries() int64 {
	return t.retries.Load()
}

// LastWaitDuration returns the duration of the most recently completed
// barrier wait, or zero if none has completed yet.
func (t *readBarrierTracker) LastWaitDuration() time.Duration {
	return time.Duration(t.lastWaitMS.Load()) * time.Millisecond
}

// WaitDurations returns the duration of every retained barrier wait,
// oldest first. The returned slice is a copy.
func (t *readBarrierTracker) WaitDurations() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]time.Duration(nil), t.durations...)
}

// stats returns a map representation suitable for inclusion in
// Store.Stats(). All values are point-in-time reads.
func (t *readBarrierTracker) stats() map[string]any {
	durations := t.WaitDurations()
	samples := make([]int64, len(durations))
	for i := range durations {
		samples[i] = durations[i].Milliseconds()
	}
	return map[string]any{
		"in_flight":         t.InFlight(),
		"num_waits":         t.Waits(),
		"num_failures":      t.Failures(),
		"num_timeouts":      t.Timeouts(),
		"num_retries":       t.Retries(),
		"last_wait_ms":      t.LastWaitDuration().Milliseconds(),
		"wait_durations_ms": samples,
	}
}
