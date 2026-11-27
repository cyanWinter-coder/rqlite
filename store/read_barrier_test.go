package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
)

func Test_ReadBarrierTracker_SuccessAndFailure(t *testing.T) {
	ResetStats()
	tr := newReadBarrierTracker()

	// A successful first attempt: one wait, no failure, no retry.
	done := tr.begin(false)
	if got := tr.InFlight(); got != 1 {
		t.Fatalf("expected in-flight 1 during wait, got %d", got)
	}
	done(nil)
	if got := tr.InFlight(); got != 0 {
		t.Fatalf("expected in-flight 0 after wait, got %d", got)
	}
	if got := tr.Waits(); got != 1 {
		t.Fatalf("expected 1 wait, got %d", got)
	}
	if got := tr.Failures(); got != 0 {
		t.Fatalf("expected 0 failures, got %d", got)
	}
	if got := tr.Retries(); got != 0 {
		t.Fatalf("expected 0 retries, got %d", got)
	}
	if len(tr.WaitDurations()) != 1 {
		t.Fatalf("expected 1 duration sample, got %d", len(tr.WaitDurations()))
	}

	// A failed retry attempt: increments waits, failures and retries, but
	// not the success path. A single request is therefore never recorded
	// as two confirmations.
	done = tr.begin(true)
	done(errors.New("nope"))
	if got := tr.Waits(); got != 2 {
		t.Fatalf("expected 2 waits, got %d", got)
	}
	if got := tr.Failures(); got != 1 {
		t.Fatalf("expected 1 failure, got %d", got)
	}
	if got := tr.Retries(); got != 1 {
		t.Fatalf("expected 1 retry, got %d", got)
	}
	if len(tr.WaitDurations()) != 2 {
		t.Fatalf("expected 2 duration samples, got %d", len(tr.WaitDurations()))
	}
}

func Test_ReadBarrierTracker_TimeoutClassification(t *testing.T) {
	ResetStats()
	tr := newReadBarrierTracker()

	finish := tr.begin(false)
	finish(ErrReadBarrierTimeout)
	if got := tr.Timeouts(); got != 1 {
		t.Fatalf("expected 1 timeout, got %d", got)
	}
	if got := tr.Failures(); got != 1 {
		t.Fatalf("a timeout is also a failure, got %d", got)
	}

	// A wrapped timeout is still classified as a timeout.
	finish = tr.begin(true)
	finish(errors.Join(ErrReadBarrierTimeout, errors.New("ctx canceled")))
	if got := tr.Timeouts(); got != 2 {
		t.Fatalf("expected 2 timeouts, got %d", got)
	}

	// A non-timeout barrier failure is not counted as a timeout.
	finish = tr.begin(false)
	finish(ErrReadBarrier)
	if got := tr.Timeouts(); got != 2 {
		t.Fatalf("non-timeout failure must not increment timeouts, got %d", got)
	}
	if got := tr.Failures(); got != 3 {
		t.Fatalf("expected 3 failures, got %d", got)
	}
}

func Test_ReadBarrierTracker_DoubleFinishIgnored(t *testing.T) {
	ResetStats()
	tr := newReadBarrierTracker()

	finish := tr.begin(false)
	finish(ErrReadBarrier)
	finish(ErrReadBarrier) // must be a no-op
	if got := tr.Failures(); got != 1 {
		t.Fatalf("double finish must be counted once, got %d", got)
	}
	if got := tr.Waits(); got != 1 {
		t.Fatalf("double finish must not add waits, got %d", got)
	}
}

func Test_ReadBarrierTracker_DurationSampleBound(t *testing.T) {
	ResetStats()
	tr := newReadBarrierTracker()

	for range barrierSampleLimit + 10 {
		finish := tr.begin(false)
		finish(nil)
	}
	if got := len(tr.WaitDurations()); got != barrierSampleLimit {
		t.Fatalf("expected samples capped at %d, got %d", barrierSampleLimit, got)
	}
}

func Test_ReadBarrierTracker_StatsShape(t *testing.T) {
	ResetStats()
	tr := newReadBarrierTracker()
	finish := tr.begin(false)
	finish(nil)

	stats := tr.stats()
	for _, key := range []string{
		"in_flight", "num_waits", "num_failures", "num_timeouts",
		"num_retries", "last_wait_ms", "wait_durations_ms",
	} {
		if _, ok := stats[key]; !ok {
			t.Fatalf("stats map missing key %q", key)
		}
	}
}

// newLinearizableStore opens and bootstraps a single-node store with a
// table ready to query.
func newLinearizableStore(t *testing.T) *Store {
	t.Helper()
	s, ln := mustNewStore(t)
	t.Cleanup(func() { ln.Close() })
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	t.Cleanup(func() { s.Close(true) })
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	if _, _, err := s.Execute(context.Background(), er); err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	return s
}

func Test_LinearizableRead_BarrierStatsRecorded(t *testing.T) {
	ResetStats()
	s := newLinearizableStore(t)

	qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
	qr.Level = proto.ConsistencyLevel_LINEARIZABLE
	if _, _, _, err := s.Query(context.Background(), qr); err != nil {
		t.Fatalf("linearizable query failed: %s", err.Error())
	}

	// The first linearizable read in a term upgrades to a strong read;
	// run a second one so a real barrier wait is recorded.
	if _, _, _, err := s.Query(context.Background(), qr); err != nil {
		t.Fatalf("second linearizable query failed: %s", err.Error())
	}

	bs := s.ReadBarrierStats()
	if got := bs["num_waits"].(int64); got < 1 {
		t.Fatalf("expected at least 1 barrier wait, got %d", got)
	}
	if got := bs["num_failures"].(int64); got != 0 {
		t.Fatalf("expected no barrier failures on a healthy node, got %d", got)
	}
	if got := bs["in_flight"].(int64); got != 0 {
		t.Fatalf("expected no in-flight barriers after completion, got %d", got)
	}
	samples := bs["wait_durations_ms"].([]int64)
	if len(samples) < 1 {
		t.Fatalf("expected at least one wait-duration sample, got %v", samples)
	}
	// The same data must be reachable through Store.Stats.
	full, err := s.Stats()
	if err != nil {
		t.Fatalf("stats failed: %s", err.Error())
	}
	if _, ok := full["read_barrier"].(map[string]any); !ok {
		t.Fatalf("Store.Stats missing read_barrier section: %v", full)
	}
}

func Test_LinearizableRead_TimeoutReturnsIdentifiableError(t *testing.T) {
	ResetStats()
	s := newLinearizableStore(t)

	// Establish the strong-read term so subsequent reads take the barrier
	// path rather than the strong-read upgrade.
	seed := queryRequestFromString("SELECT * FROM foo", false, false, false)
	seed.Level = proto.ConsistencyLevel_LINEARIZABLE
	if _, _, _, err := s.Query(context.Background(), seed); err != nil {
		t.Fatalf("seed linearizable query failed: %s", err.Error())
	}

	// Force the barrier's FSM subscription to block: resetting the target
	// drops the applied-index watermark, so subscribing to the current
	// commit index waits. A short barrier timeout must then surface as the
	// identifiable ErrReadBarrierTimeout (wrapping ErrWaitForFSMTimeout),
	// never a silent local read.
	s.fsmTarget.Reset()
	err := s.runReadBarrier(context.Background(), s.raft.CurrentTerm(),
		25*time.Millisecond, false)
	if !errors.Is(err, ErrReadBarrierTimeout) {
		t.Fatalf("expected ErrReadBarrierTimeout, got %v", err)
	}
	if !errors.Is(err, ErrWaitForFSMTimeout) {
		t.Fatalf("timeout should wrap ErrWaitForFSMTimeout, got %v", err)
	}
	bs := s.ReadBarrierStats()
	if got := bs["num_timeouts"].(int64); got < 1 {
		t.Fatalf("expected a recorded timeout, got stats %v", bs)
	}
	if got := bs["num_failures"].(int64); got < 1 {
		t.Fatalf("a timeout is also a failure, got stats %v", bs)
	}

	// Same blocking setup, but canceling the enclosing context must be
	// classified as a barrier timeout as well.
	s.fsmTarget.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()
	err = s.runReadBarrier(ctx, s.raft.CurrentTerm(), time.Second, false)
	if !errors.Is(err, ErrReadBarrierTimeout) {
		t.Fatalf("expected ErrReadBarrierTimeout on cancel, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled barrier should wrap context.Canceled, got %v", err)
	}
}

func Test_LinearizableRead_BarrierFailureNeverDowngrades(t *testing.T) {
	ResetStats()
	s := newLinearizableStore(t)

	// Establish the strong-read term so reads take the barrier path.
	seed := queryRequestFromString("SELECT * FROM foo", false, false, false)
	seed.Level = proto.ConsistencyLevel_LINEARIZABLE
	if _, _, _, err := s.Query(context.Background(), seed); err != nil {
		t.Fatalf("seed query failed: %s", err.Error())
	}

	// With the FSM target held back the barrier cannot confirm. A timeout
	// is terminal: retrying against the same deadline would only multiply
	// latency, so the read returns the identifiable barrier error after
	// the single attempt rather than serving a local read.
	s.fsmTarget.Reset()
	qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
	qr.Level = proto.ConsistencyLevel_LINEARIZABLE
	qr.LinearizableTimeout = (5 * time.Millisecond).Nanoseconds()
	rows, _, _, err := s.Query(context.Background(), qr)
	if !errors.Is(err, ErrReadBarrierTimeout) {
		t.Fatalf("expected ErrReadBarrierTimeout, got %v", err)
	}
	if rows != nil {
		t.Fatalf("a failed barrier must not return rows, got %v", rows)
	}
	bs := s.ReadBarrierStats()
	if got := bs["num_waits"].(int64); got != 1 {
		t.Fatalf("a timeout is terminal, expected 1 attempt, got %d", got)
	}
	if got := bs["num_retries"].(int64); got != 0 {
		t.Fatalf("a timeout must not be retried, got %d", got)
	}
}

func Test_LinearizableRead_ZeroTimeoutStillBounded(t *testing.T) {
	ResetStats()
	s := newLinearizableStore(t)

	// Seed the strong-read term.
	seed := queryRequestFromString("SELECT 1", false, false, false)
	seed.Level = proto.ConsistencyLevel_LINEARIZABLE
	if _, _, _, err := s.Query(context.Background(), seed); err != nil {
		t.Fatalf("seed query failed: %s", err.Error())
	}

	// An explicit tiny timeout still applies; a healthy single node
	// applies the index immediately, so the read either confirms or fails
	// with an identifiable timeout -- never a silent local read.
	qr := queryRequestFromString("SELECT 1", false, false, false)
	qr.Level = proto.ConsistencyLevel_LINEARIZABLE
	qr.LinearizableTimeout = (1 * time.Nanosecond).Nanoseconds()
	_, _, _, err := s.Query(context.Background(), qr)
	if err != nil && !errors.Is(err, ErrReadBarrierTimeout) {
		t.Fatalf("expected success or ErrReadBarrierTimeout, got %v", err)
	}
}

func Test_LinearizableRead_TermChangeFailsBarrier(t *testing.T) {
	ResetStats()
	s := newLinearizableStore(t)

	// Seed so the node is a ready leader with a committed term.
	seed := queryRequestFromString("SELECT 1", false, false, false)
	seed.Level = proto.ConsistencyLevel_LINEARIZABLE
	if _, _, _, err := s.Query(context.Background(), seed); err != nil {
		t.Fatalf("seed query failed: %s", err.Error())
	}

	// Calling one barrier attempt with a stale term directly exercises the
	// term-change branch: leadership being the same but the term differing
	// must fail with ErrReadBarrier (not a timeout, not a local read), and
	// the attempt must be observable as a non-timeout failure.
	staleTerm := s.raft.CurrentTerm() + 1
	err := s.runReadBarrier(context.Background(), staleTerm, time.Second, false)
	if !errors.Is(err, ErrReadBarrier) {
		t.Fatalf("expected ErrReadBarrier on term change, got %v", err)
	}
	if errors.Is(err, ErrReadBarrierTimeout) {
		t.Fatalf("a term change is a confirmation failure, not a timeout: %v", err)
	}
	bs := s.ReadBarrierStats()
	if got := bs["num_failures"].(int64); got != 1 {
		t.Fatalf("expected 1 recorded failure, got %v", bs)
	}
	if got := bs["num_timeouts"].(int64); got != 0 {
		t.Fatalf("term change must not be a timeout, got %v", bs)
	}
}

func Test_LinearizableRead_NoneLevelUnaffected(t *testing.T) {
	ResetStats()
	s := newLinearizableStore(t)

	// A read that does not opt into LINEARIZABLE must not touch the
	// barrier tracker: existing latency and behavior are unchanged.
	qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
	qr.Level = proto.ConsistencyLevel_NONE
	if rows, _, _, err := s.Query(context.Background(), qr); err != nil || len(rows) != 1 {
		t.Fatalf("NONE read changed behavior: rows=%v err=%v", rows, err)
	}
	weak := queryRequestFromString("SELECT * FROM foo", false, false, false)
	weak.Level = proto.ConsistencyLevel_WEAK
	if rows, _, _, err := s.Query(context.Background(), weak); err != nil || len(rows) != 1 {
		t.Fatalf("WEAK read changed behavior: rows=%v err=%v", rows, err)
	}
	if got := s.ReadBarrierStats()["num_waits"].(int64); got != 0 {
		t.Fatalf("non-linearizable reads must not run a barrier, waits=%d", got)
	}
}
