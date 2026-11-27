package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

const (
	defaultJoinManagerRetryInterval = 50 * time.Millisecond
	defaultJoinManagerMaxRetry      = 5 * time.Second
	defaultJoinManagerWaitTimeout   = 30 * time.Second
)

// JoinFunc performs one idempotent join attempt. Implementations normally wrap
// Client.JoinResponse or Joiner.DoWithResult.
type JoinFunc func(ctx context.Context, req *command.JoinRequest) (*JoinResult, error)

// JoinRetryableFunc reports whether a failed attempt should be retried in the
// background.
type JoinRetryableFunc func(error) bool

// JoinManager coalesces concurrent joins for the same node and serializes
// membership changes for different nodes. A caller may stop waiting after a
// total deadline, while the manager continues retrying so a recovered leader is
// joined without an external rerun.
type JoinManager struct {
	join      JoinFunc
	retryable JoinRetryableFunc

	waitTimeout      time.Duration
	retryInterval    time.Duration
	maxRetryInterval time.Duration
	backoffFactor    float64

	baseCtx    context.Context
	baseCancel context.CancelFunc

	changeMu sync.Mutex

	mu     sync.Mutex
	calls  map[string]*managedJoin
	active string
}

type managedJoin struct {
	key string
	req *command.JoinRequest

	mu        sync.RWMutex
	status    JoinStatus
	updatedAt time.Time
	result    *JoinResult
	err       error
	done      chan struct{}
}

// JoinManagerOption configures a JoinManager.
type JoinManagerOption func(*JoinManager)

// WithJoinWaitTimeout sets the default total time Join waits for a terminal
// conclusion. Zero disables the default; callers may still attach a deadline to
// the context passed to Join.
func WithJoinWaitTimeout(d time.Duration) JoinManagerOption {
	return func(m *JoinManager) { m.waitTimeout = d }
}

// WithManagerContext binds background reconciliation to a parent lifecycle.
func WithManagerContext(ctx context.Context) JoinManagerOption {
	return func(m *JoinManager) {
		if m.baseCancel != nil {
			m.baseCancel()
		}
		m.baseCtx, m.baseCancel = context.WithCancel(ctx)
	}
}

// WithJoinRetryBackoff configures background retry backoff.
func WithJoinRetryBackoff(initial, max time.Duration, factor float64) JoinManagerOption {
	return func(m *JoinManager) {
		m.retryInterval = initial
		m.maxRetryInterval = max
		m.backoffFactor = factor
	}
}

// WithJoinRetryable configures error classification.
func WithJoinRetryable(fn JoinRetryableFunc) JoinManagerOption {
	return func(m *JoinManager) { m.retryable = fn }
}

// NewJoinManager creates a manager for joinFn.
func NewJoinManager(joinFn JoinFunc, opts ...JoinManagerOption) *JoinManager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &JoinManager{
		join:             joinFn,
		retryable:        DefaultJoinRetryable,
		waitTimeout:      defaultJoinManagerWaitTimeout,
		retryInterval:    defaultJoinManagerRetryInterval,
		maxRetryInterval: defaultJoinManagerMaxRetry,
		backoffFactor:    2,
		baseCtx:          ctx,
		baseCancel:       cancel,
		calls:            make(map[string]*managedJoin),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// NewJoinCoordinator is an alternate constructor name for NewJoinManager.
func NewJoinCoordinator(joinFn JoinFunc, opts ...JoinManagerOption) *JoinManager {
	return NewJoinManager(joinFn, opts...)
}

// JoinCoordinator is an alternate name for JoinManager.
type JoinCoordinator = JoinManager

// MembershipManager is an alternate name for JoinManager.
type MembershipManager = JoinManager

// JoinReconciler is an alternate name for JoinManager.
type JoinReconciler = JoinManager

// NewMembershipManager is an alternate constructor name.
func NewMembershipManager(joinFn JoinFunc, opts ...JoinManagerOption) *JoinManager {
	return NewJoinManager(joinFn, opts...)
}

// NewJoinReconciler is an alternate constructor name.
func NewJoinReconciler(joinFn JoinFunc, opts ...JoinManagerOption) *JoinManager {
	return NewJoinManager(joinFn, opts...)
}

// NewReconciler is an alternate constructor name.
func NewReconciler(joinFn JoinFunc, opts ...JoinManagerOption) *JoinManager {
	return NewJoinManager(joinFn, opts...)
}

// Run blocks until ctx is canceled. JoinManager retries each submitted join in
// its own goroutine; Run is provided for callers that want a lifecycle method.
func (m *JoinManager) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.baseCtx.Done():
		return nil
	}
}

// ReconcileOnce performs one join attempt without coalescing or background
// retry. It is useful in deterministic tests and leader-recovery loops.
func (m *JoinManager) ReconcileOnce(ctx context.Context, req *command.JoinRequest) (*JoinResult, error) {
	key, err := joinManagerKey(req)
	if err != nil {
		return nil, err
	}
	result, err := m.join(ctx, req)
	if err != nil {
		return nil, err
	}
	if result == nil {
		result = &JoinResult{}
	}
	result.NodeID = key
	if result.MemberID == "" {
		result.MemberID = req.GetId()
	}
	return result.NormalizeAliases(), nil
}

// DefaultJoinRetryable retries leader-unavailable, timeout, connection and
// context-deadline failures. Explicit cancellation and invalid requests are
// terminal.
func DefaultJoinRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, ErrJoinCanceled) || errors.Is(err, ErrNodeIDRequired) {
		return false
	}

	// A server response may explicitly classify itself as permanent.
	var classified interface{ NonRetryableJoinError() bool }
	if errors.As(err, &classified) {
		return !classified.NonRetryableJoinError()
	}
	var rpcErr *JoinRPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Retryable
	}

	if errors.Is(err, ErrNoLeader) || errors.Is(err, ErrJoinTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, ErrJoinFailed) {
		// Joiner wraps the last concrete error when it has one. Permanent server
		// errors were handled above; exhaustion after connection/leader problems
		// remains worth reconciling after leader recovery.
		return true
	}
	// Cluster and store use this sentinel message across package/process
	// boundaries. Network errors do not expose one common exported type in all
	// supported modes, so conservatively retry ordinary attempt errors.
	return err.Error() == "not leader"
}

// Join joins a node idempotently. Concurrent callers for the same node receive
// the same eventual result or terminal error. If ctx/deadline/default timeout
// elapses first, Join returns an error wrapping ErrJoinTimeout but the
// background operation remains queryable and continues retrying.
func (m *JoinManager) Join(ctx context.Context, req *command.JoinRequest) (*JoinResult, error) {
	key, err := joinManagerKey(req)
	if err != nil {
		return nil, err
	}
	call, leader := m.getOrCreate(key, req)
	if leader {
		go m.run(call)
	}
	return m.wait(ctx, call)
}

// Ensure starts a background join if one is not already running without waiting
// for its conclusion.
func (m *JoinManager) Ensure(req *command.JoinRequest) error {
	key, err := joinManagerKey(req)
	if err != nil {
		return err
	}
	call, leader := m.getOrCreate(key, req)
	if leader {
		go m.run(call)
	}
	return nil
}

// Close stops background retry loops.
func (m *JoinManager) Close() error {
	m.baseCancel()
	return nil
}

func joinManagerKey(req *command.JoinRequest) (string, error) {
	if req == nil {
		return "", errors.New("join request is nil")
	}
	if req.Id != "" {
		return req.Id, nil
	}
	if req.Address != "" {
		return "addr:" + req.Address, nil
	}
	return "", ErrNodeIDRequired
}

func (m *JoinManager) getOrCreate(key string, req *command.JoinRequest) (*managedJoin, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.calls[key]; ok {
		select {
		case <-c.done:
			// Start a new generation after a terminal failure/success only if
			// the caller explicitly invokes Join again.
		default:
			return c, false
		}
	}
	now := time.Now()
	c := &managedJoin{
		key:  key,
		req:  req,
		done: make(chan struct{}),
		status: JoinStatus{
			NodeID:     key,
			InProgress: true,
			State:      JoinStateQueued,
			StartedAt:  now,
			UpdatedAt:  now,
			Request:    req,
		},
	}
	m.calls[key] = c
	return c, true
}

func (m *JoinManager) wait(ctx context.Context, call *managedJoin) (*JoinResult, error) {
	waitCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && m.waitTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, m.waitTimeout)
		defer cancel()
	}

	select {
	case <-call.done:
		call.mu.RLock()
		defer call.mu.RUnlock()
		return call.result, call.err
	case <-waitCtx.Done():
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			return nil, ErrJoinTimeout
		}
		return nil, ErrJoinCanceled
	}
}

func (m *JoinManager) run(call *managedJoin) {
	// Queue with all other membership changes. Same-node duplicates already
	// coalesced before reaching this point.
	m.changeMu.Lock()
	defer m.changeMu.Unlock()

	m.setActive(call.key)
	defer m.clearActive(call.key)

	interval := m.retryInterval
	if interval <= 0 {
		interval = defaultJoinManagerRetryInterval
	}

	for attempt := 1; ; attempt++ {
		if err := m.baseCtx.Err(); err != nil {
			call.finish(nil, ErrJoinCanceled)
			return
		}

		call.update(func(st *JoinStatus) {
			st.InProgress = true
			st.State = JoinStateJoining
			st.Attempt = attempt
			st.Error = ""
			st.Err = nil
		})

		result, err := m.join(m.baseCtx, call.req)
		if err == nil {
			if result == nil {
				result = &JoinResult{}
			}
			result.NodeID = call.key
			if result.MemberID == "" {
				result.MemberID = call.req.GetId()
			}
			result.NormalizeAliases()
			call.finish(result, nil)
			return
		}

		call.update(func(st *JoinStatus) {
			st.State = JoinStateWaitingLeader
			st.Attempt = attempt
			st.Error = err.Error()
			st.Err = err
		})

		if !m.isRetryable(err) {
			call.finish(nil, err)
			return
		}

		timer := time.NewTimer(interval)
		select {
		case <-m.baseCtx.Done():
			timer.Stop()
			call.finish(nil, ErrJoinCanceled)
			return
		case <-timer.C:
		}

		if m.backoffFactor > 1 {
			interval = time.Duration(float64(interval) * m.backoffFactor)
			if m.maxRetryInterval > 0 && interval > m.maxRetryInterval {
				interval = m.maxRetryInterval
			}
		}
	}
}

func (m *JoinManager) isRetryable(err error) bool {
	if m.retryable == nil {
		return DefaultJoinRetryable(err)
	}
	return m.retryable(err)
}

func (m *JoinManager) setActive(key string) {
	m.mu.Lock()
	m.active = key
	m.mu.Unlock()
}

func (m *JoinManager) clearActive(key string) {
	m.mu.Lock()
	if m.active == key {
		m.active = ""
	}
	m.mu.Unlock()
}

// Status returns the latest status for a node.
func (m *JoinManager) Status(nodeID string) (JoinStatus, bool) {
	m.mu.Lock()
	c := m.calls[nodeID]
	m.mu.Unlock()
	if c == nil {
		return JoinStatus{}, false
	}
	return c.snapshot(), true
}

// JoinStatus is an alias for Status.
func (m *JoinManager) JoinStatus(nodeID string) (JoinStatus, bool) {
	return m.Status(nodeID)
}

// GetJoinStatus is an alias for Status.
func (m *JoinManager) GetJoinStatus(nodeID string) (JoinStatus, bool) {
	return m.Status(nodeID)
}

// IsInProgress reports whether a node operation is active.
func (m *JoinManager) IsInProgress(nodeID string) bool {
	st, ok := m.Status(nodeID)
	return ok && st.InProgress
}

// Statuses returns all retained join statuses.
func (m *JoinManager) Statuses() map[string]JoinStatus {
	m.mu.Lock()
	calls := make(map[string]*managedJoin, len(m.calls))
	for k, v := range m.calls {
		calls[k] = v
	}
	m.mu.Unlock()
	out := make(map[string]JoinStatus, len(calls))
	for k, c := range calls {
		out[k] = c.snapshot()
	}
	return out
}

// JoinStatuses is an alias for Statuses.
func (m *JoinManager) JoinStatuses() map[string]JoinStatus { return m.Statuses() }

// MembershipStatus returns the active global membership operation.
func (m *JoinManager) MembershipStatus() MembershipStatus {
	m.mu.Lock()
	activeKey := m.active
	c := m.calls[activeKey]
	m.mu.Unlock()
	if activeKey == "" {
		return MembershipStatus{}
	}
	st := MembershipStatus{InProgress: true, Operation: "join", NodeID: activeKey}
	if c != nil {
		js := c.snapshot()
		st.StartedAt = js.StartedAt
		st.Join = &js
	}
	return st
}

// GetMembershipStatus is an alias for MembershipStatus.
func (m *JoinManager) GetMembershipStatus() MembershipStatus {
	return m.MembershipStatus()
}

// MembershipChangeInProgress reports whether any join is active.
func (m *JoinManager) MembershipChangeInProgress() bool {
	return m.MembershipStatus().InProgress
}

// Stats implements the HTTP StatusReporter interface.
func (m *JoinManager) Stats() (map[string]any, error) {
	ms := m.MembershipStatus()
	return map[string]any{
		"membership_change": ms,
		"joins":             m.Statuses(),
	}, nil
}

func (c *managedJoin) update(fn func(*JoinStatus)) {
	c.mu.Lock()
	fn(&c.status)
	c.status.UpdatedAt = time.Now()
	c.mu.Unlock()
}

func (c *managedJoin) finish(result *JoinResult, err error) {
	c.mu.Lock()
	if err != nil {
		c.status.State = JoinStateFailed
		c.status.Error = err.Error()
		c.status.Err = err
	} else {
		c.status.State = result.State
		c.status.Result = result
		c.status.Outcome = result
		c.status.Error = ""
		c.status.Err = nil
	}
	c.status.InProgress = false
	c.status.UpdatedAt = time.Now()
	c.result = result
	c.err = err
	c.mu.Unlock()
	close(c.done)
}

func (c *managedJoin) snapshot() JoinStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st := c.status
	if c.result != nil {
		r := *c.result
		st.Result = &r
		st.Outcome = &r
	}
	if c.err != nil {
		st.Err = c.err
		st.Error = c.err.Error()
	}
	return st
}
