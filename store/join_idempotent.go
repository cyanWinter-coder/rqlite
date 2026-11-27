package store

import (
	"errors"
	"sync"
	"time"

	clusterproto "github.com/rqlite/rqlite/v10/cluster/proto"
	commandproto "github.com/rqlite/rqlite/v10/command/proto"
)

var (
	// ErrJoinRequestInvalid is returned when a join request has neither a node
	// ID nor address that can be used as an idempotency key.
	ErrJoinRequestInvalid = errors.New("join request requires node ID or address")
)

// JoinState describes the state of one node-join operation.
type JoinState string

const (
	JoinStateQueued        JoinState = "queued"
	JoinStateJoining       JoinState = "joining"
	JoinStateAlreadyMember JoinState = "already_member"
	JoinStateJoined        JoinState = "joined"
	JoinStateFailed        JoinState = "failed"

	// Coarse status aliases for callers modeling idle/in-progress/complete.
	JoinStateIdle       JoinState = "idle"
	JoinStateInProgress JoinState = "in_progress"
	JoinStateComplete   JoinState = "complete"
)

// JoinResult is the confirmed membership state returned by an idempotent join.
type JoinResult struct {
	// MemberID and MemberAddr identify the confirmed cluster member.
	MemberID   string `json:"member_id"`
	MemberAddr string `json:"member_addr"`

	// Voter reports whether the member is a voting member.
	Voter bool `json:"voter"`

	// AlreadyMember is true when the request was an idempotent no-op.
	AlreadyMember bool `json:"already_member"`

	// LeaderID and LeaderAddr identify the current leader.
	LeaderID   string `json:"leader_id"`
	LeaderAddr string `json:"leader_addr"`

	// Server is the confirmed member, retained for compatibility with code that
	// already uses the Store Server type.
	Server *Server `json:"server,omitempty"`

	// Member is an alias for Server.
	Member *Server `json:"member,omitempty"`

	// Leader is the confirmed leader Server, when available.
	Leader *Server `json:"leader,omitempty"`
}

// JoinStatus is a queryable snapshot of a join operation.
type JoinStatus struct {
	NodeID     string      `json:"node_id"`
	InProgress bool        `json:"in_progress"`
	State      JoinState   `json:"state"`
	StartedAt  time.Time   `json:"started_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
	Result     *JoinResult `json:"result,omitempty"`
	Error      string      `json:"error,omitempty"`
	Err        error       `json:"-"`
}

// MembershipChangeStatus describes the currently active membership change.
type MembershipChangeStatus struct {
	InProgress bool        `json:"in_progress"`
	Operation  string      `json:"operation,omitempty"`
	NodeID     string      `json:"node_id,omitempty"`
	StartedAt  time.Time   `json:"started_at,omitempty"`
	Join       *JoinStatus `json:"join,omitempty"`
}

type joinCall struct {
	key       string
	request   *commandproto.JoinRequest
	startedAt time.Time

	mu        sync.RWMutex
	state     JoinState
	updatedAt time.Time
	result    *JoinResult
	err       error
	done      chan struct{}
}

func newJoinCall(key string, req *commandproto.JoinRequest) *joinCall {
	now := time.Now()
	return &joinCall{
		key:       key,
		request:   req,
		startedAt: now,
		updatedAt: now,
		state:     JoinStateQueued,
		done:      make(chan struct{}),
	}
}

func (c *joinCall) setState(state JoinState) {
	c.mu.Lock()
	c.state = state
	c.updatedAt = time.Now()
	c.mu.Unlock()
}

func (c *joinCall) finish(result *JoinResult, err error) {
	c.mu.Lock()
	if err != nil {
		c.state = JoinStateFailed
	} else if result != nil && result.AlreadyMember {
		c.state = JoinStateAlreadyMember
	} else {
		c.state = JoinStateJoined
	}
	c.result = result
	c.err = err
	c.updatedAt = time.Now()
	c.mu.Unlock()
	close(c.done)
}

func (c *joinCall) wait() (*JoinResult, error) {
	<-c.done
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.result, c.err
}

func (c *joinCall) snapshot() JoinStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st := JoinStatus{
		NodeID:     c.key,
		InProgress: c.state == JoinStateQueued || c.state == JoinStateJoining,
		State:      c.state,
		StartedAt:  c.startedAt,
		UpdatedAt:  c.updatedAt,
		Result:     c.result,
	}
	if c.err != nil {
		st.Err = c.err
		st.Error = c.err.Error()
	}
	return st
}

type activeMembershipChange struct {
	operation string
	nodeID    string
	startedAt time.Time
	call      *joinCall
}

func joinRequestKey(jr *commandproto.JoinRequest) (string, error) {
	if jr == nil {
		return "", ErrJoinRequestInvalid
	}
	if jr.Id != "" {
		return jr.Id, nil
	}
	if jr.Address != "" {
		return "addr:" + jr.Address, nil
	}
	return "", ErrJoinRequestInvalid
}

// JoinWithResult joins a node idempotently and returns the confirmed membership
// state. Concurrent or repeated requests for the same node share one operation
// and therefore receive the same result or error. Membership changes for
// different nodes are serialized so remove/add operations cannot interleave.
func (s *Store) JoinWithResult(jr *commandproto.JoinRequest) (*JoinResult, error) {
	key, err := joinRequestKey(jr)
	if err != nil {
		return nil, err
	}

	call, leader := s.getOrCreateJoinCall(key, jr)
	if !leader {
		return call.wait()
	}

	call.setState(JoinStateQueued)
	s.membershipChangeMu.Lock()
	defer s.membershipChangeMu.Unlock()

	s.setActiveMembership(&activeMembershipChange{
		operation: "join",
		nodeID:    key,
		startedAt: call.startedAt,
		call:      call,
	})
	defer s.clearActiveMembership()

	call.setState(JoinStateJoining)
	result, err := s.joinNow(jr)
	call.finish(result, err)
	return result, err
}

// JoinIdempotent is an alias for JoinWithResult.
func (s *Store) JoinIdempotent(jr *commandproto.JoinRequest) (*JoinResult, error) {
	return s.JoinWithResult(jr)
}

// IdempotentJoin is an alias for JoinWithResult.
func (s *Store) IdempotentJoin(jr *commandproto.JoinRequest) (*JoinResult, error) {
	return s.JoinWithResult(jr)
}

// JoinResponse adapts the confirmed join result to the internode wire response.
// The cluster service discovers this method through an optional interface, so
// the core Manager interface and existing mocks remain unchanged.
func (s *Store) JoinResponse(jr *commandproto.JoinRequest) (*clusterproto.CommandJoinResponse, error) {
	result, err := s.JoinWithResult(jr)
	if err != nil {
		return nil, err
	}
	state := string(JoinStateJoined)
	if result.AlreadyMember {
		state = string(JoinStateAlreadyMember)
	}
	return &clusterproto.CommandJoinResponse{
		Id:            result.MemberID,
		MemberId:      result.MemberID,
		Leader:        result.LeaderAddr,
		LeaderId:      result.LeaderID,
		State:         state,
		AlreadyMember: result.AlreadyMember,
	}, nil
}

func (s *Store) getOrCreateJoinCall(key string, jr *commandproto.JoinRequest) (*joinCall, bool) {
	s.joinCallsMu.Lock()
	defer s.joinCallsMu.Unlock()

	if s.joinCalls == nil {
		s.joinCalls = make(map[string]*joinCall)
	}
	if call, ok := s.joinCalls[key]; ok {
		select {
		case <-call.done:
			// The previous generation is complete. Replace it atomically and
			// let this caller lead the new generation.
		default:
			return call, false
		}
	}
	call := newJoinCall(key, jr)
	s.joinCalls[key] = call
	return call, true
}

// JoinStatus returns the latest status for a node join operation.
func (s *Store) JoinStatus(nodeID string) (JoinStatus, bool) {
	s.joinCallsMu.Lock()
	call := s.joinCalls[nodeID]
	s.joinCallsMu.Unlock()
	if call == nil {
		return JoinStatus{}, false
	}
	return call.snapshot(), true
}

// GetJoinStatus is an alias for JoinStatus.
func (s *Store) GetJoinStatus(nodeID string) (JoinStatus, bool) {
	return s.JoinStatus(nodeID)
}

// IsJoinInProgress reports whether a node's join is active.
func (s *Store) IsJoinInProgress(nodeID string) bool {
	st, ok := s.JoinStatus(nodeID)
	return ok && st.InProgress
}

// JoinStatuses returns snapshots of all retained join operations, keyed by node
// ID or address-derived idempotency key.
func (s *Store) JoinStatuses() map[string]JoinStatus {
	s.joinCallsMu.Lock()
	calls := make([]*joinCall, 0, len(s.joinCalls))
	keys := make([]string, 0, len(s.joinCalls))
	for k, c := range s.joinCalls {
		keys = append(keys, k)
		calls = append(calls, c)
	}
	s.joinCallsMu.Unlock()

	out := make(map[string]JoinStatus, len(calls))
	for i, c := range calls {
		out[keys[i]] = c.snapshot()
	}
	return out
}

// MembershipChangeStatus reports whether a membership change is active.
func (s *Store) MembershipChangeStatus() MembershipChangeStatus {
	s.joinCallsMu.Lock()
	active := s.activeMembership
	s.joinCallsMu.Unlock()

	if active == nil {
		return MembershipChangeStatus{}
	}
	st := MembershipChangeStatus{
		InProgress: true,
		Operation:  active.operation,
		NodeID:     active.nodeID,
		StartedAt:  active.startedAt,
	}
	if active.call != nil {
		js := active.call.snapshot()
		st.Join = &js
	}
	return st
}

// GetMembershipStatus is an alias for MembershipChangeStatus.
func (s *Store) GetMembershipStatus() MembershipChangeStatus {
	return s.MembershipChangeStatus()
}

// MembershipChangeInProgress reports whether any join/remove/demote/promote
// operation is active.
func (s *Store) MembershipChangeInProgress() bool {
	return s.MembershipChangeStatus().InProgress
}

func (s *Store) setActiveMembership(change *activeMembershipChange) {
	s.joinCallsMu.Lock()
	s.activeMembership = change
	s.joinCallsMu.Unlock()
}

func (s *Store) clearActiveMembership() {
	s.joinCallsMu.Lock()
	s.activeMembership = nil
	s.joinCallsMu.Unlock()
}

// withMembershipChange serializes a non-join membership mutation with joins and
// other configuration changes.
func (s *Store) withMembershipChange(operation, nodeID string, fn func() error) error {
	s.membershipChangeMu.Lock()
	defer s.membershipChangeMu.Unlock()

	startedAt := time.Now()
	s.setActiveMembership(&activeMembershipChange{
		operation: operation,
		nodeID:    nodeID,
		startedAt: startedAt,
	})
	defer s.clearActiveMembership()
	return fn()
}
