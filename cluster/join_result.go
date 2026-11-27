package cluster

import (
	"context"
	"errors"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

var (
	// ErrJoinTimeout is returned when a join does not reach a terminal
	// successful state within its total time budget. It implements Is so
	// errors.Is(err, context.DeadlineExceeded) also succeeds.
	ErrJoinTimeout = joinTimeoutError{}

	// ErrJoinInProgress is returned when a caller attempts to start a second
	// concurrent join operation for the same node without joining the existing
	// operation.
	ErrJoinInProgress = errors.New("join already in progress")

	// ErrMembershipChangeInProgress is returned when a membership change for
	// another node is already running and the caller asked for immediate
	// rejection rather than queueing.
	ErrMembershipChangeInProgress = errors.New("membership change in progress")

	// ErrNoLeader is returned when a node cannot route a join to a leader.
	ErrNoLeader = errors.New("no leader")
)

type joinTimeoutError struct{}

func (joinTimeoutError) Error() string {
	return "join timed out"
}

// Timeout reports the error as a timeout to clients that use that interface.
func (joinTimeoutError) Timeout() bool { return true }

// Is lets errors.Is treat ErrJoinTimeout and context.DeadlineExceeded as the
// same timeout class without requiring callers to match a wrapped value.
func (joinTimeoutError) Is(target error) bool {
	return target == context.DeadlineExceeded
}

// JoinState is the state of a node-join operation.
type JoinState string

const (
	// JoinStateUnknown is the zero/unknown state.
	JoinStateUnknown JoinState = ""

	// JoinStateQueued means the request is waiting behind another membership
	// change.
	JoinStateQueued JoinState = "queued"

	// JoinStateWaitingLeader means no leader is currently available and the
	// operation is backing off before another attempt.
	JoinStateWaitingLeader JoinState = "waiting_for_leader"

	// JoinStateRetrying is an alias for the waiting/retry state.
	JoinStateRetrying JoinState = "retrying"

	// JoinStateJoining means a join RPC or Raft configuration change is in
	// flight.
	JoinStateJoining JoinState = "joining"

	// JoinStateInProgress is a generic in-progress state.
	JoinStateInProgress JoinState = "in_progress"

	// JoinStateAlreadyMember means the node was already present with the same
	// identity and address.
	JoinStateAlreadyMember JoinState = "already_member"

	// JoinStateJoined means the node was added by this operation.
	JoinStateJoined JoinState = "joined"

	// JoinStateSucceeded means the operation reached a successful terminal
	// state. It is also used for idempotent repeats that found the node present.
	JoinStateSucceeded JoinState = "succeeded"

	// JoinStateComplete is an alias for succeeded.
	JoinStateComplete JoinState = "complete"

	// JoinStateFailed means the operation hit a non-retryable error.
	JoinStateFailed JoinState = "failed"

	// JoinStateError is an alias for failed.
	JoinStateError JoinState = "error"

	// JoinStateTimedOut means the caller's total wait elapsed. A reconciler may
	// continue trying in the background after returning this state.
	JoinStateTimedOut JoinState = "timed_out"

	// Short status aliases used by callers that model status as idle/running/
	// complete/error.
	JoinIdle     = JoinStateUnknown
	JoinRunning  = JoinStateJoining
	JoinComplete = JoinStateSucceeded
	JoinError    = JoinStateFailed
)

// MemberInfo is the observed membership state of one node.
type MemberInfo struct {
	ID     string `json:"id,omitempty"`
	Addr   string `json:"addr,omitempty"`
	Voter  bool   `json:"voter"`
	Leader bool   `json:"leader,omitempty"`
}

// JoinResult is the terminal result of an idempotent join. Short and explicit
// field aliases are populated so callers can use whichever naming style their
// code already uses.
type JoinResult struct {
	// NodeID/ID/MemberID all identify the confirmed Raft member.
	NodeID   string `json:"node_id,omitempty"`
	ID       string `json:"id,omitempty"`
	MemberID string `json:"member_id"`

	// Address/Addr/MemberAddr all contain the member's Raft address.
	Address    string `json:"address,omitempty"`
	Addr       string `json:"addr,omitempty"`
	MemberAddr string `json:"member_addr,omitempty"`

	// Voter reports whether the confirmed member is a voting member.
	Voter bool `json:"voter"`

	// AlreadyMember is true when the request was an idempotent no-op.
	AlreadyMember bool `json:"already_member"`

	// Leader/LeaderAddr contain the current leader's Raft address.
	Leader     string `json:"leader,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`

	// LeaderID is the current leader's Raft ID.
	LeaderID string `json:"leader_id,omitempty"`

	// State is the terminal join/member state.
	State JoinState `json:"state,omitempty"`

	// Member is the nested confirmed member view.
	Member *MemberInfo `json:"member,omitempty"`

	// CurrentLeader is an explicit alias used by some script-facing responses.
	CurrentLeader string `json:"current_leader,omitempty"`

	// Joinee is the target address that served the successful join RPC.
	Joinee string `json:"joinee,omitempty"`

	// Attempts is the number of RPC rounds used.
	Attempts int `json:"attempts,omitempty"`
}

// JoinOutcome is retained as the older name for JoinResult.
type JoinOutcome = JoinResult

// NormalizeAliases fills omitted aliases from the canonical fields.
func (r *JoinResult) NormalizeAliases() *JoinResult {
	if r == nil {
		return nil
	}
	if r.MemberID == "" {
		r.MemberID = firstNonEmpty(r.ID, r.NodeID)
	}
	if r.ID == "" {
		r.ID = r.MemberID
	}
	if r.NodeID == "" {
		r.NodeID = r.MemberID
	}
	if r.MemberAddr == "" {
		r.MemberAddr = firstNonEmpty(r.Address, r.Addr)
	}
	if r.Address == "" {
		r.Address = r.MemberAddr
	}
	if r.Addr == "" {
		r.Addr = r.MemberAddr
	}
	if r.LeaderAddr == "" {
		r.LeaderAddr = r.Leader
	}
	if r.Leader == "" {
		r.Leader = r.LeaderAddr
	}
	if r.State == "" {
		if r.AlreadyMember {
			r.State = JoinStateAlreadyMember
		} else {
			r.State = JoinStateJoined
		}
	}
	if r.CurrentLeader == "" {
		r.CurrentLeader = r.LeaderAddr
	}
	if r.Member == nil {
		r.Member = &MemberInfo{
			ID:     r.MemberID,
			Addr:   r.MemberAddr,
			Voter:  r.Voter,
			Leader: r.LeaderID != "" && r.MemberID == r.LeaderID,
		}
	}
	return r
}

// JoinStatus is a point-in-time snapshot of a join operation.
type JoinStatus struct {
	NodeID     string               `json:"node_id"`
	InProgress bool                 `json:"in_progress"`
	State      JoinState            `json:"state"`
	Attempt    int                  `json:"attempt,omitempty"`
	StartedAt  time.Time            `json:"started_at,omitempty"`
	UpdatedAt  time.Time            `json:"updated_at,omitempty"`
	Result     *JoinResult          `json:"result,omitempty"`
	Outcome    *JoinResult          `json:"outcome,omitempty"`
	Error      string               `json:"error,omitempty"`
	Err        error                `json:"-"`
	Request    *command.JoinRequest `json:"-"`
}

// MembershipStatus describes the currently active membership change, if any.
type MembershipStatus struct {
	InProgress bool        `json:"in_progress"`
	Operation  string      `json:"operation,omitempty"`
	NodeID     string      `json:"node_id,omitempty"`
	StartedAt  time.Time   `json:"started_at,omitempty"`
	Join       *JoinStatus `json:"join,omitempty"`
}

// Stats renders the status in the shape used by /status StatusReporters.
func (m MembershipStatus) Stats() (map[string]any, error) {
	return map[string]any{
		"in_progress": m.InProgress,
		"operation":   m.Operation,
		"node_id":     m.NodeID,
		"started_at":  m.StartedAt,
		"join":        m.Join,
	}, nil
}
