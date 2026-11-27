package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v10/cluster/proto"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

var (
	// ErrNodeIDRequired is returned when a join request doesn't supply a node ID
	ErrNodeIDRequired = errors.New("node required")

	// ErrJoinFailed is returned when a node fails to join a cluster
	ErrJoinFailed = errors.New("failed to join cluster")

	// ErrJoinCanceled is returned when a join operation is canceled
	ErrJoinCanceled = errors.New("join operation canceled")

	// ErrNotifyFailed is returned when a node fails to notify another node
	ErrNotifyFailed = errors.New("failed to notify node")
)

const (
	defaultJoinBackoffMultiplier = 2.0
	defaultMaxJoinBackoff        = 30 * time.Second
)

// Joiner executes a node-join operation.
type Joiner struct {
	numAttempts     int
	attemptInterval time.Duration

	// totalTimeout bounds Do/DoWithResult when the supplied context has no
	// earlier deadline. Zero means no Joiner-imposed total timeout.
	totalTimeout time.Duration

	// backoffMultiplier controls exponential growth of the wait between full
	// attempt rounds. A value <= 1 keeps the interval constant.
	backoffMultiplier float64
	maxBackoff        time.Duration

	client *Client
	creds  *proto.Credentials
	logger *log.Logger

	statusMu sync.RWMutex
	status   JoinStatus
	statuses map[string]JoinStatus
}

// NewJoiner returns an instantiated Joiner.
func NewJoiner(client *Client, numAttempts int, attemptInterval time.Duration) *Joiner {
	return &Joiner{
		client:            client,
		numAttempts:       numAttempts,
		attemptInterval:   attemptInterval,
		backoffMultiplier: defaultJoinBackoffMultiplier,
		maxBackoff:        defaultMaxJoinBackoff,
		logger:            log.New(os.Stderr, "[cluster-join] ", log.LstdFlags),
		statuses:          make(map[string]JoinStatus),
	}
}

// SetCredentials sets the credentials for the Joiner.
func (j *Joiner) SetCredentials(creds *proto.Credentials) {
	j.creds = creds
}

// SetTotalTimeout sets the total time budget for a Do or DoWithResult call.
// A non-positive duration removes the Joiner-imposed budget.
func (j *Joiner) SetTotalTimeout(d time.Duration) {
	j.totalTimeout = d
}

// SetBackoff configures exponential backoff between attempt rounds. The first
// wait remains the attempt interval supplied to NewJoiner. A multiplier <= 1
// selects constant intervals; maxInterval <= 0 disables capping.
func (j *Joiner) SetBackoff(multiplier float64, maxInterval time.Duration) {
	j.backoffMultiplier = multiplier
	j.maxBackoff = maxInterval
}

// Do makes the actual join request. If the join is successful with any address,
// that address is returned. Otherwise, an error is returned.
func (j *Joiner) Do(ctx context.Context, targetAddrs []string, id, addr string, suf command.Suffrage) (string, error) {
	result, err := j.DoWithResult(ctx, targetAddrs, id, addr, suf)
	if err != nil {
		return "", err
	}
	return result.Joinee, nil
}

// DoWithResult is like Do, but returns the confirmed member and leader state
// from the successful join response when the server provides it.
func (j *Joiner) DoWithResult(ctx context.Context, targetAddrs []string, id, addr string, suf command.Suffrage) (result *JoinOutcome, retErr error) {
	if id == "" {
		return nil, ErrNodeIDRequired
	}
	j.beginJoinStatus(id)
	defer func() {
		j.completeJoinStatus(id, result, retErr)
	}()

	runCtx := ctx
	if j.totalTimeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, j.totalTimeout)
		defer cancel()
	}

	var err error
	var outcome *JoinOutcome
	attempts := 0
	for i := 0; i < j.numAttempts; i++ {
		for _, ta := range targetAddrs {
			attempts++
			select {
			case <-runCtx.Done():
				return nil, j.contextError(runCtx)
			default:
				outcome, err = j.join(runCtx, ta, id, addr, suf)
				if err == nil {
					outcome.Attempts = attempts
					if outcome.MemberID == "" {
						outcome.MemberID = id
					}
					if outcome.MemberAddr == "" {
						outcome.MemberAddr = addr
					}
					outcome.NormalizeAliases()
					return outcome, nil
				}
				helpMsg := ""
				if j.creds == nil {
					helpMsg = " (did you forget to set -join-as?)"
				}
				j.logger.Printf("failed to join via node at %s: %s%s", ta, err, helpMsg)
			}
		}
		if i+1 < j.numAttempts {
			delay := j.backoffInterval(i)
			// This log message only makes sense if performing more than 1 join-attempt.
			j.logger.Printf("failed to join cluster at %s, sleeping %s before retry", targetAddrs, delay)
			timer := time.NewTimer(delay)
			select {
			case <-runCtx.Done():
				timer.Stop()
				return nil, j.contextError(runCtx)
			case <-timer.C:
				// Proceed with the next attempt.
			}
		}
	}
	j.logger.Printf("failed to join cluster at %s, after %d attempt(s)", targetAddrs, j.numAttempts)
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return nil, ErrJoinTimeout
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrJoinFailed, err)
	}
	return nil, ErrJoinFailed
}

// Join is an alias for DoWithResult.
func (j *Joiner) Join(ctx context.Context, targetAddrs []string, id, addr string, suf command.Suffrage) (*JoinResult, error) {
	return j.DoWithResult(ctx, targetAddrs, id, addr, suf)
}

// JoinWithResult is an alias for DoWithResult.
func (j *Joiner) JoinWithResult(ctx context.Context, targetAddrs []string, id, addr string, suf command.Suffrage) (*JoinResult, error) {
	return j.DoWithResult(ctx, targetAddrs, id, addr, suf)
}

// DoIdempotent is an alias for DoWithResult.
func (j *Joiner) DoIdempotent(ctx context.Context, targetAddrs []string, id, addr string, suf command.Suffrage) (*JoinResult, error) {
	return j.DoWithResult(ctx, targetAddrs, id, addr, suf)
}

// Status returns the most recent or currently running join status.
func (j *Joiner) Status() JoinStatus {
	j.statusMu.RLock()
	defer j.statusMu.RUnlock()
	return j.status
}

// JoinStatus returns the retained status for a node ID.
func (j *Joiner) JoinStatus(nodeID string) (JoinStatus, bool) {
	j.statusMu.RLock()
	defer j.statusMu.RUnlock()
	st, ok := j.statuses[nodeID]
	return st, ok
}

// StatusFor is an alias for JoinStatus.
func (j *Joiner) StatusFor(nodeID string) (JoinStatus, bool) {
	return j.JoinStatus(nodeID)
}

// GetJoinStatus is an alias for JoinStatus.
func (j *Joiner) GetJoinStatus(nodeID string) (JoinStatus, bool) {
	return j.JoinStatus(nodeID)
}

// JoinStatuses returns all retained join statuses.
func (j *Joiner) JoinStatuses() map[string]JoinStatus {
	j.statusMu.RLock()
	defer j.statusMu.RUnlock()
	out := make(map[string]JoinStatus, len(j.statuses))
	for k, v := range j.statuses {
		out[k] = v
	}
	return out
}

// Stats implements the HTTP status reporter interface.
func (j *Joiner) Stats() (map[string]any, error) {
	return map[string]any{"joins": j.JoinStatuses()}, nil
}

func (j *Joiner) beginJoinStatus(id string) {
	now := time.Now()
	st := JoinStatus{
		NodeID:     id,
		InProgress: true,
		State:      JoinStateJoining,
		StartedAt:  now,
		UpdatedAt:  now,
	}
	j.statusMu.Lock()
	if existing, ok := j.statuses[id]; ok && !existing.StartedAt.IsZero() {
		st.StartedAt = existing.StartedAt
	}
	j.status = st
	j.statuses[id] = st
	j.statusMu.Unlock()
}

func (j *Joiner) completeJoinStatus(id string, result *JoinResult, err error) {
	j.statusMu.Lock()
	defer j.statusMu.Unlock()
	st, ok := j.statuses[id]
	if !ok {
		st = JoinStatus{NodeID: id, StartedAt: time.Now()}
	}
	st.InProgress = false
	st.UpdatedAt = time.Now()
	st.Result = result
	st.Outcome = result
	st.Err = err
	if err != nil {
		st.Error = err.Error()
		switch {
		case errors.Is(err, ErrJoinTimeout):
			st.State = JoinStateTimedOut
		case errors.Is(err, ErrJoinCanceled):
			st.State = JoinStateFailed
		default:
			st.State = JoinStateFailed
		}
	} else if result != nil {
		st.Error = ""
		st.Err = nil
		st.State = result.State
	} else {
		st.State = JoinStateSucceeded
	}
	j.status = st
	j.statuses[id] = st
}

func (j *Joiner) contextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ErrJoinTimeout
	}
	return ErrJoinCanceled
}

func (j *Joiner) backoffInterval(completedRound int) time.Duration {
	d := j.attemptInterval
	if j.backoffMultiplier > 1 && completedRound > 0 {
		d = time.Duration(float64(j.attemptInterval) * math.Pow(j.backoffMultiplier, float64(completedRound)))
	}
	if j.maxBackoff > 0 && d > j.maxBackoff {
		d = j.maxBackoff
	}
	if d < 0 {
		return 0
	}
	return d
}

func (j *Joiner) join(ctx context.Context, targetAddr, id, addr string, suf command.Suffrage) (*JoinOutcome, error) {
	req := &command.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   suf == command.Suffrage_VOTER,
	}

	// Attempt to join. Use the response to expose the current leader and, on
	// newer servers, the confirmed member state.
	resp, err := j.client.JoinResponse(ctx, req, targetAddr, j.creds, time.Second)
	if err != nil {
		return nil, err
	}
	outcome := (&JoinResult{
		Joinee:        targetAddr,
		MemberID:      firstNonEmpty(resp.GetMemberId(), resp.GetId(), id),
		NodeID:        id,
		ID:            firstNonEmpty(resp.GetId(), resp.GetMemberId(), id),
		MemberAddr:    addr,
		Address:       addr,
		Addr:          addr,
		Voter:         req.Voter,
		AlreadyMember: resp.GetAlreadyMember() || resp.GetState() == string(JoinStateAlreadyMember),
		Leader:        resp.GetLeader(),
		LeaderAddr:    resp.GetLeader(),
		LeaderID:      resp.GetLeaderId(),
		State:         JoinState(resp.GetState()),
	}).NormalizeAliases()
	return outcome, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
