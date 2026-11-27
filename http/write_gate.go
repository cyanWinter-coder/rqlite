package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v10/admission"
	"github.com/rqlite/rqlite/v10/auth"
	"github.com/rqlite/rqlite/v10/command/proto"
)

// defaultGateWaitTimeout bounds how long a request may wait for a write-gate
// slot. It guarantees a queued request releases its memory even if the client
// never cancels the HTTP request.
const defaultGateWaitTimeout = 30 * time.Second

// writeGateReject is the JSON body of a write-gate rejection. Successful
// write responses keep their existing format; this shape is used only when a
// request is shed before the write is submitted.
type writeGateReject struct {
	Error      string `json:"error"`
	QueueDepth int    `json:"queue_depth"`
	Waiting    int    `json:"waiting"`
	RetryAfter int64  `json:"retry_after_ms"`
}

// clientKey returns a stable identifier for the calling client. Authenticated
// clients are keyed by their username, anonymous clients by peer IP. All
// anonymous peers behind an unauthenticated deployment share the anonymous
// key, which is safe: fairness only restricts a key while *other* keys wait.
func clientKey(r *http.Request) string {
	if username, _, ok := r.BasicAuth(); ok && username != "" {
		return "user:" + username
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}

// isWriteStatement reports whether the statement may mutate the database.
// The check is deliberately conservative: only plain SELECT and EXPLAIN
// statements are treated as reads; anything else, including statements that
// fail to classify cheaply, is gated as a write so writes can never slip past
// the bound. Statements rewritten with a RETURNING clause carry ForceQuery so
// their result set can be returned, but they still mutate.
func isWriteStatement(stmt *proto.Statement) bool {
	if stmt == nil {
		return false
	}
	if stmt.ForceQuery {
		return true
	}
	first := firstSQLToken(stmt.Sql)
	return first != "select" && first != "explain"
}

// containsWrite reports whether any statement in the batch may mutate.
func containsWrite(stmts []*proto.Statement) bool {
	for _, stmt := range stmts {
		if isWriteStatement(stmt) {
			return true
		}
	}
	return false
}

// firstSQLToken returns the lower-cased first SQL keyword token, ignoring
// leading whitespace and SQL comments. It performs no full parsing, because
// the admission decision only needs to distinguish reads from everything
// else.
func firstSQLToken(sqlText string) string {
	i := 0
	for i < len(sqlText) {
		// Skip whitespace.
		if c := sqlText[i]; c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' {
			i++
			continue
		}
		// Skip line comments.
		if strings.HasPrefix(sqlText[i:], "--") {
			nl := strings.IndexByte(sqlText[i:], '\n')
			if nl < 0 {
				return ""
			}
			i += nl + 1
			continue
		}
		// Skip block comments.
		if strings.HasPrefix(sqlText[i:], "/*") {
			end := strings.Index(sqlText[i:], "*/")
			if end < 0 {
				return ""
			}
			i += end + 2
			continue
		}
		break
	}
	var b strings.Builder
	for i < len(sqlText) {
		c := sqlText[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '(' || c == ';' {
			break
		}
		b.WriteByte(c)
		i++
	}
	return strings.ToLower(b.String())
}

// gateWrite acquires a write-gate slot for the request and returns the
// ticket to release once the write has completed (or been rejected). The
// boolean is false when the request was shed, in which case a rejection
// response has already been written and the caller must return immediately.
// A nil ticket with a true boolean means the gate is disabled or the request
// contains no writes, so the caller proceeds without releasing anything.
func (s *Service) gateWrite(w http.ResponseWriter, r *http.Request, stmts []*proto.Statement) (*admission.Ticket, bool) {
	if s.writeGate == nil || !containsWrite(stmts) {
		return nil, true
	}

	waitTimeout := s.gateWaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = defaultGateWaitTimeout
	}
	ctx, cancel := context.WithTimeout(r.Context(), waitTimeout)
	defer cancel()

	ticket, err := s.writeGate.Acquire(ctx, clientKey(r), admission.PolicyWait)
	if err == nil {
		return ticket, true
	}

	retryAfter := s.writeGate.RetryAfter()
	body := writeGateReject{
		QueueDepth: s.writeGate.Depth(),
		Waiting:    s.writeGate.Waiting(),
		RetryAfter: retryAfter.Milliseconds(),
	}
	switch {
	case errors.Is(err, admission.ErrGateFull):
		stats.Add(numWriteGateRejected, 1)
		body.Error = "write queue is full, retry later"
	case errors.Is(err, admission.ErrGateClosed):
		stats.Add(numWriteGateClosed, 1)
		body.Error = "write queue is closing"
	default:
		// Context deadline: the request's own timeout, or the bounded gate
		// wait timeout. Either way the queued request is released here.
		stats.Add(numWriteGateTimedOut, 1)
		body.Error = "timed out waiting for a write queue slot"
	}
	// Retry-After is specified in whole seconds; round up so a sub-second
	// hint never collapses to zero.
	w.Header().Set("Retry-After", strconv.FormatInt((retryAfter.Milliseconds()+999)/1000, 10))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	if encErr := json.NewEncoder(w).Encode(body); encErr != nil {
		s.logger.Printf("failed to encode write-gate rejection: %s", encErr.Error())
	}
	return nil, false
}

// handleWriteGate serves the write-gate admin endpoint. GET returns the
// current gate stats; POST applies any supplied limit changes immediately,
// without restarting the node, and returns the resulting stats.
func (s *Service) handleWriteGate(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	if !s.CheckRequestPerm(r, auth.PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if s.writeGate == nil {
		http.Error(w, "write gate is disabled", http.StatusNotFound)
		return
	}

	if r.Method == http.MethodPost {
		if !s.CheckRequestPerm(r, auth.PermExecute) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var req struct {
			Capacity   *int   `json:"capacity"`
			WaitLimit  *int   `json:"wait_limit"`
			FairShare  *int   `json:"fair_share"`
			MinBackoff string `json:"min_backoff"`
			MaxBackoff string `json:"max_backoff"`
		}
		if r.Body != nil {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, fmt.Sprintf("invalid request body: %s", err.Error()), http.StatusBadRequest)
				return
			}
		}
		if req.Capacity != nil {
			if *req.Capacity < 1 {
				http.Error(w, "capacity must be at least 1", http.StatusBadRequest)
				return
			}
			s.writeGate.SetCapacity(*req.Capacity)
		}
		if req.WaitLimit != nil {
			if *req.WaitLimit < 1 {
				http.Error(w, "wait_limit must be at least 1", http.StatusBadRequest)
				return
			}
			s.writeGate.SetWaitLimit(*req.WaitLimit)
		}
		if req.FairShare != nil {
			if *req.FairShare < 0 {
				http.Error(w, "fair_share must be zero (auto) or positive", http.StatusBadRequest)
				return
			}
			s.writeGate.SetFairShare(*req.FairShare)
		}
		if req.MinBackoff != "" {
			d, err := time.ParseDuration(req.MinBackoff)
			if err != nil || d <= 0 {
				http.Error(w, "min_backoff is not a valid duration", http.StatusBadRequest)
				return
			}
			s.writeGate.SetMinBackoff(d)
		}
		if req.MaxBackoff != "" {
			d, err := time.ParseDuration(req.MaxBackoff)
			if err != nil || d <= 0 {
				http.Error(w, "max_backoff is not a valid duration", http.StatusBadRequest)
				return
			}
			s.writeGate.SetMaxBackoff(d)
		}
	} else if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	b, err := json.Marshal(s.writeGate.Stats())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(b); err != nil {
		s.logger.Printf("failed to write write-gate status: %s", err.Error())
	}
}
