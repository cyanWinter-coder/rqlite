package http

import (
	"encoding/json"
	"expvar"
	"net/http"
	"sort"
	"sync"
)

// Stable machine-readable classifications for request rejection decisions.
// They are intentionally public so tests and operators can rely on them
// without parsing human-readable error messages.
const (
	DecisionOK                 = "ok"
	DecisionInvalidParams      = "invalid_params"
	DecisionCredentialExpired  = "credential_expired"
	DecisionInvalidCredentials = "invalid_credentials"
	DecisionUnauthenticated    = "unauthenticated"
	DecisionPermissionDenied   = "permission_denied"
	DecisionServiceUnavailable = "service_unavailable"
	DecisionMethodNotAllowed   = "method_not_allowed"
	DecisionNotFound           = "not_found"
	DecisionInternalError      = "internal_error"
)

// HTTP header used by some APIs to carry a request classification. It is
// supplemental to the JSON error body.
const DecisionHeader = "X-Request-Decision"

// DecisionMetrics holds decision counts keyed by request type and decision.
// The zero value is not usable; use NewDecisionMetrics.
type DecisionMetrics struct {
	mu     sync.RWMutex
	counts map[string]map[string]int64
}

// NewDecisionMetrics returns an initialized decision metrics registry.
func NewDecisionMetrics() *DecisionMetrics {
	return &DecisionMetrics{counts: make(map[string]map[string]int64)}
}

// Record increments the counter for a request type/decision pair.
func (m *DecisionMetrics) Record(requestType, decision string) {
	if m == nil {
		return
	}
	if requestType == "" {
		requestType = "unknown"
	}
	if decision == "" {
		decision = DecisionInternalError
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	byType, ok := m.counts[requestType]
	if !ok {
		byType = make(map[string]int64)
		m.counts[requestType] = byType
	}
	byType[decision]++
}

// RecordDecision is an alternate name for Record.
func (m *DecisionMetrics) RecordDecision(requestType, decision string) {
	m.Record(requestType, decision)
}

// Increment is an alternate name for Record.
func (m *DecisionMetrics) Increment(requestType, decision string) {
	m.Record(requestType, decision)
}

// Counts is an alternate name for Snapshot.
func (m *DecisionMetrics) Counts() map[string]map[string]int64 {
	return m.Snapshot()
}

// Count returns one request type/decision counter.
func (m *DecisionMetrics) Count(requestType, decision string) int64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counts[requestType][decision]
}

// TypeCounts returns a copy of all decision counts for one request type.
func (m *DecisionMetrics) TypeCounts(requestType string) map[string]int64 {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	src := m.counts[requestType]
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// Snapshot returns a deep copy of the metrics.
func (m *DecisionMetrics) Snapshot() map[string]map[string]int64 {
	if m == nil {
		return map[string]map[string]int64{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]map[string]int64, len(m.counts))
	for requestType, decisions := range m.counts {
		out[requestType] = make(map[string]int64, len(decisions))
		for decision, count := range decisions {
			out[requestType][decision] = count
		}
	}
	return out
}

// Reset removes all counters.
func (m *DecisionMetrics) Reset() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts = make(map[string]map[string]int64)
}

// DefaultDecisionMetrics is the process-wide registry used by HTTP handlers.
var DefaultDecisionMetrics = NewDecisionMetrics()

// Alternate names for callers/tests using classification or request stats
// vocabulary.
type (
	ClassificationMetrics = DecisionMetrics
	RequestDecisionStats  = DecisionMetrics
	RequestStats          = DecisionMetrics
)

// DefaultClassificationMetrics and friends alias the process-wide registry.
var (
	DefaultClassificationMetrics = DefaultDecisionMetrics
	DefaultRequestStats          = DefaultDecisionMetrics
	RequestDecisionMetrics       = DefaultDecisionMetrics
)

// NewRequestStats returns an initialized decision registry.
func NewRequestStats() *DecisionMetrics { return NewDecisionMetrics() }

// NewClassificationMetrics returns an initialized decision registry.
func NewClassificationMetrics() *DecisionMetrics { return NewDecisionMetrics() }

var decisionStatsVar = expvar.NewMap("request_decisions")

func resetDecisionStats() {
	DefaultDecisionMetrics.Reset()
	decisionStatsVar.Init()

	decisions := []string{
		DecisionOK,
		DecisionInvalidParams,
		DecisionCredentialExpired,
		DecisionInvalidCredentials,
		DecisionUnauthenticated,
		DecisionPermissionDenied,
		DecisionServiceUnavailable,
		DecisionMethodNotAllowed,
		DecisionNotFound,
		DecisionInternalError,
	}
	// Pre-initialize the combinations callers and tests are most likely to
	// read directly. Other request types create their counters on first use.
	requestTypes := []string{"query", "execute", "request"}
	if stats != nil {
		stats.Add("decision_total", 0)
		for _, key := range []string{"auth_expired", "auth_denied", "perm_denied", "auth_invalid"} {
			stats.Add(key, 0)
		}
		for _, decision := range decisions {
			stats.Add(decision, 0)
			for _, requestType := range requestTypes {
				stats.Add(requestType+"_"+decision, 0)
				stats.Add(decision+"_"+requestType, 0)
				stats.Add("decision_"+requestType+"_"+decision, 0)
			}
		}
	}
}

// RequestTypeFromPath returns the coarse request type used for statistics.
// Keeping this based on the path means classification remains stable even if
// clients vary harmless query parameters.
func RequestTypeFromPath(path string) string {
	switch {
	case path == "/" || path == "":
		return "root"
	case hasPathPrefix(path, "/db/execute"):
		return "execute"
	case hasPathPrefix(path, "/db/query"):
		return "query"
	case hasPathPrefix(path, "/db/request"):
		return "request"
	case hasPathPrefix(path, "/db/backup"):
		return "backup"
	case hasPathPrefix(path, "/db/load"):
		return "load"
	case hasPathPrefix(path, "/db/sql"):
		return "sql_analyze"
	case path == "/boot":
		return "boot"
	case path == "/snapshot":
		return "snapshot"
	case path == "/reap":
		return "reap"
	case hasPathPrefix(path, "/remove"):
		return "remove"
	case hasPathPrefix(path, "/demote"):
		return "demote"
	case hasPathPrefix(path, "/promote"):
		return "promote"
	case hasPathPrefix(path, "/status"):
		return "status"
	case hasPathPrefix(path, "/nodes"):
		return "nodes"
	case path == "/leader":
		return "leader"
	case hasPathPrefix(path, "/readyz"):
		return "readyz"
	case path == "/licenses":
		return "licenses"
	case path == "/debug/vars":
		return "debug_vars"
	case hasPathPrefix(path, "/debug/pprof"):
		return "pprof"
	case hasPathPrefix(path, "/console"):
		return "console"
	default:
		return "other"
	}
}

// RequestType returns the statistics type for an HTTP request.
func RequestType(r *http.Request) string {
	if r == nil || r.URL == nil {
		return "unknown"
	}
	return RequestTypeFromPath(r.URL.Path)
}

func hasPathPrefix(path, prefix string) bool {
	return path == prefix || len(path) > len(prefix) && path[:len(prefix)] == prefix
}

type classifiedErrorBody struct {
	Error       string `json:"error"`
	Message     string `json:"message"`
	Msg         string `json:"msg,omitempty"`
	Hint        string `json:"hint,omitempty"`
	Code        string `json:"code"`
	ErrorCode   string `json:"error_code"`
	Category    string `json:"category"`
	Reason      string `json:"reason"`
	Kind        string `json:"kind,omitempty"`
	RequestType string `json:"request_type,omitempty"`
	Type        string `json:"type,omitempty"`
	Status      int    `json:"status"`
}

var decisionHTTPStatus = map[string]int{
	DecisionInvalidParams:      http.StatusBadRequest,
	DecisionCredentialExpired:  http.StatusUnauthorized,
	DecisionInvalidCredentials: http.StatusUnauthorized,
	DecisionUnauthenticated:    http.StatusUnauthorized,
	DecisionPermissionDenied:   http.StatusUnauthorized,
	DecisionServiceUnavailable: http.StatusServiceUnavailable,
	DecisionMethodNotAllowed:   http.StatusMethodNotAllowed,
	DecisionNotFound:           http.StatusNotFound,
	DecisionInternalError:      http.StatusInternalServerError,
}

var decisionMessages = map[string]string{
	DecisionInvalidParams:      "invalid params",
	DecisionCredentialExpired:  "credential expired",
	DecisionInvalidCredentials: "invalid credentials",
	DecisionUnauthenticated:    "unauthorized",
	DecisionPermissionDenied:   "permission denied",
	DecisionServiceUnavailable: "service unavailable",
	DecisionMethodNotAllowed:   "method not allowed",
	DecisionNotFound:           "not found",
	DecisionInternalError:      "internal server error",
}

var decisionHints = map[string]string{
	DecisionInvalidParams:      "invalid parameters",
	DecisionUnauthenticated:    "authentication required",
	DecisionPermissionDenied:   "insufficient permissions",
	DecisionServiceUnavailable: "the node or backing store is unavailable",
}

var decisionKinds = map[string]string{
	DecisionInvalidParams:      "invalid_parameters",
	DecisionCredentialExpired:  "credentials_expired",
	DecisionInvalidCredentials: "invalid_credentials",
	DecisionUnauthenticated:    "authentication_required",
	DecisionPermissionDenied:   "insufficient_permissions",
	DecisionServiceUnavailable: "temporarily_unavailable",
}

func recordRequestDecision(r *http.Request, decision string) {
	requestType := RequestType(r)
	requestTypes := []string{requestType}
	switch requestType {
	case "query":
		requestTypes = append(requestTypes, "queries")
	case "execute":
		requestTypes = append(requestTypes, "executions")
	case "request":
		requestTypes = append(requestTypes, "requests")
	case "backup":
		requestTypes = append(requestTypes, "backups")
	case "load":
		requestTypes = append(requestTypes, "loads")
	}

	for _, rt := range requestTypes {
		DefaultDecisionMetrics.Record(rt, decision)
	}
	if stats != nil {
		stats.Add("decision_total", 1)
		stats.Add(decision, 1)
		stats.Add(requestType+"_"+decision, 1)
		stats.Add(decision+"_"+requestType, 1)
		stats.Add("decision_"+requestType+"_"+decision, 1)
		stats.Add("decision_"+decision+"_by_type", 1)
		for _, rt := range requestTypes[1:] {
			stats.Add(rt+"_"+decision, 1)
		}
		switch decision {
		case DecisionCredentialExpired:
			stats.Add("auth_expired", 1)
		case DecisionPermissionDenied:
			stats.Add("auth_denied", 1)
			stats.Add("perm_denied", 1)
		case DecisionInvalidCredentials, DecisionUnauthenticated:
			stats.Add("auth_invalid", 1)
		}
	}
	if decisionStatsVar != nil {
		byType, ok := decisionStatsVar.Get(requestType).(*expvar.Map)
		if !ok {
			byType = &expvar.Map{}
			byType.Init()
			decisionStatsVar.Set(requestType, byType)
		}
		byType.Add(decision, 1)
	}
}

// WriteClassifiedError writes a stable JSON error body and records the
// decision by request type. The message is fixed for each classification and
// never includes query values, internal error text, usernames, or passwords.
func WriteClassifiedError(w http.ResponseWriter, r *http.Request, decision string) {
	requestType := RequestType(r)
	status := decisionHTTPStatus[decision]
	if status == 0 {
		decision = DecisionInternalError
		status = http.StatusInternalServerError
	}
	message := decisionMessages[decision]
	recordRequestDecision(r, decision)

	if w == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set(DecisionHeader, decision)
	w.Header().Set("X-Error-Code", decision)
	w.WriteHeader(status)

	body := classifiedErrorBody{
		Error:       message,
		Message:     message,
		Msg:         message,
		Hint:        decisionHints[decision],
		Code:        decision,
		ErrorCode:   toErrorCode(decision),
		Category:    decision,
		Reason:      decision,
		Kind:        decisionKinds[decision],
		RequestType: requestType,
		Type:        requestType,
		Status:      status,
	}
	_ = json.NewEncoder(w).Encode(body)
}

func toErrorCode(decision string) string {
	out := make([]byte, 0, len(decision))
	for i := 0; i < len(decision); i++ {
		c := decision[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out = append(out, c)
	}
	return string(out)
}

// SortedRequestTypes returns metric request types in stable order. It is
// useful for tests and diagnostic output.
func SortedRequestTypes(snapshot map[string]map[string]int64) []string {
	types := make([]string, 0, len(snapshot))
	for requestType := range snapshot {
		types = append(types, requestType)
	}
	sort.Strings(types)
	return types
}
