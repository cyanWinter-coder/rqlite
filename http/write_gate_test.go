package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/proxy"
)

// newGatedTestService starts a service whose write gate is configured with
// the given capacity and wait limit. The returned cleanup function closes it.
func newGatedTestService(t *testing.T, m *MockStore, cap, waitLimit int, waitTimeout time.Duration) *Service {
	t.Helper()
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.WriteGateCap = cap
	s.WriteGateWaitLimit = waitLimit
	s.WriteGateWaitTimeout = waitTimeout
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err.Error())
	}
	return s
}

func postExecute(t *testing.T, host, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(host+"/db/execute", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("execute request failed: %s", err.Error())
	}
	return resp
}

func Test_WriteGateDisabledByConfig(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.WriteGateCap = 0
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err.Error())
	}
	defer s.Close()

	if s.writeGate != nil {
		t.Fatalf("write gate should be disabled")
	}
	resp := postExecute(t, fmt.Sprintf("http://%s", s.Addr().String()), `["INSERT INTO foo VALUES(1)"]`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("disabled gate should pass writes through, got %d", resp.StatusCode)
	}
}

func Test_WriteGateRejectsWhenFullWithDepthAndBackoff(t *testing.T) {
	release := make(chan struct{})
	var executing atomic.Int32
	m := &MockStore{
		executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
			executing.Add(1)
			<-release
			return nil, 0, nil
		},
	}
	s := newGatedTestService(t, m, 1, 1, 50*time.Millisecond)
	defer func() {
		close(release)
		s.Close()
	}()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	// Occupy the single slot.
	go func() {
		resp := postExecute(t, host, `["INSERT INTO foo VALUES(1)"]`)
		resp.Body.Close()
	}()
	deadline := time.Now().Add(time.Second)
	for executing.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// The next request waits for the wait-limit slot; the one after that is
	// rejected immediately because the wait queue is full.
	waiterStarted := make(chan struct{})
	go func() {
		close(waiterStarted)
		resp := postExecute(t, host, `["INSERT INTO foo VALUES(2)"]`)
		resp.Body.Close()
	}()
	<-waiterStarted
	for s.writeGate.Waiting() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	resp := postExecute(t, host, `["INSERT INTO foo VALUES(3)"]`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got == "" {
		t.Fatalf("Retry-After header missing")
	}
	var rejected writeGateReject
	if err := json.NewDecoder(resp.Body).Decode(&rejected); err != nil {
		t.Fatalf("decode rejection body: %s", err.Error())
	}
	if rejected.Error == "" {
		t.Fatalf("rejection body missing error message")
	}
	if rejected.QueueDepth != 1 {
		t.Fatalf("queue_depth: exp 1, got %d", rejected.QueueDepth)
	}
	if rejected.RetryAfter <= 0 {
		t.Fatalf("retry_after_ms should be positive, got %d", rejected.RetryAfter)
	}
}

func Test_WriteGateWaitTimeoutReleasesAndFails(t *testing.T) {
	release := make(chan struct{})
	m := &MockStore{
		executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
			<-release
			return nil, 0, nil
		},
	}
	s := newGatedTestService(t, m, 1, 4, 80*time.Millisecond)
	defer func() {
		close(release)
		s.Close()
	}()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	occupy := make(chan struct{})
	go func() {
		resp := postExecute(t, host, `["INSERT INTO foo VALUES(1)"]`)
		resp.Body.Close()
		close(occupy)
	}()
	deadline := time.Now().Add(time.Second)
	for s.writeGate.Depth() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// This request can never get a slot within the wait timeout.
	start := time.Now()
	resp := postExecute(t, host, `["INSERT INTO foo VALUES(2)"]`)
	defer resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after wait timeout, got %d", resp.StatusCode)
	}
	if elapsed < 70*time.Millisecond {
		t.Fatalf("request returned before the wait timeout elapsed")
	}
	// The timed-out request no longer occupies a wait slot.
	if got := s.writeGate.Waiting(); got != 0 {
		t.Fatalf("timed-out request should be removed from wait queue, got %d", got)
	}
}

func Test_WriteGateDoesNotGateReads(t *testing.T) {
	block := make(chan struct{})
	m := &MockStore{
		executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
			<-block
			return nil, 0, nil
		},
	}
	s := newGatedTestService(t, m, 1, 1, time.Second)
	defer func() {
		close(block)
		s.Close()
	}()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	// Fill the gate with a write.
	go func() {
		resp := postExecute(t, host, `["INSERT INTO foo VALUES(1)"]`)
		resp.Body.Close()
	}()
	deadline := time.Now().Add(time.Second)
	for s.writeGate.Depth() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// A SELECT must bypass the gate and succeed immediately.
	resp, err := http.Post(host+"/db/query", "application/json",
		strings.NewReader(`["SELECT * FROM foo"]`))
	if err != nil {
		t.Fatalf("query request failed: %s", err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("read should bypass the write gate, got %d: %s", resp.StatusCode, string(body))
	}
}

func Test_WriteGateAdminEndpoint(t *testing.T) {
	m := &MockStore{}
	s := newGatedTestService(t, m, 2, 2, time.Second)
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	// GET reports current stats.
	resp, err := http.Get(host + "/write-gate")
	if err != nil {
		t.Fatalf("GET write-gate failed: %s", err.Error())
	}
	var stats map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats: %s", err.Error())
	}
	resp.Body.Close()
	if stats["capacity"].(float64) != 2 {
		t.Fatalf("capacity: exp 2, got %v", stats["capacity"])
	}

	// POST changes the capacity and the very next admission uses it.
	req, _ := http.NewRequest(http.MethodPost, host+"/write-gate",
		strings.NewReader(`{"capacity":5}`))
	postResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST write-gate failed: %s", err.Error())
	}
	postResp.Body.Close()
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", postResp.StatusCode)
	}
	if got := s.writeGate.Capacity(); got != 5 {
		t.Fatalf("capacity not applied immediately: exp 5, got %d", got)
	}
}

func Test_WriteGateInFlightWritesCompleteOnClose(t *testing.T) {
	release := make(chan struct{})
	var completed chan struct{}
	m := &MockStore{
		executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
			<-release
			close(completed)
			return nil, 0, nil
		},
	}
	s := newGatedTestService(t, m, 1, 1, time.Second)
	host := fmt.Sprintf("http://%s", s.Addr().String())

	completed = make(chan struct{})
	go func() {
		resp := postExecute(t, host, `["INSERT INTO foo VALUES(1)"]`)
		resp.Body.Close()
	}()
	deadline := time.Now().Add(time.Second)
	for s.writeGate.Depth() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatalf("Close returned while an in-flight write was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatalf("in-flight write did not run to completion")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatalf("Close did not return after in-flight write completed")
	}
}

func Test_WriteGateQueuedWriteIsBounded(t *testing.T) {
	release := make(chan struct{})
	m := &MockStore{
		executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
			<-release
			return nil, 0, nil
		},
	}
	s := newGatedTestService(t, m, 1, 1, time.Second)
	defer func() {
		close(release)
		s.Close()
	}()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	// A standard write fills the gate.
	go func() {
		resp := postExecute(t, host, `["INSERT INTO foo VALUES(1)"]`)
		resp.Body.Close()
	}()
	deadline := time.Now().Add(time.Second)
	for s.writeGate.Depth() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// First queued write takes the single wait slot.
	go func() {
		resp := postExecute(t, host+"/db/execute?queue&noleader", `["INSERT INTO foo VALUES(2)"]`)
		resp.Body.Close()
	}()
	for s.writeGate.Waiting() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// The second queued write is rejected by the gate, proving queued
	// requests share the same bound.
	resp := postExecute(t, host+"/db/execute?queue&noleader", `["INSERT INTO foo VALUES(3)"]`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("queued write should be bounded, got %d", resp.StatusCode)
	}
}

func Test_FirstSQLToken(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"SELECT 1", "select"},
		{"  select 1", "select"},
		{"-- comment\nSELECT 1", "select"},
		{"/* block */\n\tWITH x AS (SELECT 1) SELECT * FROM x", "with"},
		{"INSERT INTO foo VALUES(1)", "insert"},
		{"UPDATE foo SET id=2", "update"},
		{"EXPLAIN SELECT 1", "explain"},
		{"", ""},
	}
	for _, tc := range tests {
		if got := firstSQLToken(tc.sql); got != tc.want {
			t.Fatalf("firstSQLToken(%q): exp %q, got %q", tc.sql, tc.want, got)
		}
	}
}

func Test_ContainsWrite(t *testing.T) {
	if containsWrite([]*command.Statement{{Sql: "SELECT 1"}}) {
		t.Fatalf("SELECT should not be classified as a write")
	}
	if !containsWrite([]*command.Statement{
		{Sql: "SELECT 1"},
		{Sql: "INSERT INTO foo VALUES(1)"},
	}) {
		t.Fatalf("a batch containing an INSERT must be classified as a write")
	}
	if !containsWrite([]*command.Statement{{Sql: "SELECT 1", ForceQuery: true}}) {
		t.Fatalf("a ForceQuery (RETURNING) statement still mutates and must be gated")
	}
	if containsWrite(nil) {
		t.Fatalf("empty batch should not be classified as a write")
	}
}
