package http

import (
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/auth"
	"github.com/rqlite/rqlite/v10/proxy"
)

func startClassificationTestService(t *testing.T, credentials CredentialStore) *Service {
	t.Helper()
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), credentials)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start test service: %v", err)
	}
	t.Cleanup(s.Close)
	ResetStats()
	return s
}

func decodeClassifiedBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON error body: %v", err)
	}
	return body
}

func TestRequestClassification_InvalidParams(t *testing.T) {
	s := startClassificationTestService(t, nil)
	resp, err := http.Get("http://" + s.Addr().String() + "/db/query?timeout=not-a-duration")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	body := decodeClassifiedBody(t, resp)
	if body["code"] != DecisionInvalidParams {
		t.Fatalf("expected code %q, got %v", DecisionInvalidParams, body["code"])
	}
	if body["request_type"] != "query" {
		t.Fatalf("expected query request type, got %v", body["request_type"])
	}
	if got := DefaultDecisionMetrics.Count("query", DecisionInvalidParams); got != 1 {
		t.Fatalf("expected one invalid_params query decision, got %d", got)
	}
	v, ok := stats.Get("query_invalid_params").(*expvar.Int)
	if !ok || v.Value() != 1 {
		t.Fatalf("expected expvar query_invalid_params=1, got %v (%T)", stats.Get("query_invalid_params"), stats.Get("query_invalid_params"))
	}
}

func TestRequestClassification_ExpiredCredentialNotAnonymous(t *testing.T) {
	store := auth.NewCredentialsStore()
	err := store.Load(strings.NewReader(`[
		{"username":"*","perms":["query"]},
		{"username":"old-client","password":"super-secret-value","perms":["query"],"expired":true}
	]`))
	if err != nil {
		t.Fatalf("failed to load credentials: %v", err)
	}
	s := startClassificationTestService(t, store)

	req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr().String()+"/db/query?q=SELECT+1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("old-client", "super-secret-value")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	body := decodeClassifiedBody(t, resp)
	if body["code"] != DecisionCredentialExpired {
		t.Fatalf("expected code %q, got %v; body=%v", DecisionCredentialExpired, body["code"], body)
	}
	if got := DefaultDecisionMetrics.Count("query", DecisionCredentialExpired); got != 1 {
		t.Fatalf("expected one expired query decision, got %d", got)
	}
}

func TestRequestClassification_PermissionDenied(t *testing.T) {
	store := auth.NewCredentialsStore()
	err := store.Load(strings.NewReader(`[
		{"username":"viewer","password":"super-secret-value","perms":["status"]}
	]`))
	if err != nil {
		t.Fatalf("failed to load credentials: %v", err)
	}
	s := startClassificationTestService(t, store)

	req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr().String()+"/db/query?q=SELECT+1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("viewer", "super-secret-value")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	body := decodeClassifiedBody(t, resp)
	if body["code"] != DecisionPermissionDenied {
		t.Fatalf("expected code %q, got %v", DecisionPermissionDenied, body["code"])
	}
	if got := DefaultDecisionMetrics.Count("query", DecisionPermissionDenied); got != 1 {
		t.Fatalf("expected one permission_denied query decision, got %d", got)
	}
}

func TestRequestClassification_LegacyQueryCombinationStillSucceeds(t *testing.T) {
	s := startClassificationTestService(t, nil)
	u := "http://" + s.Addr().String() +
		"/db/query?pretty&timings&associative&blob_array&level=weak&q=SELECT+1"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("legacy parameter combination was rejected: got %d", resp.StatusCode)
	}
}

func TestRequestClassification_LegacyParameterAliases(t *testing.T) {
	qp, err := NewQueryParams(httptest.NewRequest(http.MethodGet, "/db/query?byte_array&commit", nil))
	if err != nil {
		t.Fatal(err)
	}
	if !qp.BlobArray() || !qp.ByteArray() {
		t.Fatal("legacy byte_array parameter was not interpreted as BLOB array output")
	}
	if !qp.Sync() || !qp.Commit() {
		t.Fatal("legacy commit parameter was not interpreted as sync")
	}
}

func TestRequestClassification_ServiceUnavailableBody(t *testing.T) {
	ResetStats()
	req := httptest.NewRequest(http.MethodGet, "/db/query", nil)
	rr := httptest.NewRecorder()
	WriteClassifiedError(rr, req, DecisionServiceUnavailable)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body was not JSON: %v", err)
	}
	if body["code"] != DecisionServiceUnavailable {
		t.Fatalf("expected service_unavailable code, got %v", body["code"])
	}
	if body["error"] == "" {
		t.Fatal("service unavailable response must not have an empty error message")
	}
	if got := DefaultDecisionMetrics.Count("query", DecisionServiceUnavailable); got != 1 {
		t.Fatalf("expected one unavailable query decision, got %d", got)
	}
}
