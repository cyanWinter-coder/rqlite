package auth

import (
	"strings"
	"testing"
	"time"
)

func TestCredentialStore_ExpiredCredentialDeniedBeforeAllUsers(t *testing.T) {
	store := NewCredentialsStore()
	err := store.Load(strings.NewReader(`[
		{"username":"*","perms":["query"]},
		{"username":"old","password":"secret","perms":["query"],"expires_at":"2000-01-01T00:00:00Z"}
	]`))
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	if got := store.AAStatus("old", "secret", PermQuery); got != StatusCredentialExpired {
		t.Fatalf("expected expired status, got %q", got)
	}
	if store.AA("old", "secret", PermQuery) {
		t.Fatal("expired credential must not be downgraded to AllUsers/anonymous access")
	}
	if !store.AA("", "", PermQuery) {
		t.Fatal("anonymous AllUsers access should remain available when no credential is presented")
	}
}

func TestCredentialStore_ExpirationAliases(t *testing.T) {
	for _, raw := range []string{
		`{"username":"a","password":"p","perms":["query"],"expiry":"2000-01-01T00:00:00Z"}`,
		`{"username":"b","password":"p","perms":["query"],"exp":946684800}`,
		`{"username":"c","password":"p","perms":["query"],"expired":true}`,
	} {
		store := NewCredentialsStore()
		if err := store.Load(strings.NewReader("[" + raw + "]")); err != nil {
			t.Fatalf("failed to load %s: %v", raw, err)
		}
		username := strings.Split(raw, `"username":"`)[1]
		username = strings.Split(username, `"`)[0]
		if !store.IsExpired(username) {
			t.Fatalf("expected %s to be expired", username)
		}
	}

	future := Credential{ExpiresAt: time.Now().Add(time.Hour)}
	if future.IsExpired() {
		t.Fatal("future expiration should not be expired")
	}
}
