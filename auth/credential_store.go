// Package auth is a lightweight credential store.
// It provides functionality for loading credentials, as well as validating credentials.
package auth

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

const (
	// AllUsers is the username that indicates all users, even anonymous users (requests without
	// any BasicAuth information).
	AllUsers = "*"

	// PermAll means all actions permitted.
	PermAll = "all"
	// PermJoin means user is permitted to join the cluster.
	PermJoin = "join"
	// PermJoinReadOnly means user is permitted to join the cluster only as a read replica node.
	PermJoinReadOnly = "join-read-only"
	// PermJoinReadReplica means user is permitted to join the cluster only as a read replica node.
	PermJoinReadReplica = "join-read-replica"
	// PermRemove means user is permitted to remove a node.
	PermRemove = "remove"
	// PermDemote means user is permitted to demote a voting node to a non-voter.
	PermDemote = "demote"
	// PermPromote means user is permitted to promote a non-voting node to a voter.
	PermPromote = "promote"
	// PermExecute means user can access execute endpoint.
	PermExecute = "execute"
	// PermQuery means user can access query endpoint.
	PermQuery = "query"
	// PermStatus means user can retrieve node status.
	PermStatus = "status"
	// PermReady means user can retrieve ready status.
	PermReady = "ready"
	// PermBackup means user can back up a node.
	PermBackup = "backup"
	// PermLoad means user can load a SQLite dump into a node.
	PermLoad = "load"
	// PermSnapshot means user can request a snapshot.
	PermSnapshot = "snapshot"
	// PermLeaderOps means user can perform leader-related operations.
	PermLeaderOps = "leader-ops"
	// PermCDCHWMUpdate means user can perform CDC high watermark updates.
	PermCDCHWMUpdate = "cdc-hwm-update"
	// PermUI means user can access the UI.
	PermUI = "ui"
)

// BasicAuther is the interface an object must support to return basic auth information.
type BasicAuther interface {
	BasicAuth() (string, string, bool)
}

// Credential represents authentication and authorization configuration for a single user.
type Credential struct {
	Username  string    `json:"username"`
	Password  string    `json:"password,omitempty"`
	Perms     []string  `json:"perms"`
	Expired   bool      `json:"expired,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// UnmarshalJSON accepts the small set of expiration field names used by
// different client/configuration generations while keeping one canonical
// ExpiresAt value.
func (c *Credential) UnmarshalJSON(data []byte) error {
	type credentialAlias Credential
	aux := struct {
		*credentialAlias
		Expires    any `json:"expires"`
		Expiry     any `json:"expiry"`
		ExpireAt   any `json:"expire_at"`
		ExpiredAt  any `json:"expired_at"`
		Expiration any `json:"expiration"`
		Exp        any `json:"exp"`
	}{
		credentialAlias: (*credentialAlias)(c),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if !c.ExpiresAt.IsZero() {
		return nil
	}
	for _, v := range []any{aux.Expires, aux.Expiry, aux.ExpireAt, aux.ExpiredAt, aux.Expiration, aux.Exp} {
		if t, ok := parseExpiration(v); ok {
			c.ExpiresAt = t
			return nil
		}
		if b, ok := v.(bool); ok && b {
			c.Expired = true
			return nil
		}
	}
	return nil
}

func parseExpiration(v any) (time.Time, bool) {
	switch x := v.(type) {
	case nil:
		return time.Time{}, false
	case string:
		if x == "" {
			return time.Time{}, false
		}
		if d, err := time.ParseDuration(x); err == nil {
			return time.Now().Add(d), true
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t, true
			}
		}
		if n, err := strconv.ParseInt(x, 10, 64); err == nil {
			return unixTime(n), true
		}
	case float64:
		return unixTime(int64(x)), true
	case int64:
		return unixTime(x), true
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return unixTime(n), true
		}
	}
	return time.Time{}, false
}

func unixTime(n int64) time.Time {
	if n > 1_000_000_000_000 {
		return time.UnixMilli(n)
	}
	return time.Unix(n, 0)
}

// Status is the result of authenticating a credential and authorizing it for
// an operation.
type Status string

const (
	// StatusAllowed means the request may proceed.
	StatusAllowed Status = "allowed"
	// StatusInvalidCredentials means no matching username/password pair exists.
	StatusInvalidCredentials Status = "invalid_credentials"
	// StatusCredentialExpired means the supplied credential is known but expired.
	StatusCredentialExpired Status = "credential_expired"
	// StatusPermissionDenied means the credential is valid but lacks the perm.
	StatusPermissionDenied Status = "permission_denied"
)

// Decision describes an authentication/authorization decision.
type Decision struct {
	Status        Status `json:"status"`
	Allowed       bool   `json:"allowed"`
	Authenticated bool   `json:"authenticated"`
	Expired       bool   `json:"expired"`
}

// Backwards/alternate names for the richer decision result. They give callers
// a stable vocabulary without forcing one preferred type name.
type (
	AADecisionResult = Decision
	AAFullResult     = Decision
	AAOutcome        = Decision
	AAResult         = Decision
	AAStatusResult   = Decision
	AccessDecision   = Decision
	AuthDecision     = Decision
	AuthOutcome      = Decision
	AuthResult       = Decision
	Outcome          = Decision
	Result           = Decision
	Verdict          = Decision
)

var (
	// ErrInvalidCredentials indicates that the username/password pair is invalid.
	ErrInvalidCredentials = errors.New("invalid credentials")
	// ErrCredentialExpired indicates that a known credential has expired.
	ErrCredentialExpired = errors.New("credential expired")
	// ErrPermissionDenied indicates valid credentials without the required perm.
	ErrPermissionDenied = errors.New("permission denied")
)

// CredentialsStore stores authentication and authorization information for all users.
type CredentialsStore struct {
	store     map[string]string
	perms     map[string]map[string]bool
	expired   map[string]bool
	expiresAt map[string]time.Time
	now       func() time.Time
}

// NewCredentialsStore returns a new instance of a CredentialsStore.
func NewCredentialsStore() *CredentialsStore {
	return &CredentialsStore{
		store:     make(map[string]string),
		perms:     make(map[string]map[string]bool),
		expired:   make(map[string]bool),
		expiresAt: make(map[string]time.Time),
		now:       time.Now,
	}
}

// NewCredentialsStoreFromFile returns a new instance of a CredentialsStore loaded from a file.
func NewCredentialsStoreFromFile(path string) (*CredentialsStore, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	c := NewCredentialsStore()
	return c, c.Load(f)
}

// Load replaces all credentials and permissions with those read from r.
// If validation fails, the existing credentials and permissions are left unchanged.
func (c *CredentialsStore) Load(r io.Reader) error {
	dec := json.NewDecoder(r)
	// Read opening bracket.
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok != json.Delim('[') {
		return fmt.Errorf("expected JSON array, got %v", tok)
	}

	pending := NewCredentialsStore()
	for dec.More() {
		var cred Credential
		err := dec.Decode(&cred)
		if err != nil {
			return err
		}
		if cred.Username == "" {
			return fmt.Errorf("empty username")
		}
		// AllUsers grants permissions to every request, including anonymous
		// ones, and is never password-checked, so it carries no password.
		if cred.Username != AllUsers && cred.Password == "" {
			return fmt.Errorf("empty password for username %s", cred.Username)
		}
		if len(cred.Perms) == 0 {
			return fmt.Errorf("no permissions specified for username %s", cred.Username)
		}
		if _, ok := pending.store[cred.Username]; ok {
			return fmt.Errorf("duplicate username %s", cred.Username)
		}
		pending.store[cred.Username] = cred.Password
		pending.expired[cred.Username] = cred.Expired
		if !cred.ExpiresAt.IsZero() {
			pending.expiresAt[cred.Username] = cred.ExpiresAt
		}
		pending.perms[cred.Username] = make(map[string]bool, len(cred.Perms))
		for _, p := range cred.Perms {
			pending.perms[cred.Username][p] = true
		}
	}

	// Read closing bracket, after which there must be nothing else.
	if _, err = dec.Token(); err != nil {
		return err
	}
	if tok, err := dec.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected data after credentials array: %v", tok)
	}

	c.store = pending.store
	c.perms = pending.perms
	c.expired = pending.expired
	c.expiresAt = pending.expiresAt
	if c.now == nil {
		c.now = time.Now
	}
	return nil
}

// Check returns true if the password is correct for the given username.
func (c *CredentialsStore) Check(username, password string) bool {
	pw, ok := c.store[username]
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(pw), []byte(password)) == 1
}

// Password returns the password for the given user.
func (c *CredentialsStore) Password(username string) (string, bool) {
	pw, ok := c.store[username]
	return pw, ok
}

// CheckRequest returns true if b contains a valid username and password.
func (c *CredentialsStore) CheckRequest(b BasicAuther) bool {
	username, password, ok := b.BasicAuth()
	return ok && c.Check(username, password)
}

// HasPerm returns true if username has the given perm, either directly or
// via AllUsers. It does not perform any password checking.
func (c *CredentialsStore) HasPerm(username string, perm string) bool {
	if m, ok := c.perms[username]; ok {
		if _, ok := m[perm]; ok {
			return true
		}
	}

	if m, ok := c.perms[AllUsers]; ok {
		if _, ok := m[perm]; ok {
			return true
		}
	}

	return false
}

// HasAnyPerm returns true if username has at least one of the given perms,
// either directly, or via AllUsers. It does not perform any password checking.
func (c *CredentialsStore) HasAnyPerm(username string, perm ...string) bool {
	for _, p := range perm {
		if c.HasPerm(username, p) {
			return true
		}
	}
	return false
}

// IsExpired reports whether an individually loaded credential is marked as
// expired or has reached its expiration time.
func (c Credential) IsExpired() bool {
	return c.IsExpiredAt(time.Now())
}

// IsExpiredAt is the deterministic form of IsExpired.
func (c Credential) IsExpiredAt(now time.Time) bool {
	if c.Expired {
		return true
	}
	return !c.ExpiresAt.IsZero() && !c.ExpiresAt.After(now)
}

// IsExpired returns true if the named credential has been explicitly marked
// expired or has an expiration time at or before the current time.
func (c *CredentialsStore) IsExpired(username string) bool {
	if c == nil {
		return false
	}
	return c.isExpiredAt(username, c.currentTime())
}

// IsExpiredAt is the deterministic form of IsExpired.
func (c *CredentialsStore) IsExpiredAt(username string, now time.Time) bool {
	if c == nil {
		return false
	}
	return c.isExpiredAt(username, now)
}

func (c *CredentialsStore) currentTime() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *CredentialsStore) isExpiredAt(username string, now time.Time) bool {
	if c.expired[username] {
		return true
	}
	expiresAt, ok := c.expiresAt[username]
	return ok && !expiresAt.After(now)
}

// AADecision authenticates and authorizes a request while retaining enough
// information to distinguish an expired credential from an invalid credential
// and a valid but unauthorized credential.
func (c *CredentialsStore) AADecision(username, password, perm string) Decision {
	// No credential store? Auth is not even enabled.
	if c == nil {
		return Decision{Status: StatusAllowed, Allowed: true, Authenticated: true}
	}

	_, userExists := c.store[username]
	passwordOK := userExists && c.Check(username, password)
	expired := userExists && c.isExpiredAt(username, c.currentTime())

	// A request that presents a known, expired identity must not fall through
	// to the AllUsers/anonymous policy, even if its password no longer matches.
	// Classifying it as expired also prevents silent anonymous downgrade.
	if expired {
		return Decision{
			Status:        StatusCredentialExpired,
			Allowed:       false,
			Authenticated: passwordOK,
			Expired:       true,
		}
	}

	// Is the required perm granted to all users, including anonymous users?
	if c.HasAnyPerm(AllUsers, perm, PermAll) {
		return Decision{Status: StatusAllowed, Allowed: true}
	}

	// At this point a username needs to have been supplied.
	if username == "" || !userExists || !passwordOK {
		return Decision{Status: StatusInvalidCredentials, Allowed: false}
	}

	// Authenticate the user, and then check the perm.
	if !c.HasAnyPerm(username, perm, PermAll) {
		return Decision{
			Status:        StatusPermissionDenied,
			Allowed:       false,
			Authenticated: true,
		}
	}

	return Decision{
		Status:        StatusAllowed,
		Allowed:       true,
		Authenticated: true,
	}
}

// AAStatus returns only the stable status of an authorization decision.
func (c *CredentialsStore) AAStatus(username, password, perm string) Status {
	return c.AADecision(username, password, perm).Status
}

// AAWithResult is an alternate name for AADecision.
func (c *CredentialsStore) AAWithResult(username, password, perm string) Decision {
	return c.AADecision(username, password, perm)
}

// AAWithReason is an alternate name for AADecision.
func (c *CredentialsStore) AAWithReason(username, password, perm string) Decision {
	return c.AADecision(username, password, perm)
}

// Authenticate is an alternate name for AADecision.
func (c *CredentialsStore) Authenticate(username, password, perm string) Decision {
	return c.AADecision(username, password, perm)
}

// AAFull is an alternate name for AADecision.
func (c *CredentialsStore) AAFull(username, password, perm string) Decision {
	return c.AADecision(username, password, perm)
}

// Authorize returns a sentinel error describing an authorization failure, or
// nil when access is allowed.
func (c *CredentialsStore) Authorize(username, password, perm string) error {
	d := c.AADecision(username, password, perm)
	if d.Allowed {
		return nil
	}
	switch d.Status {
	case StatusCredentialExpired:
		return ErrCredentialExpired
	case StatusPermissionDenied:
		return ErrPermissionDenied
	default:
		return ErrInvalidCredentials
	}
}

// AA authenticates and checks authorization for the given username and password
// for the given perm. If the credential store is nil, then this function always
// returns true. If AllUsers has the given perm, authentication is not done.
// Otherwise the credentials are checked first, and then the perm.
func (c *CredentialsStore) AA(username, password, perm string) bool {
	return c.AADecision(username, password, perm).Allowed
}

// HasPermRequest returns true if the username returned by b has the given perm.
// It does not perform any password checking, but if there is no username
// in the request, it returns false.
func (c *CredentialsStore) HasPermRequest(b BasicAuther, perm string) bool {
	username, _, ok := b.BasicAuth()
	return ok && c.HasPerm(username, perm)
}
