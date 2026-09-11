// Package domain contains the identity model and its framework-independent
// rules.
package domain

import (
	"strings"
	"time"
)

// AccountStatus controls whether a user may authenticate.
type AccountStatus string

const (
	AccountStatusActive   AccountStatus = "ACTIVE"
	AccountStatusDisabled AccountStatus = "DISABLED"
)

// UserAccount is the global login identity owned by the Control API.
type UserAccount struct {
	ID                 string
	Username           string
	NormalizedUsername string
	DisplayName        string
	Status             AccountStatus
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// CanLogin reports whether the account may create and use sessions.
func (a UserAccount) CanLogin() bool {
	return a.Status == AccountStatusActive
}

// NormalizeUsername produces the stable lookup form used by Identity.
func NormalizeUsername(username string) string {
	return strings.ToLower(strings.TrimSpace(username))
}

// PasswordCredential is the password material needed during authentication.
type PasswordCredential struct {
	EncodedHash           string
	MustChangeAtNextLogin bool
}

// LoginIdentity is the single read result required by password login.
type LoginIdentity struct {
	Account    UserAccount
	Credential PasswordCredential
}
