// Package domain owns non-secret stateful account projections and local lease
// grants. It contains no provider SDK, credential value, database or transport.
package domain

import (
	"errors"
	"regexp"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	ErrHeld          = errors.New("connection lease is held")
	ErrLost          = errors.New("connection lease is lost")
	ErrDisabled      = errors.New("connection account is disabled")
	ErrStaleRevision = errors.New("connection account revision is stale")
	ErrConflict      = errors.New("connection account identity or revision conflicts")
	ErrInvalid       = errors.New("invalid connection account or lease input")
	ErrReplaced      = errors.New("connection account revision is blocked after replacement")
)

const (
	MinLeaseTTL = 100 * time.Millisecond
	MaxLeaseTTL = 10 * time.Minute
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var credentialReference = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._:/-]*$`)

type Account struct {
	ID            string
	BotID         string
	CredentialRef string
	Revision      int64
	Enabled       bool
}

func (a Account) Validate() error {
	if !identifier.MatchString(a.ID) || !validBotID(a.BotID) || len(a.CredentialRef) > 2048 || !credentialReference.MatchString(a.CredentialRef) || a.Revision < 1 {
		return ErrInvalid
	}
	return nil
}

type OwnerGrant struct {
	AccountID  string
	BotID      string
	InstanceID string
	Epoch      int64
	Revision   int64
	LeaseUntil time.Time
	ObservedAt time.Time
}

// Validate checks identity only. Caller-supplied grant timestamps never decide
// authorization; the Store checks the current database row and database clock.
func (g OwnerGrant) Validate() error {
	if !validBotID(g.BotID) {
		return ErrInvalid
	}
	return ValidateOwnerIdentity(g.AccountID, g.InstanceID, g.Epoch, g.Revision)
}

func ValidateOwnerIdentity(accountID, instanceID string, epoch, revision int64) error {
	if !identifier.MatchString(accountID) || !identifier.MatchString(instanceID) || epoch < 1 || revision < 1 {
		return ErrInvalid
	}
	return nil
}

func ValidateInstanceID(id string) error {
	if !identifier.MatchString(id) {
		return ErrInvalid
	}
	return nil
}

func ValidateLeaseTTL(ttl time.Duration) error {
	if ttl < MinLeaseTTL || ttl > MaxLeaseTTL {
		return ErrInvalid
	}
	return nil
}

func validBotID(id string) bool {
	if id == "" || len(id) > 1024 || !utf8.ValidString(id) {
		return false
	}
	for _, r := range id {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}
