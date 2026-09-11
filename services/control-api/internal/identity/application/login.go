// Package application implements Identity use cases and declares only the
// external capabilities those use cases consume.
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

var (
	// ErrInvalidCredentials deliberately covers unknown users, wrong passwords,
	// and disabled accounts so callers do not reveal account state.
	ErrInvalidCredentials = errors.New("invalid credentials")
	// ErrLoginIdentityNotFound is returned by AccountReader and is translated to
	// ErrInvalidCredentials at the LoginWithPassword interface.
	ErrLoginIdentityNotFound = errors.New("login identity not found")
)

// AccountReader returns the exact account and credential view required for a
// password login.
type AccountReader interface {
	FindLoginIdentity(ctx context.Context, normalizedUsername string) (domain.LoginIdentity, error)
}

// PasswordVerifier verifies the encoded password format owned by its adapter.
type PasswordVerifier interface {
	Verify(encodedHash, password string) (bool, error)
}

// SessionWriter persists a new server-side session.
type SessionWriter interface {
	CreateSession(ctx context.Context, session domain.Session) error
}

// GeneratedSessionToken keeps plaintext transport material separate from the
// hash written to persistence.
type GeneratedSessionToken struct {
	ID        string
	Plaintext string
	Hash      []byte
}

// SessionTokenGenerator creates an opaque, high-entropy session token.
type SessionTokenGenerator func() (GeneratedSessionToken, error)

// LoginDependencies contains the three outward seams used by password login.
type LoginDependencies struct {
	Accounts         AccountReader
	Passwords        PasswordVerifier
	Sessions         SessionWriter
	NewSessionToken  SessionTokenGenerator
	Now              func() time.Time
	Lifetime         time.Duration
	DummyEncodedHash string
}

// LoginWithPassword authenticates one local account and creates one session.
type LoginWithPassword struct {
	deps LoginDependencies
}

// NewLoginWithPassword constructs the password-login use case.
func NewLoginWithPassword(deps LoginDependencies) *LoginWithPassword {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &LoginWithPassword{deps: deps}
}

// LoginCommand is the protocol-independent password login input.
type LoginCommand struct {
	Username string
	Password string
}

// LoggedInUser is the minimal account view returned after authentication.
type LoggedInUser struct {
	ID          string
	Username    string
	DisplayName string
}

// LoginResult carries the one-time token and the session/account state needed
// by the inbound adapter.
type LoginResult struct {
	SessionToken           string
	ExpiresAt              time.Time
	PasswordChangeRequired bool
	User                   LoggedInUser
}

// Execute authenticates a local username/password pair.
func (uc *LoginWithPassword) Execute(ctx context.Context, command LoginCommand) (LoginResult, error) {
	if err := uc.validateDependencies(); err != nil {
		return LoginResult{}, err
	}

	normalizedUsername := domain.NormalizeUsername(command.Username)
	identity, err := uc.deps.Accounts.FindLoginIdentity(ctx, normalizedUsername)
	if err != nil {
		if errors.Is(err, ErrLoginIdentityNotFound) {
			// Verify a fixed encoded hash to reduce the timing difference between
			// unknown usernames and wrong passwords.
			_, _ = uc.deps.Passwords.Verify(uc.deps.DummyEncodedHash, command.Password)
			return LoginResult{}, ErrInvalidCredentials
		}
		return LoginResult{}, fmt.Errorf("find login identity: %w", err)
	}

	matches, err := uc.deps.Passwords.Verify(identity.Credential.EncodedHash, command.Password)
	if err != nil {
		return LoginResult{}, fmt.Errorf("verify password: %w", err)
	}
	if !matches || !identity.Account.CanLogin() {
		return LoginResult{}, ErrInvalidCredentials
	}

	token, err := uc.deps.NewSessionToken()
	if err != nil {
		return LoginResult{}, fmt.Errorf("generate session token: %w", err)
	}
	now := uc.deps.Now().UTC()
	expiresAt := now.Add(uc.deps.Lifetime)
	session := domain.Session{
		ID:         token.ID,
		UserID:     identity.Account.ID,
		TokenHash:  append([]byte(nil), token.Hash...),
		Restricted: identity.Credential.MustChangeAtNextLogin,
		CreatedAt:  now,
		ExpiresAt:  expiresAt,
	}
	if err := uc.deps.Sessions.CreateSession(ctx, session); err != nil {
		return LoginResult{}, fmt.Errorf("create session: %w", err)
	}

	return LoginResult{
		SessionToken:           token.Plaintext,
		ExpiresAt:              expiresAt,
		PasswordChangeRequired: identity.Credential.MustChangeAtNextLogin,
		User: LoggedInUser{
			ID:          identity.Account.ID,
			Username:    identity.Account.Username,
			DisplayName: identity.Account.DisplayName,
		},
	}, nil
}

func (uc *LoginWithPassword) validateDependencies() error {
	if uc == nil || uc.deps.Accounts == nil || uc.deps.Passwords == nil ||
		uc.deps.Sessions == nil || uc.deps.NewSessionToken == nil {
		return errors.New("login with password: incomplete dependencies")
	}
	if uc.deps.Lifetime <= 0 {
		return errors.New("login with password: session lifetime must be positive")
	}
	return nil
}
