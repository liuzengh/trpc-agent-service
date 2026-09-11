package application

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

var (
	ErrUnauthenticated        = errors.New("unauthenticated")
	ErrSessionNotFound        = errors.New("session not found")
	ErrInvalidCurrentPassword = errors.New("current password is invalid")
)

type SessionIdentityReader interface {
	FindSessionIdentity(context.Context, []byte) (domain.SessionIdentity, error)
}

type PasswordChangeStore interface {
	FindPasswordCredential(context.Context, string) (domain.PasswordCredential, error)
	ChangePassword(context.Context, string, string, string, time.Time) error
}

type SessionRevoker interface {
	RevokeSession(context.Context, string, string, time.Time) error
}

// IdentityContext is the only trusted user/session state propagated to other
// modules. It carries no Admin or Tenant authorization.
type IdentityContext struct {
	UserID      string
	SessionID   string
	Username    string
	DisplayName string
	Restricted  bool
}

type identityContextKey struct{}

func WithIdentity(ctx context.Context, identity IdentityContext) context.Context {
	return context.WithValue(ctx, identityContextKey{}, identity)
}

func IdentityFromContext(ctx context.Context) (IdentityContext, bool) {
	identity, ok := ctx.Value(identityContextKey{}).(IdentityContext)
	return identity, ok
}

type AuthenticateSession struct {
	sessions SessionIdentityReader
	now      func() time.Time
}

func NewAuthenticateSession(sessions SessionIdentityReader, now func() time.Time) *AuthenticateSession {
	if now == nil {
		now = time.Now
	}
	return &AuthenticateSession{sessions: sessions, now: now}
}

func (a *AuthenticateSession) Execute(ctx context.Context, token string) (IdentityContext, error) {
	if a == nil || a.sessions == nil || token == "" {
		return IdentityContext{}, ErrUnauthenticated
	}
	hash := sha256.Sum256([]byte(token))
	state, err := a.sessions.FindSessionIdentity(ctx, hash[:])
	if errors.Is(err, ErrSessionNotFound) {
		return IdentityContext{}, ErrUnauthenticated
	}
	if err != nil {
		return IdentityContext{}, fmt.Errorf("find session identity: %w", err)
	}
	now := a.now().UTC()
	if state.Session.RevokedAt != nil || !now.Before(state.Session.ExpiresAt) ||
		!state.Account.CanLogin() {
		return IdentityContext{}, ErrUnauthenticated
	}
	return IdentityContext{
		UserID:      state.Account.ID,
		SessionID:   state.Session.ID,
		Username:    state.Account.Username,
		DisplayName: state.Account.DisplayName,
		Restricted:  state.Session.Restricted,
	}, nil
}

type LogoutSession struct {
	sessions SessionRevoker
	now      func() time.Time
}

func NewLogoutSession(sessions SessionRevoker, now func() time.Time) *LogoutSession {
	if now == nil {
		now = time.Now
	}
	return &LogoutSession{sessions: sessions, now: now}
}

func (l *LogoutSession) Execute(ctx context.Context, identity IdentityContext) error {
	if l == nil || l.sessions == nil || identity.UserID == "" || identity.SessionID == "" {
		return ErrUnauthenticated
	}
	if err := l.sessions.RevokeSession(
		ctx, identity.UserID, identity.SessionID, l.now().UTC(),
	); err != nil {
		return fmt.Errorf("revoke current session: %w", err)
	}
	return nil
}

type ChangePassword struct {
	store     PasswordChangeStore
	passwords PasswordHasher
	now       func() time.Time
}

func NewChangePassword(
	store PasswordChangeStore,
	passwords PasswordHasher,
	now func() time.Time,
) *ChangePassword {
	if now == nil {
		now = time.Now
	}
	return &ChangePassword{store: store, passwords: passwords, now: now}
}

type ChangePasswordCommand struct {
	Identity        IdentityContext
	CurrentPassword string
	NewPassword     string
}

func (c *ChangePassword) Execute(ctx context.Context, command ChangePasswordCommand) error {
	if c == nil || c.store == nil || c.passwords == nil ||
		command.Identity.UserID == "" || command.Identity.SessionID == "" {
		return ErrUnauthenticated
	}
	if err := ValidatePassword(command.NewPassword); err != nil {
		return err
	}
	credential, err := c.store.FindPasswordCredential(ctx, command.Identity.UserID)
	if err != nil {
		return fmt.Errorf("find password credential: %w", err)
	}
	matches, err := c.passwords.Verify(credential.EncodedHash, command.CurrentPassword)
	if err != nil {
		return fmt.Errorf("verify current password: %w", err)
	}
	if !matches {
		return ErrInvalidCurrentPassword
	}
	encodedHash, err := c.passwords.Hash(command.NewPassword)
	if err != nil {
		return fmt.Errorf("hash new password: %w", err)
	}
	if err := c.store.ChangePassword(
		ctx,
		command.Identity.UserID,
		command.Identity.SessionID,
		encodedHash,
		c.now().UTC(),
	); err != nil {
		return fmt.Errorf("change password: %w", err)
	}
	return nil
}
