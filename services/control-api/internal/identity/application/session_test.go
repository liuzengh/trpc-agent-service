package application_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

func TestAuthenticateSessionBuildsIdentityContext(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	store := &sessionStoreStub{sessionIdentity: domain.SessionIdentity{
		Session: domain.Session{
			ID:         "session-1",
			UserID:     "user-1",
			Restricted: true,
			ExpiresAt:  now.Add(time.Hour),
		},
		Account: domain.UserAccount{
			ID:          "user-1",
			Username:    "alice",
			DisplayName: "Alice",
			Status:      domain.AccountStatusActive,
		},
	}}
	authenticate := application.NewAuthenticateSession(store, func() time.Time { return now })

	identity, err := authenticate.Execute(context.Background(), "opaque-token")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	wantHash := sha256.Sum256([]byte("opaque-token"))
	if string(store.tokenHash) != string(wantHash[:]) {
		t.Fatal("session lookup did not hash the opaque token")
	}
	if identity.UserID != "user-1" || identity.SessionID != "session-1" || !identity.Restricted {
		t.Fatalf("identity = %#v", identity)
	}
}

func TestAuthenticateSessionRejectsUnusableState(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	revokedAt := now.Add(-time.Minute)
	tests := []struct {
		name  string
		state domain.SessionIdentity
		err   error
	}{
		{name: "unknown session", err: application.ErrSessionNotFound},
		{
			name: "expired session",
			state: domain.SessionIdentity{
				Session: domain.Session{ExpiresAt: now},
				Account: domain.UserAccount{Status: domain.AccountStatusActive},
			},
		},
		{
			name: "revoked session",
			state: domain.SessionIdentity{
				Session: domain.Session{ExpiresAt: now.Add(time.Hour), RevokedAt: &revokedAt},
				Account: domain.UserAccount{Status: domain.AccountStatusActive},
			},
		},
		{
			name: "disabled account",
			state: domain.SessionIdentity{
				Session: domain.Session{ExpiresAt: now.Add(time.Hour)},
				Account: domain.UserAccount{Status: domain.AccountStatusDisabled},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authenticate := application.NewAuthenticateSession(
				&sessionStoreStub{sessionIdentity: tt.state, findErr: tt.err},
				func() time.Time { return now },
			)
			_, err := authenticate.Execute(context.Background(), "token")
			if !errors.Is(err, application.ErrUnauthenticated) {
				t.Fatalf("Execute() error = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

func TestChangePasswordReplacesCredentialAndUnlocksCurrentSession(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	store := &sessionStoreStub{credential: domain.PasswordCredential{EncodedHash: "old-hash"}}
	hasher := &passwordHasherStub{
		verifyHash:     "old-hash",
		verifyPassword: "temporary password",
		verifyMatches:  true,
		hashedPassword: "a new strong password",
		encodedHash:    "new-hash",
	}
	change := application.NewChangePassword(store, hasher, func() time.Time { return now })

	err := change.Execute(context.Background(), application.ChangePasswordCommand{
		Identity:        application.IdentityContext{UserID: "user-1", SessionID: "session-1", Restricted: true},
		CurrentPassword: "temporary password",
		NewPassword:     "a new strong password",
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if store.changedUserID != "user-1" || store.changedSessionID != "session-1" {
		t.Fatalf("change target = %s/%s", store.changedUserID, store.changedSessionID)
	}
	if store.changedHash != "new-hash" || !store.changedAt.Equal(now) {
		t.Fatalf("change = hash %q at %v", store.changedHash, store.changedAt)
	}
}

func TestChangePasswordRejectsWrongCurrentPassword(t *testing.T) {
	store := &sessionStoreStub{credential: domain.PasswordCredential{EncodedHash: "old-hash"}}
	change := application.NewChangePassword(
		store,
		&passwordHasherStub{verifyMatches: false},
		time.Now,
	)

	err := change.Execute(context.Background(), application.ChangePasswordCommand{
		Identity:        application.IdentityContext{UserID: "user-1", SessionID: "session-1"},
		CurrentPassword: "wrong",
		NewPassword:     "a new strong password",
	})
	if !errors.Is(err, application.ErrInvalidCurrentPassword) {
		t.Fatalf("Execute() error = %v, want ErrInvalidCurrentPassword", err)
	}
	if store.changeCalls != 0 {
		t.Fatalf("change calls = %d, want 0", store.changeCalls)
	}
}

func TestLogoutRevokesCurrentSession(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	store := &sessionStoreStub{}
	logout := application.NewLogoutSession(store, func() time.Time { return now })

	if err := logout.Execute(context.Background(), application.IdentityContext{
		UserID: "user-1", SessionID: "session-1",
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if store.revokedUserID != "user-1" || store.revokedSessionID != "session-1" || !store.revokedAt.Equal(now) {
		t.Fatalf("revoke = %s/%s at %v", store.revokedUserID, store.revokedSessionID, store.revokedAt)
	}
}

type sessionStoreStub struct {
	sessionIdentity domain.SessionIdentity
	findErr         error
	tokenHash       []byte
	credential      domain.PasswordCredential
	credentialErr   error

	changedUserID    string
	changedSessionID string
	changedHash      string
	changedAt        time.Time
	changeCalls      int

	revokedUserID    string
	revokedSessionID string
	revokedAt        time.Time
}

func (s *sessionStoreStub) FindSessionIdentity(
	_ context.Context,
	tokenHash []byte,
) (domain.SessionIdentity, error) {
	s.tokenHash = append([]byte(nil), tokenHash...)
	return s.sessionIdentity, s.findErr
}

func (s *sessionStoreStub) FindPasswordCredential(
	context.Context,
	string,
) (domain.PasswordCredential, error) {
	return s.credential, s.credentialErr
}

func (s *sessionStoreStub) ChangePassword(
	_ context.Context,
	userID, currentSessionID, encodedHash string,
	changedAt time.Time,
) error {
	s.changedUserID = userID
	s.changedSessionID = currentSessionID
	s.changedHash = encodedHash
	s.changedAt = changedAt
	s.changeCalls++
	return nil
}

func (s *sessionStoreStub) RevokeSession(
	_ context.Context,
	userID, sessionID string,
	revokedAt time.Time,
) error {
	s.revokedUserID = userID
	s.revokedSessionID = sessionID
	s.revokedAt = revokedAt
	return nil
}

type passwordHasherStub struct {
	verifyHash     string
	verifyPassword string
	verifyMatches  bool
	verifyErr      error
	hashedPassword string
	encodedHash    string
	hashErr        error
}

func (s *passwordHasherStub) Verify(encodedHash, password string) (bool, error) {
	if s.verifyHash != "" && encodedHash != s.verifyHash {
		return false, errors.New("unexpected verify hash")
	}
	if s.verifyPassword != "" && password != s.verifyPassword {
		return false, errors.New("unexpected verify password")
	}
	return s.verifyMatches, s.verifyErr
}

func (s *passwordHasherStub) Hash(password string) (string, error) {
	if s.hashedPassword != "" && password != s.hashedPassword {
		return "", errors.New("unexpected hash password")
	}
	return s.encodedHash, s.hashErr
}
