package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

func TestLoginWithPasswordCreatesServerSideSession(t *testing.T) {
	now := time.Date(2026, time.August, 31, 10, 0, 0, 0, time.UTC)
	accounts := &accountReaderStub{identity: domain.LoginIdentity{
		Account: domain.UserAccount{
			ID:                 "user-1",
			Username:           "Alice",
			NormalizedUsername: "alice",
			DisplayName:        "Alice",
			Status:             domain.AccountStatusActive,
		},
		Credential: domain.PasswordCredential{EncodedHash: "encoded-password"},
	}}
	sessions := &sessionWriterSpy{}

	login := application.NewLoginWithPassword(application.LoginDependencies{
		Accounts: accounts,
		Passwords: passwordVerifierStub{
			encodedHash: "encoded-password",
			password:    "correct horse battery staple",
			matches:     true,
		},
		Sessions: sessions,
		NewSessionToken: func() (application.GeneratedSessionToken, error) {
			return application.GeneratedSessionToken{
				ID:        "session-1",
				Plaintext: "session-token",
				Hash:      []byte("session-token-hash"),
			}, nil
		},
		Now:      func() time.Time { return now },
		Lifetime: 24 * time.Hour,
	})

	result, err := login.Execute(context.Background(), application.LoginCommand{
		Username: "  Alice  ",
		Password: "correct horse battery staple",
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if accounts.normalizedUsername != "alice" {
		t.Fatalf("account lookup = %q, want %q", accounts.normalizedUsername, "alice")
	}
	if result.SessionToken != "session-token" {
		t.Fatalf("SessionToken = %q, want %q", result.SessionToken, "session-token")
	}
	if result.User.ID != "user-1" || result.User.Username != "Alice" {
		t.Fatalf("User = %#v, want user-1/Alice", result.User)
	}
	if result.PasswordChangeRequired {
		t.Fatal("PasswordChangeRequired = true, want false")
	}
	if !result.ExpiresAt.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("ExpiresAt = %v, want %v", result.ExpiresAt, now.Add(24*time.Hour))
	}

	created := sessions.created
	if created.ID != "session-1" || created.UserID != "user-1" {
		t.Fatalf("created session = %#v, want session-1/user-1", created)
	}
	if string(created.TokenHash) != "session-token-hash" {
		t.Fatalf("created token hash = %q, want %q", created.TokenHash, "session-token-hash")
	}
	if created.Restricted {
		t.Fatal("created session Restricted = true, want false")
	}
}

func TestLoginWithTemporaryPasswordCreatesRestrictedSession(t *testing.T) {
	now := time.Date(2026, time.August, 31, 10, 0, 0, 0, time.UTC)
	sessions := &sessionWriterSpy{}
	login := application.NewLoginWithPassword(application.LoginDependencies{
		Accounts: &accountReaderStub{identity: domain.LoginIdentity{
			Account: domain.UserAccount{
				ID:                 "user-1",
				Username:           "alice",
				NormalizedUsername: "alice",
				Status:             domain.AccountStatusActive,
			},
			Credential: domain.PasswordCredential{
				EncodedHash:           "encoded-password",
				MustChangeAtNextLogin: true,
			},
		}},
		Passwords: passwordVerifierStub{matches: true},
		Sessions:  sessions,
		NewSessionToken: func() (application.GeneratedSessionToken, error) {
			return application.GeneratedSessionToken{
				ID:        "session-1",
				Plaintext: "session-token",
				Hash:      []byte("session-token-hash"),
			}, nil
		},
		Now:      func() time.Time { return now },
		Lifetime: time.Hour,
	})

	result, err := login.Execute(context.Background(), application.LoginCommand{
		Username: "alice",
		Password: "temporary password",
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !result.PasswordChangeRequired {
		t.Fatal("PasswordChangeRequired = false, want true")
	}
	if !sessions.created.Restricted {
		t.Fatal("created session Restricted = false, want true")
	}
}

func TestLoginWithPasswordDoesNotRevealCredentialFailure(t *testing.T) {
	tests := []struct {
		name      string
		accounts  *accountReaderStub
		passwords passwordVerifierStub
	}{
		{
			name:     "unknown username",
			accounts: &accountReaderStub{err: application.ErrLoginIdentityNotFound},
		},
		{
			name: "wrong password",
			accounts: &accountReaderStub{identity: domain.LoginIdentity{
				Account: domain.UserAccount{ID: "user-1", Status: domain.AccountStatusActive},
				Credential: domain.PasswordCredential{
					EncodedHash: "encoded-password",
				},
			}},
			passwords: passwordVerifierStub{matches: false},
		},
		{
			name: "disabled account",
			accounts: &accountReaderStub{identity: domain.LoginIdentity{
				Account: domain.UserAccount{ID: "user-1", Status: domain.AccountStatusDisabled},
				Credential: domain.PasswordCredential{
					EncodedHash: "encoded-password",
				},
			}},
			passwords: passwordVerifierStub{matches: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessions := &sessionWriterSpy{}
			login := application.NewLoginWithPassword(application.LoginDependencies{
				Accounts:  tt.accounts,
				Passwords: tt.passwords,
				Sessions:  sessions,
				Lifetime:  time.Hour,
				Now:       time.Now,
				NewSessionToken: func() (application.GeneratedSessionToken, error) {
					return application.GeneratedSessionToken{}, errors.New("token generation should not run")
				},
			})

			_, err := login.Execute(context.Background(), application.LoginCommand{
				Username: "alice",
				Password: "wrong password",
			})
			if !errors.Is(err, application.ErrInvalidCredentials) {
				t.Fatalf("Execute() error = %v, want ErrInvalidCredentials", err)
			}
			if sessions.createCalls != 0 {
				t.Fatalf("session create calls = %d, want 0", sessions.createCalls)
			}
		})
	}
}

type accountReaderStub struct {
	identity           domain.LoginIdentity
	err                error
	normalizedUsername string
}

func (s *accountReaderStub) FindLoginIdentity(
	_ context.Context,
	normalizedUsername string,
) (domain.LoginIdentity, error) {
	s.normalizedUsername = normalizedUsername
	return s.identity, s.err
}

type passwordVerifierStub struct {
	encodedHash string
	password    string
	matches     bool
	err         error
}

func (s passwordVerifierStub) Verify(encodedHash, password string) (bool, error) {
	if s.encodedHash != "" && encodedHash != s.encodedHash {
		return false, errors.New("unexpected encoded hash")
	}
	if s.password != "" && password != s.password {
		return false, errors.New("unexpected password")
	}
	return s.matches, s.err
}

type sessionWriterSpy struct {
	created     domain.Session
	createCalls int
	err         error
}

func (s *sessionWriterSpy) CreateSession(_ context.Context, session domain.Session) error {
	s.created = session
	s.createCalls++
	return s.err
}
