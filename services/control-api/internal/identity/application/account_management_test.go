package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

func TestAccountManagementCreatesManagedAccountWithTemporaryCredential(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	store := &accountStoreStub{}
	manager := application.NewAccountManagement(application.AccountManagementDependencies{
		Store: store,
		Passwords: &passwordHasherStub{
			hashedPassword: "temporary password",
			encodedHash:    "encoded-password",
		},
		NewID: func() (string, error) { return "user-1", nil },
		Now:   func() time.Time { return now },
	})

	account, err := manager.CreateManagedAccount(
		context.Background(),
		application.CreateManagedAccountCommand{
			Username:          " Alice ",
			DisplayName:       "Alice Example",
			TemporaryPassword: "temporary password",
		},
	)
	if err != nil {
		t.Fatalf("CreateManagedAccount() error = %v", err)
	}
	if account.ID != "user-1" || account.Username != "Alice" || account.NormalizedUsername != "alice" {
		t.Fatalf("account = %#v", account)
	}
	if store.createdCredential.EncodedHash != "encoded-password" ||
		!store.createdCredential.MustChangeAtNextLogin {
		t.Fatalf("credential = %#v", store.createdCredential)
	}
	if !store.createdAccount.CreatedAt.Equal(now) || !store.createdAccount.UpdatedAt.Equal(now) {
		t.Fatalf("account timestamps = %v/%v", store.createdAccount.CreatedAt, store.createdAccount.UpdatedAt)
	}
}

func TestAccountManagementValidatesUsernameAndTemporaryPassword(t *testing.T) {
	manager := application.NewAccountManagement(application.AccountManagementDependencies{
		Store:     &accountStoreStub{},
		Passwords: &passwordHasherStub{},
		NewID:     func() (string, error) { return "user-1", nil },
		Now:       time.Now,
	})

	tests := []struct {
		name    string
		command application.CreateManagedAccountCommand
		wantErr error
	}{
		{
			name: "invalid username",
			command: application.CreateManagedAccountCommand{
				Username: "a!", TemporaryPassword: "a sufficiently long password",
			},
			wantErr: application.ErrInvalidUsername,
		},
		{
			name: "short password",
			command: application.CreateManagedAccountCommand{
				Username: "alice", TemporaryPassword: "short",
			},
			wantErr: application.ErrWeakPassword,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := manager.CreateManagedAccount(context.Background(), tt.command)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

type accountStoreStub struct {
	createdAccount    domain.UserAccount
	createdCredential domain.PasswordCredential
	createErr         error
}

func (s *accountStoreStub) CreateAccount(
	_ context.Context,
	account domain.UserAccount,
	credential domain.PasswordCredential,
) error {
	s.createdAccount = account
	s.createdCredential = credential
	return s.createErr
}

func (s *accountStoreStub) GetAccount(context.Context, string) (domain.UserAccount, error) {
	return domain.UserAccount{}, application.ErrAccountNotFound
}

func (s *accountStoreStub) ListAccounts(context.Context, application.Page) (application.AccountPage, error) {
	return application.AccountPage{}, nil
}
