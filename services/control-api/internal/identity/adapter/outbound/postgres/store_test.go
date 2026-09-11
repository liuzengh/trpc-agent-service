package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	postgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

func TestStoreReadsOneLoginIdentity(t *testing.T) {
	db := &dbStub{row: rowStub{values: []any{
		"user-1",
		"Alice",
		"alice",
		"Alice Example",
		"ACTIVE",
		"encoded-password",
		true,
	}}}
	store := postgres.NewStore(db)

	identity, err := store.FindLoginIdentity(context.Background(), "alice")
	if err != nil {
		t.Fatalf("FindLoginIdentity() error = %v", err)
	}
	if db.queryArg != "alice" {
		t.Fatalf("query username = %q, want alice", db.queryArg)
	}
	if identity.Account.ID != "user-1" || identity.Account.Status != domain.AccountStatusActive {
		t.Fatalf("account = %#v", identity.Account)
	}
	if identity.Credential.EncodedHash != "encoded-password" || !identity.Credential.MustChangeAtNextLogin {
		t.Fatalf("credential = %#v", identity.Credential)
	}
}

func TestStoreMapsMissingLoginIdentity(t *testing.T) {
	store := postgres.NewStore(&dbStub{row: rowStub{err: pgx.ErrNoRows}})

	_, err := store.FindLoginIdentity(context.Background(), "unknown")
	if !errors.Is(err, application.ErrLoginIdentityNotFound) {
		t.Fatalf("FindLoginIdentity() error = %v, want ErrLoginIdentityNotFound", err)
	}
}

func TestStorePersistsOnlySessionHash(t *testing.T) {
	db := &dbStub{}
	store := postgres.NewStore(db)
	now := time.Date(2026, time.August, 31, 10, 0, 0, 0, time.UTC)

	err := store.CreateSession(context.Background(), domain.Session{
		ID:         "session-1",
		UserID:     "user-1",
		TokenHash:  []byte("token-hash"),
		Restricted: true,
		CreatedAt:  now,
		ExpiresAt:  now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	if db.execCalls != 1 {
		t.Fatalf("exec calls = %d, want 1", db.execCalls)
	}
	if got := db.execArgs[2]; string(got.([]byte)) != "token-hash" {
		t.Fatalf("persisted token hash = %q", got)
	}
}

func TestStoreReadsAuthenticatedSessionState(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	db := &dbStub{row: rowStub{values: []any{
		"session-1", "user-1", true, now.Add(-time.Minute), now.Add(time.Hour), (*time.Time)(nil),
		"alice", "Alice", "ACTIVE",
	}}}
	store := postgres.NewStore(db)

	state, err := store.FindSessionIdentity(context.Background(), []byte("token-hash"))
	if err != nil {
		t.Fatalf("FindSessionIdentity() error = %v", err)
	}
	if state.Session.ID != "session-1" || !state.Session.Restricted {
		t.Fatalf("session = %#v", state.Session)
	}
	if state.Account.ID != "user-1" || state.Account.Status != domain.AccountStatusActive {
		t.Fatalf("account = %#v", state.Account)
	}
}

func TestStoreCreatesAccountAndCredentialAtomically(t *testing.T) {
	db := &dbStub{}
	store := postgres.NewStore(db)
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)

	err := store.CreateAccount(context.Background(), domain.UserAccount{
		ID:                 "user-1",
		Username:           "Alice",
		NormalizedUsername: "alice",
		DisplayName:        "Alice",
		Status:             domain.AccountStatusActive,
		CreatedAt:          now,
		UpdatedAt:          now,
	}, domain.PasswordCredential{
		EncodedHash:           "encoded-password",
		MustChangeAtNextLogin: true,
	})
	if err != nil {
		t.Fatalf("CreateAccount() error = %v", err)
	}
	if db.execCalls != 1 || db.execArgs[0] != "user-1" {
		t.Fatalf("exec calls/args = %d/%#v", db.execCalls, db.execArgs)
	}
}

func TestStoreListsAccountsFromOneReadModel(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	encoded, err := json.Marshal([]map[string]any{{
		"id": "user-1", "username": "alice", "display_name": "Alice",
		"status": "ACTIVE", "created_at": now, "updated_at": now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	db := &dbStub{row: rowStub{values: []any{encoded, 1}}}
	store := postgres.NewStore(db)

	page, err := store.ListAccounts(context.Background(), application.Page{Offset: 0, Limit: 20})
	if err != nil {
		t.Fatalf("ListAccounts() error = %v", err)
	}
	if page.Total != 1 || len(page.Accounts) != 1 || page.Accounts[0].ID != "user-1" {
		t.Fatalf("page = %#v", page)
	}
}

func TestStoreChangesPasswordAndUnlocksCurrentSessionAtomically(t *testing.T) {
	db := &dbStub{row: rowStub{values: []any{"session-1"}}}
	store := postgres.NewStore(db)
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)

	err := store.ChangePassword(context.Background(), "user-1", "session-1", "new-hash", now)
	if err != nil {
		t.Fatalf("ChangePassword() error = %v", err)
	}
	if db.queryArgs[0] != "user-1" || db.queryArgs[1] != "session-1" || db.queryArgs[2] != "new-hash" {
		t.Fatalf("query args = %#v", db.queryArgs)
	}
}

type dbStub struct {
	row       pgx.Row
	queryArg  string
	queryArgs []any
	execCalls int
	execArgs  []any
	execErr   error
}

func (s *dbStub) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	s.queryArgs = append([]any(nil), args...)
	if len(args) > 0 {
		s.queryArg, _ = args[0].(string)
	}
	return s.row
}

func (s *dbStub) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	s.execCalls++
	s.execArgs = append([]any(nil), args...)
	return pgconn.NewCommandTag("INSERT 0 1"), s.execErr
}

type rowStub struct {
	values []any
	err    error
}

func (r rowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for index, value := range r.values {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *bool:
			*target = value.(bool)
		case *[]byte:
			*target = append((*target)[:0], value.([]byte)...)
		case *int:
			*target = value.(int)
		case *time.Time:
			*target = value.(time.Time)
		case **time.Time:
			*target = value.(*time.Time)
		default:
			panic("unsupported scan destination")
		}
	}
	return nil
}
