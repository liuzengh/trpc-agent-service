package identity

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLoginTransactionCarriesProviderAndPKCE(t *testing.T) {
	transaction, challenge, err := NewLoginTransaction("corp-sso")
	if err != nil {
		t.Fatalf("NewLoginTransaction() error = %v", err)
	}
	if transaction.ProviderID != "corp-sso" || transaction.State == "" || transaction.Nonce == "" || transaction.PKCEVerifier == "" || challenge == "" {
		t.Fatalf("transaction = %+v, challenge=%q", transaction, challenge)
	}
	if challenge == transaction.PKCEVerifier {
		t.Fatal("PKCE challenge must not expose the verifier")
	}
}

func TestMemorySessionStoreCreateGet(t *testing.T) {
	store := NewMemorySessionStore()
	sessionID, err := store.Create(context.Background(), SessionUser{PlatformUserID: "platform-u1", Role: RoleAdmin, IsSystemAdmin: true}, time.Hour)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if sessionID == "" {
		t.Fatal("Create() returned empty session ID")
	}
	got, err := store.Get(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.PlatformUserID != "platform-u1" || got.Role != RoleAdmin || !got.IsSystemAdmin {
		t.Fatalf("Get() = %+v, want the created user", got)
	}
}

func TestMemorySessionStoreUniqueIDs(t *testing.T) {
	store := NewMemorySessionStore()
	first, _ := store.Create(context.Background(), SessionUser{PlatformUserID: "platform-u1"}, time.Hour)
	second, _ := store.Create(context.Background(), SessionUser{PlatformUserID: "platform-u2"}, time.Hour)
	if first == second {
		t.Fatal("Create() returned the same session ID twice")
	}
}

func TestMemorySessionStoreUnknownSession(t *testing.T) {
	store := NewMemorySessionStore()
	if _, err := store.Get(context.Background(), "missing"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrSessionNotFound", err)
	}
}

func TestMemorySessionStoreDelete(t *testing.T) {
	store := NewMemorySessionStore()
	sessionID, _ := store.Create(context.Background(), SessionUser{PlatformUserID: "platform-u1"}, time.Hour)
	if err := store.Delete(context.Background(), sessionID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Get(context.Background(), sessionID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Get(after delete) error = %v, want ErrSessionNotFound", err)
	}
}

func TestMemorySessionStoreExpiry(t *testing.T) {
	store := NewMemorySessionStore()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	sessionID, err := store.Create(context.Background(), SessionUser{PlatformUserID: "platform-u1"}, time.Hour)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// Advance beyond the TTL.
	now = now.Add(2 * time.Hour)
	if _, err := store.Get(context.Background(), sessionID); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("Get(after ttl) error = %v, want ErrSessionExpired", err)
	}
	// Expired sessions are removed, so a later read is NotFound, not Expired.
	if _, err := store.Get(context.Background(), sessionID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Get(after expiry cleanup) error = %v, want ErrSessionNotFound", err)
	}
}
