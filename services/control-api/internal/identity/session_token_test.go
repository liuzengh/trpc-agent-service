package identity_test

import (
	"crypto/sha256"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity"
)

func TestGenerateSessionTokenKeepsOnlyHashForPersistence(t *testing.T) {
	token, err := identity.GenerateSessionToken()
	if err != nil {
		t.Fatalf("GenerateSessionToken() error = %v", err)
	}
	if token.ID == "" || token.Plaintext == "" {
		t.Fatalf("token = %#v, want non-empty id and plaintext", token)
	}
	wantHash := sha256.Sum256([]byte(token.Plaintext))
	if string(token.Hash) != string(wantHash[:]) {
		t.Fatal("persisted hash does not match plaintext token")
	}

	other, err := identity.GenerateSessionToken()
	if err != nil {
		t.Fatalf("second GenerateSessionToken() error = %v", err)
	}
	if other.ID == token.ID || other.Plaintext == token.Plaintext {
		t.Fatal("two generated sessions reused identity or token material")
	}
}
