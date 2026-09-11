package argon2id_test

import (
	"testing"

	passwordargon2id "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/outbound/argon2id"
)

func TestHasherRoundTrip(t *testing.T) {
	hasher := passwordargon2id.New(passwordargon2id.DefaultParameters())

	encoded, err := hasher.Hash("correct horse battery staple")
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}

	matches, err := hasher.Verify(encoded, "correct horse battery staple")
	if err != nil {
		t.Fatalf("Verify(correct) error = %v", err)
	}
	if !matches {
		t.Fatal("Verify(correct) = false, want true")
	}

	matches, err = hasher.Verify(encoded, "wrong password")
	if err != nil {
		t.Fatalf("Verify(wrong) error = %v", err)
	}
	if matches {
		t.Fatal("Verify(wrong) = true, want false")
	}
}

func TestHasherRejectsMalformedEncodedHash(t *testing.T) {
	hasher := passwordargon2id.New(passwordargon2id.DefaultParameters())

	if _, err := hasher.Verify("not-a-phc-string", "password"); err == nil {
		t.Fatal("Verify() error = nil, want malformed-hash error")
	}
}
