package secret

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"testing"
)

// newCodecStore builds a MySQLStore with only its AEAD codec set (no db), for
// exercising seal/open independently of MySQL.
func newCodecStore(t *testing.T, masterKey string) *MySQLStore {
	t.Helper()
	block, err := aes.NewCipher(deriveKey(masterKey))
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}
	return &MySQLStore{aead: aead}
}

// TestMemStore exercises the in-memory backend semantics (used by tests and
// the shared contract assertions).
func TestMemStore(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()

	if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get missing = %v, want ErrNotFound", err)
	}
	if err := s.Put(ctx, "k1", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, _ := s.Get(ctx, "k1"); got != "v1" {
		t.Errorf("Get = %q, want v1", got)
	}
	// overwrite
	if err := s.Put(ctx, "k1", "v2"); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	if got, _ := s.Get(ctx, "k1"); got != "v2" {
		t.Errorf("Get after overwrite = %q, want v2", got)
	}
	// list returns metadata only
	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Key != "k1" {
		t.Errorf("List = %+v, want [k1]", list)
	}
	// empty key rejected
	if err := s.Put(ctx, "", "x"); err == nil {
		t.Error("Put with empty key must error")
	}
	// delete
	if err := s.Delete(ctx, "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "k1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
}

// TestMySQLStoreRejectsEmptyMasterKey guards the no-plaintext-at-rest rule.
func TestMySQLStoreRejectsEmptyMasterKey(t *testing.T) {
	if _, err := NewMySQLStore(nil, ""); err == nil {
		t.Error("NewMySQLStore with empty master key must error")
	}
}

// TestSealOpenRoundTrip verifies the AES-GCM codec independent of MySQL.
func TestSealOpenRoundTrip(t *testing.T) {
	s := newCodecStore(t, "master")
	ct, err := s.seal("top-secret-🔑")
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if string(ct) == "top-secret-🔑" {
		t.Error("ciphertext must not equal plaintext")
	}
	got, err := s.open(ct)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got != "top-secret-🔑" {
		t.Errorf("round trip = %q, want original", got)
	}
}

// TestSealOpenWrongKey shows a different master key cannot decrypt (the GCM
// tag check fails).
func TestSealOpenWrongKey(t *testing.T) {
	a := newCodecStore(t, "master-a")
	b := newCodecStore(t, "master-b")
	ct, err := a.seal("value")
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := b.open(ct); err == nil {
		t.Error("decrypting with a different master key must fail")
	}
}
