package tool

import (
	"bytes"
	"database/sql"
	"testing"
)

func TestPostgresExecutionLedgerRequiresDatabaseAndStrongKeyMaterial(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte("k"), 32)
	if _, err := NewPostgresExecutionLedger(nil, key); err == nil {
		t.Fatal("NewPostgresExecutionLedger accepted nil database")
	}
	if _, err := NewPostgresExecutionLedger(&sql.DB{}, []byte("short")); err == nil {
		t.Fatal("NewPostgresExecutionLedger accepted short key material")
	}
	ledger, err := NewPostgresExecutionLedger(&sql.DB{}, key)
	if err != nil || ledger == nil {
		t.Fatalf("NewPostgresExecutionLedger() = %#v, %v", ledger, err)
	}
}

func TestToolResultEncryptionRoundTripAndTamperDetection(t *testing.T) {
	t.Parallel()
	var key [32]byte
	copy(key[:], bytes.Repeat([]byte{0x42}, len(key)))
	plaintext := []byte("{\"approved\":true,\"reference\":\"refund-42\"}")
	first, err := encryptToolResult(key, plaintext)
	if err != nil {
		t.Fatalf("encryptToolResult() error = %v", err)
	}
	second, err := encryptToolResult(key, plaintext)
	if err != nil {
		t.Fatalf("second encryptToolResult() error = %v", err)
	}
	if bytes.Equal(first, plaintext) || bytes.Equal(first, second) {
		t.Fatal("tool result encryption exposed plaintext or reused a nonce")
	}
	decrypted, err := decryptToolResult(key, first)
	if err != nil || !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decryptToolResult() = %q, %v", decrypted, err)
	}

	tampered := append([]byte(nil), first...)
	tampered[len(tampered)-1] ^= 0xff
	if _, err := decryptToolResult(key, tampered); err == nil {
		t.Fatal("tampered tool result ciphertext was accepted")
	}
	otherKey := key
	otherKey[0] ^= 0xff
	if _, err := decryptToolResult(otherKey, first); err == nil {
		t.Fatal("tool result encrypted under another key was accepted")
	}
	if _, err := decryptToolResult(key, []byte("short")); err == nil {
		t.Fatal("truncated tool result ciphertext was accepted")
	}
}
