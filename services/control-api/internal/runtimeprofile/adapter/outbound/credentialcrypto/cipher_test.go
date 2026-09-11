package credentialcrypto_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/credentialcrypto"
)

func TestCipherRoundTripUsesRandomNonce(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cipher, err := credentialcrypto.New(key)
	if err != nil {
		t.Fatal(err)
	}
	aad := []byte("tenant-a/profile-a/credential-a/model-api-key")
	plaintext := []byte("example-private-key-value")
	first, err := cipher.Encrypt(context.Background(), aad, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cipher.Encrypt(context.Background(), aad, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("repeated encryption reused an envelope")
	}
	for _, encrypted := range [][]byte{first, second} {
		decrypted, err := cipher.Decrypt(context.Background(), aad, encrypted)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decrypted, plaintext) {
			t.Fatal("decrypted plaintext differs from input")
		}
	}
}

func TestNewRejectsInvalidKeyLengths(t *testing.T) {
	for _, size := range []int{0, 1, 16, 24, 31, 33, 64} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			cipher, err := credentialcrypto.New(make([]byte, size))
			if !errors.Is(err, credentialcrypto.ErrInvalidKey) || cipher != nil {
				t.Fatal("invalid key length did not fail without a cipher")
			}
		})
	}
}

func TestDecryptRejectsTamperingWrongScopeAndWrongKey(t *testing.T) {
	cipher := newCipher(t)
	aad := []byte("tenant-a/profile-a/credential-a/model-api-key")
	encrypted, err := cipher.Encrypt(context.Background(), aad, []byte("private-value-not-for-errors"))
	if err != nil {
		t.Fatal(err)
	}
	otherCipher := newCipher(t)
	tests := []struct {
		name      string
		cipher    *credentialcrypto.Cipher
		aad       []byte
		encrypted []byte
	}{
		{"different tenant", cipher, []byte("tenant-b/profile-a/credential-a/model-api-key"), encrypted},
		{"different profile", cipher, []byte("tenant-a/profile-b/credential-a/model-api-key"), encrypted},
		{"different identity", cipher, []byte("tenant-a/profile-a/credential-b/model-api-key"), encrypted},
		{"different purpose", cipher, []byte("tenant-a/profile-a/credential-a/mcp-token"), encrypted},
		{"missing scope", cipher, nil, encrypted},
		{"wrong key", otherCipher, aad, encrypted},
		{"empty envelope", cipher, aad, nil},
		{"only version", cipher, aad, []byte{1}},
		{"missing tag", cipher, aad, encrypted[:13]},
		{"truncated", cipher, aad, encrypted[:len(encrypted)-1]},
		{"oversized", cipher, aad, make([]byte, credentialcrypto.MaxPlaintextBytes+30)},
	}
	for _, position := range []int{0, 1, 13, len(encrypted) - 1} {
		tampered := bytes.Clone(encrypted)
		tampered[position] ^= 0xff
		tests = append(tests, struct {
			name      string
			cipher    *credentialcrypto.Cipher
			aad       []byte
			encrypted []byte
		}{"tampered byte " + strconv.Itoa(position), cipher, aad, tampered})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plaintext, err := tt.cipher.Decrypt(context.Background(), tt.aad, tt.encrypted)
			if !errors.Is(err, credentialcrypto.ErrInvalidCiphertext) || plaintext != nil {
				t.Fatal("unauthenticated envelope returned plaintext or an unexpected error")
			}
			if err.Error() != "credentialcrypto: invalid encrypted value" {
				t.Fatal("decryption error was not the fixed value-free message")
			}
		})
	}
}

func TestPlaintextSizeBoundary(t *testing.T) {
	cipher := newCipher(t)
	for _, size := range []int{0, 1, credentialcrypto.MaxPlaintextBytes} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			plaintext := bytes.Repeat([]byte{'x'}, size)
			encrypted, err := cipher.Encrypt(context.Background(), []byte("scope"), plaintext)
			if err != nil {
				t.Fatal(err)
			}
			decrypted, err := cipher.Decrypt(context.Background(), []byte("scope"), encrypted)
			if err != nil || !bytes.Equal(plaintext, decrypted) {
				t.Fatal("valid boundary size failed round trip")
			}
		})
	}
	encrypted, err := cipher.Encrypt(context.Background(), nil, make([]byte, credentialcrypto.MaxPlaintextBytes+1))
	if !errors.Is(err, credentialcrypto.ErrValueTooLarge) || encrypted != nil {
		t.Fatal("oversized input was not rejected")
	}
}

func TestCancellationReturnsNoValue(t *testing.T) {
	cipher := newCipher(t)
	encrypted, err := cipher.Encrypt(context.Background(), []byte("scope"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if value, err := cipher.Encrypt(ctx, []byte("scope"), []byte("value")); value != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled encryption did not return only context cancellation")
	}
	if value, err := cipher.Decrypt(ctx, []byte("scope"), encrypted); value != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled decryption did not return only context cancellation")
	}
	deadlineCtx, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	if value, err := cipher.Decrypt(deadlineCtx, nil, nil); value != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired context was not handled before decryption")
	}
}

func TestMACIsKeyedDeterministicAndPurposeSeparated(t *testing.T) {
	key := bytes.Repeat([]byte{0x7b}, 32)
	cipher, err := credentialcrypto.New(key)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("private-request-input")
	mac := cipher.MAC(credentialcrypto.PurposeWriteIdempotency, payload)
	if decoded, err := hex.DecodeString(mac); err != nil || len(decoded) != 32 {
		t.Fatal("MAC is not a 32-byte hexadecimal digest")
	}
	if mac != cipher.MAC(credentialcrypto.PurposeWriteIdempotency, payload) {
		t.Fatal("identical MAC inputs were not deterministic")
	}
	if mac == cipher.MAC(credentialcrypto.PurposeAssociation, payload) {
		t.Fatal("MAC purpose was not separated")
	}
	if mac == cipher.MAC(credentialcrypto.PurposeWriteIdempotency, []byte("different-private-input")) {
		t.Fatal("MAC did not authenticate its payload")
	}
	if mac == newCipher(t).MAC(credentialcrypto.PurposeWriteIdempotency, payload) {
		t.Fatal("MAC did not depend on the configured key")
	}
	if cipher.MAC("a", []byte("bc")) == cipher.MAC("ab", []byte("c")) {
		t.Fatal("ambiguous purpose/payload concatenation")
	}
	clear(key)
	if mac != cipher.MAC(credentialcrypto.PurposeWriteIdempotency, payload) {
		t.Fatal("cipher retained mutable caller-owned key bytes")
	}
}

func TestMACRejectsEmptyPurpose(t *testing.T) {
	cipher := newCipher(t)
	defer func() {
		if recovered := recover(); recovered != "credentialcrypto: MAC purpose is required" {
			t.Fatal("empty MAC purpose did not panic with a fixed programming error")
		}
	}()
	cipher.MAC("", []byte("not-for-error-output"))
}

func TestUninitializedCipherFailsClosed(t *testing.T) {
	for _, cipher := range []*credentialcrypto.Cipher{nil, {}} {
		if value, err := cipher.Encrypt(context.Background(), nil, nil); value != nil || !errors.Is(err, credentialcrypto.ErrNotInitialized) {
			t.Fatal("uninitialized encryption did not fail closed")
		}
		if value, err := cipher.Decrypt(context.Background(), nil, nil); value != nil || !errors.Is(err, credentialcrypto.ErrNotInitialized) {
			t.Fatal("uninitialized decryption did not fail closed")
		}
	}
}

func newCipher(t *testing.T) *credentialcrypto.Cipher {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cipher, err := credentialcrypto.New(key)
	if err != nil {
		t.Fatal(err)
	}
	return cipher
}
