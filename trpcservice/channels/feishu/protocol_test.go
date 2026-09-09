package feishu

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"
)

func TestVerifySignatureAndTimestamp(t *testing.T) {
	body := []byte(`{"encrypt":"ciphertext"}`)
	timestamp := "1787716800"
	nonce := "nonce-1"
	key := "encrypt-key"
	hash := sha256.New()
	_, _ = hash.Write([]byte(timestamp + nonce + key))
	_, _ = hash.Write(body)
	signature := hex.EncodeToString(hash.Sum(nil))
	if !verifySignature(timestamp, nonce, key, body, signature) {
		t.Fatal("verifySignature() rejected valid signature")
	}
	if verifySignature(timestamp, nonce, key, body, "wrong") {
		t.Fatal("verifySignature() accepted invalid signature")
	}
	now := time.Unix(1787716800, 0)
	if err := verifyTimestamp(timestamp, now); err != nil {
		t.Fatalf("verifyTimestamp() error = %v", err)
	}
	if err := verifyTimestamp(timestamp, now.Add(10*time.Minute)); err == nil {
		t.Fatal("verifyTimestamp() accepted stale callback")
	}
}

func TestDecryptCallback(t *testing.T) {
	key := "encrypt-key"
	plaintext := []byte(`{"type":"url_verification","challenge":"ok"}`)
	encrypted := encryptCallbackForTest(t, plaintext, key)
	got, err := decryptCallback(encrypted, key)
	if err != nil {
		t.Fatalf("decryptCallback() error = %v", err)
	}
	if string(got) != string(plaintext) {
		t.Fatalf("decryptCallback() = %q, want %q", got, plaintext)
	}
}

func encryptCallbackForTest(t *testing.T, plaintext []byte, key string) string {
	t.Helper()
	hash := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(hash[:])
	if err != nil {
		t.Fatalf("aes.NewCipher() error = %v", err)
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := append([]byte(nil), plaintext...)
	for range padding {
		padded = append(padded, byte(padding))
	}
	iv := []byte("0123456789abcdef")
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, padded)
	return base64.StdEncoding.EncodeToString(append(iv, ciphertext...))
}
