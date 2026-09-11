// Package credentialcrypto encrypts Profile-owned credential values and computes
// purpose-separated request MACs. It does not load keys, store credentials, or
// decide tenant authorization. Callers supply canonical, unambiguous AAD that
// binds the tenant, profile, credential identity, and credential purpose.
package credentialcrypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
)

const (
	// MaxPlaintextBytes bounds one credential value before encryption.
	MaxPlaintextBytes = 64 * 1024

	// PurposeWriteIdempotency separates private request-comparison MACs.
	PurposeWriteIdempotency = "profile-write-idempotency-v1"
	// PurposeAssociation separates conditional-write association tokens.
	PurposeAssociation = "profile-credential-association-v1"

	envelopeVersion byte = 1
	keySize              = 32
	keyDomain            = "trpc-agent-service/runtimeprofile/credentialcrypto/v1/"
)

var (
	ErrInvalidKey        = errors.New("credentialcrypto: key must contain exactly 32 bytes")
	ErrValueTooLarge     = errors.New("credentialcrypto: value exceeds size limit")
	ErrInvalidCiphertext = errors.New("credentialcrypto: invalid encrypted value")
	ErrNotInitialized    = errors.New("credentialcrypto: cipher is not initialized")
)

// Cipher is immutable after New and may be shared by concurrent callers.
// Construct it with New; the zero value has no encryption key.
type Cipher struct {
	aead   cipher.AEAD
	macKey [sha256.Size]byte
}

// New requires an externally generated 32-byte random master key. It validates
// length, not entropy; deployment configuration owns secure key generation and
// persistence. It never generates a default key or retains the caller's slice.
func New(key []byte) (*Cipher, error) {
	if len(key) != keySize {
		return nil, ErrInvalidKey
	}
	encryptionKey := deriveKey(key, "encryption")
	defer clear(encryptionKey[:])
	block, err := aes.NewCipher(encryptionKey[:])
	if err != nil {
		return nil, ErrInvalidKey
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrNotInitialized
	}
	return &Cipher{aead: aead, macKey: deriveKey(key, "mac")}, nil
}

// Encrypt produces version || nonce || AES-256-GCM ciphertext-and-tag. A fresh
// cryptographic nonce is generated for every call; the version and caller AAD
// are authenticated. The plaintext and AAD are not modified.
func (c *Cipher) Encrypt(ctx context.Context, aad, plaintext []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || c.aead == nil {
		return nil, ErrNotInitialized
	}
	if len(plaintext) > MaxPlaintextBytes {
		return nil, ErrValueTooLarge
	}
	prefixSize := 1 + c.aead.NonceSize()
	envelope := make([]byte, prefixSize, prefixSize+len(plaintext)+c.aead.Overhead())
	envelope[0] = envelopeVersion
	nonce := envelope[1:prefixSize]
	if _, err := rand.Read(nonce); err != nil {
		return nil, errors.New("credentialcrypto: nonce generation failed")
	}
	envelope = c.aead.Seal(envelope, nonce, plaintext, authenticatedData(aad))
	if err := ctx.Err(); err != nil {
		clear(envelope)
		return nil, err
	}
	return envelope, nil
}

// Decrypt rejects unknown versions, malformed or oversized envelopes, and
// authentication failures with one value-free error. It returns no plaintext on
// error, including cancellation observed after authenticated decryption.
func (c *Cipher) Decrypt(ctx context.Context, aad, encrypted []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || c.aead == nil {
		return nil, ErrNotInitialized
	}
	prefixSize := 1 + c.aead.NonceSize()
	minSize := prefixSize + c.aead.Overhead()
	if len(encrypted) < minSize || len(encrypted) > minSize+MaxPlaintextBytes || encrypted[0] != envelopeVersion {
		return nil, ErrInvalidCiphertext
	}
	plaintext, err := c.aead.Open(nil, encrypted[1:prefixSize], encrypted[prefixSize:], authenticatedData(aad))
	if err != nil {
		return nil, ErrInvalidCiphertext
	}
	if err := ctx.Err(); err != nil {
		clear(plaintext)
		return nil, err
	}
	return plaintext, nil
}

// MAC returns a hexadecimal, purpose-separated HMAC-SHA256. Purpose must be a
// nonempty application-owned constant, never unvalidated request input. An empty
// purpose or uninitialized Cipher is a programming error and panics because this
// API deliberately has no error result. MACs are private comparison artifacts,
// not public password digests or replacements for authentication/authorization.
func (c *Cipher) MAC(purpose string, payload []byte) string {
	if c == nil || c.aead == nil {
		panic("credentialcrypto: cipher is not initialized")
	}
	if purpose == "" {
		panic("credentialcrypto: MAC purpose is required")
	}
	mac := hmac.New(sha256.New, c.macKey[:])
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(purpose)))
	_, _ = mac.Write(size[:])
	_, _ = mac.Write([]byte(purpose))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func deriveKey(master []byte, label string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, master)
	_, _ = mac.Write([]byte(keyDomain + label))
	var key [sha256.Size]byte
	copy(key[:], mac.Sum(nil))
	return key
}

func authenticatedData(aad []byte) []byte {
	result := make([]byte, 1+len(aad))
	result[0] = envelopeVersion
	copy(result[1:], aad)
	return result
}
