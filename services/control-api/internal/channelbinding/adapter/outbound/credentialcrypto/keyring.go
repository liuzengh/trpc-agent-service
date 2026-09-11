// Package credentialcrypto supplies channel-owned AEAD encryption and request
// MACs. It has no persistence, authorization, Profile or Gateway dependency.
package credentialcrypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
)

var (
	ErrKeyUnavailable  = errors.New("channel credential key unavailable")
	ErrInvalidMaterial = errors.New("invalid channel credential material")
)

const maxPlaintextBytes = 16 * 1024
const envelopeVersion byte = 1

// Key contains independently generated 256-bit encryption and MAC keys. Old
// keys must remain configured while ciphertexts or receipts still refer to them.
type Key struct {
	Encryption []byte
	MAC        []byte
}

func (Key) String() string   { return "[channel key]" }
func (Key) GoString() string { return "[channel key]" }

type material struct {
	aead cipher.AEAD
	mac  [32]byte
}
type Keyring struct {
	active string
	keys   map[string]material
}

func (*Keyring) String() string   { return "[channel keyring]" }
func (*Keyring) GoString() string { return "[channel keyring]" }
func New(active string, keys map[string]Key) (*Keyring, error) {
	if active == "" || len(keys) == 0 {
		return nil, ErrKeyUnavailable
	}
	result := &Keyring{active: active, keys: make(map[string]material, len(keys))}
	for id, key := range keys {
		if id == "" || len(id) > 128 || len(key.Encryption) != 32 || len(key.MAC) != 32 || hmac.Equal(key.Encryption, key.MAC) {
			return nil, ErrKeyUnavailable
		}
		block, err := aes.NewCipher(key.Encryption)
		if err != nil {
			return nil, ErrKeyUnavailable
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, ErrKeyUnavailable
		}
		m := material{aead: aead}
		copy(m.mac[:], key.MAC)
		result.keys[id] = m
	}
	if _, ok := result.keys[active]; !ok {
		return nil, ErrKeyUnavailable
	}
	return result, nil
}
func (k *Keyring) Encrypt(ctx context.Context, aad, plaintext []byte) (string, []byte, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if k == nil {
		return "", nil, ErrKeyUnavailable
	}
	m, ok := k.keys[k.active]
	if !ok {
		return "", nil, ErrKeyUnavailable
	}
	if len(aad) == 0 || len(plaintext) == 0 || len(plaintext) > maxPlaintextBytes {
		return "", nil, ErrInvalidMaterial
	}
	nonce := make([]byte, m.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", nil, ErrKeyUnavailable
	}
	envelope := append([]byte{envelopeVersion}, nonce...)
	envelope = m.aead.Seal(envelope, nonce, plaintext, append([]byte{envelopeVersion}, aad...))
	if err := ctx.Err(); err != nil {
		clear(envelope)
		return "", nil, err
	}
	return k.active, envelope, nil
}
func (k *Keyring) Decrypt(ctx context.Context, keyID string, aad, envelope []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if k == nil {
		return nil, ErrKeyUnavailable
	}
	m, ok := k.keys[keyID]
	if !ok {
		return nil, ErrKeyUnavailable
	}
	prefix := 1 + m.aead.NonceSize()
	minimum := prefix + m.aead.Overhead()
	if len(aad) == 0 || len(envelope) <= minimum || len(envelope) > minimum+maxPlaintextBytes || envelope[0] != envelopeVersion {
		return nil, ErrInvalidMaterial
	}
	plaintext, err := m.aead.Open(nil, envelope[1:prefix], envelope[prefix:], append([]byte{envelopeVersion}, aad...))
	if err != nil {
		return nil, ErrInvalidMaterial
	}
	if err := ctx.Err(); err != nil {
		clear(plaintext)
		return nil, err
	}
	return plaintext, nil
}

// SignRequest authenticates a canonical command containing operation, tenant,
// actor, scope, expected versions and write-only credential input. It returns
// only a MAC and its key identity, never a plain credential hash.
func (k *Keyring) SignRequest(ctx context.Context, canonical []byte) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if k == nil {
		return "", "", ErrKeyUnavailable
	}
	m, ok := k.keys[k.active]
	if !ok {
		return "", "", ErrKeyUnavailable
	}
	return k.active, requestMAC(m, canonical), nil
}
func (k *Keyring) VerifyRequest(ctx context.Context, keyID, digest string, canonical []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if k == nil {
		return false, ErrKeyUnavailable
	}
	m, ok := k.keys[keyID]
	if !ok {
		return false, ErrKeyUnavailable
	}
	actual, err := hex.DecodeString(digest)
	if err != nil || len(actual) != 32 {
		return false, ErrInvalidMaterial
	}
	expected, _ := hex.DecodeString(requestMAC(m, canonical))
	return hmac.Equal(actual, expected), nil
}
func requestMAC(m material, canonical []byte) string {
	h := hmac.New(sha256.New, m.mac[:])
	_, _ = h.Write([]byte("channel-account-command-v1\x00"))
	_, _ = h.Write(canonical)
	return hex.EncodeToString(h.Sum(nil))
}

// Rewrap changes the encryption key only. The caller preserves credential ID,
// purpose/version, account/connection versions and all command receipts.
func (k *Keyring) Rewrap(ctx context.Context, keyID string, aad, envelope []byte) (string, []byte, error) {
	plaintext, err := k.Decrypt(ctx, keyID, aad, envelope)
	if err != nil {
		return "", nil, err
	}
	defer clear(plaintext)
	return k.Encrypt(ctx, aad, plaintext)
}
