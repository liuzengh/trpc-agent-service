package storage

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const minimumAuditHMACKeyBytes = 32

// AuditContentDigester converts private conversation content into a stable,
// tenant-opaque fingerprint. Its key must come from deployment secret storage.
type AuditContentDigester struct {
	key []byte
}

func NewAuditContentDigester(key []byte) (AuditContentDigester, error) {
	if len(key) < minimumAuditHMACKeyBytes {
		return AuditContentDigester{}, fmt.Errorf("audit HMAC key must be at least %d bytes", minimumAuditHMACKeyBytes)
	}
	return AuditContentDigester{key: append([]byte(nil), key...)}, nil
}

func (d AuditContentDigester) Digest(content string) string {
	mac := hmac.New(sha256.New, d.key)
	_, _ = mac.Write([]byte(content))
	return "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
}

// memoryAuditDigester fingerprints conversation content for the in-memory
// state store. The key is generated per process so the binary never ships a
// hard-coded secret. Digests are stable within a process but not across
// restarts, which is acceptable because MemoryStateStore is an isolated,
// non-persistent backend intended for tests and local demos.
var memoryAuditDigester = newMemoryAuditDigester()

func newMemoryAuditDigester() AuditContentDigester {
	key := make([]byte, minimumAuditHMACKeyBytes)
	if _, err := rand.Read(key); err != nil {
		// crypto/rand is unavailable only in catastrophic environments; fall
		// back to a deterministic, clearly non-secret placeholder that still
		// satisfies the minimum key length.
		copy(key, []byte("insecure-memory-store-fallback-key-32b"))
	}
	// key always satisfies the minimum length, so this cannot fail; construct
	// directly instead of panicking or ignoring the error.
	return AuditContentDigester{key: key}
}
