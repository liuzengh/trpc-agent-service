package assembly

import (
	"crypto/sha256"
	"encoding/hex"
)

// backendConnectionFingerprint keeps resolved credentials out of cache keys
// while ensuring secret rotation or endpoint failover produces a fresh
// framework adapter instead of silently reusing an old connection pool.
func backendConnectionFingerprint(connection string) string {
	digest := sha256.Sum256([]byte(connection))
	return hex.EncodeToString(digest[:16])
}
