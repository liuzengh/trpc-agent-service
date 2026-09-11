package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// GenerateID returns a stable opaque identifier with a domain-readable prefix.
func GenerateID(prefix string) (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate %s id: %w", prefix, err)
	}
	return prefix + "_" + hex.EncodeToString(random), nil
}
