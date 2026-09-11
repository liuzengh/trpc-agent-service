// Package identity composes the Identity module.
package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

// GenerateSessionToken creates an opaque browser token and a separate stable
// session identifier. Only Hash is persisted.
func GenerateSessionToken() (application.GeneratedSessionToken, error) {
	id, err := GenerateID("ses")
	if err != nil {
		return application.GeneratedSessionToken{}, err
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return application.GeneratedSessionToken{}, fmt.Errorf("generate session secret: %w", err)
	}

	plaintext := base64.RawURLEncoding.EncodeToString(tokenBytes)
	hash := sha256.Sum256([]byte(plaintext))
	return application.GeneratedSessionToken{
		ID:        id,
		Plaintext: plaintext,
		Hash:      hash[:],
	}, nil
}
