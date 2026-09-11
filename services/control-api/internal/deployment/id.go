package deployment

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func generateID(prefix string) (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("read random id: %w", err)
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(value), nil
}
