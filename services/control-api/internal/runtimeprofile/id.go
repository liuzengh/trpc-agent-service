package runtimeprofile

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

func generateID(prefix string) (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("read random id: %w", err)
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(value), nil
}

func generateCredentialID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate credential identity: %w", err)
	}
	return "crd_" + hex.EncodeToString(value), nil
}
