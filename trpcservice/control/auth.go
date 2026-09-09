package control

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"
)

type AdminAuthenticator struct {
	enabled bool
	digest  [sha256.Size]byte
}

func NewAdminAuthenticator(token string) AdminAuthenticator {
	token = strings.TrimSpace(token)
	if token == "" {
		return AdminAuthenticator{}
	}
	return AdminAuthenticator{enabled: true, digest: sha256.Sum256([]byte(token))}
}

func (a AdminAuthenticator) Enabled() bool { return a.enabled }

func (a AdminAuthenticator) Authenticate(authorization string) error {
	if !a.enabled {
		return ErrAdminAPIDisabled
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return ErrAdminUnauthorized
	}
	candidate := sha256.Sum256([]byte(strings.TrimSpace(strings.TrimPrefix(authorization, prefix))))
	if subtle.ConstantTimeCompare(candidate[:], a.digest[:]) != 1 {
		return ErrAdminUnauthorized
	}
	return nil
}
