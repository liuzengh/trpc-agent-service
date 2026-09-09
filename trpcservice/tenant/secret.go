package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
)

var (
	ErrSecretRefEmpty              = errors.New("secret reference is empty")
	ErrSecretRefMalformed          = errors.New("secret reference is malformed")
	ErrSecretRefPlaintext          = errors.New("secret reference must not contain plaintext")
	ErrSecretRefUnsupportedScheme  = errors.New("secret reference scheme is unsupported")
	ErrSecretValueMissing          = errors.New("secret value is unavailable")
	ErrSecretResolutionUnavailable = errors.New("secret resolver is unavailable")
)

type SecretResolver interface {
	Resolve(context.Context, string) (string, error)
}

type SecretResolverFunc func(context.Context, string) (string, error)

func (f SecretResolverFunc) Resolve(ctx context.Context, ref string) (string, error) {
	if f == nil {
		return "", ErrSecretResolutionUnavailable
	}
	return f(ctx, ref)
}

// EnvironmentSecretResolver is the bounded local reference implementation.
// It is a reference boundary, not a cloud Secret Manager integration.
type EnvironmentSecretResolver struct{}

func (EnvironmentSecretResolver) Resolve(ctx context.Context, ref string) (string, error) {
	if ctx == nil {
		return "", ErrSecretResolutionUnavailable
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := ValidateSecretRef(ref); err != nil {
		return "", err
	}
	if !strings.HasPrefix(ref, "env://") {
		return "", ErrSecretRefUnsupportedScheme
	}
	name := strings.TrimPrefix(ref, "env://")
	value, ok := os.LookupEnv(name)
	if !ok || value == "" || len(value) > 4096 {
		return "", ErrSecretValueMissing
	}
	return value, nil
}

func ValidateSecretRef(ref string) error {
	if ref == "" {
		return ErrSecretRefEmpty
	}
	if len(ref) > 256 || strings.TrimSpace(ref) != ref || strings.ContainsAny(ref, "\r\n\x00") {
		return ErrSecretRefMalformed
	}
	separator := strings.Index(ref, "://")
	if separator <= 0 {
		return ErrSecretRefPlaintext
	}
	scheme, value := ref[:separator], ref[separator+3:]
	switch scheme {
	case "env":
		if !validEnvironmentName(value) {
			return ErrSecretRefMalformed
		}
	case "secret":
		if !validOpaqueID(value, 248) || strings.Contains(value, "//") {
			return ErrSecretRefMalformed
		}
	default:
		return ErrSecretRefUnsupportedScheme
	}
	return nil
}

// SecretRefFingerprint is safe for bounded audit metadata and diagnostics.
// It is deliberately not reversible and never returns the reference value.
func IdentityFingerprint(value string) string {
	digest := sha256.Sum256([]byte("identity|" + value))
	return "sha256:" + hex.EncodeToString(digest[:])[:16]
}

func SecretRefFingerprint(ref string) string {
	digest := sha256.Sum256([]byte(ref))
	return "sha256:" + hex.EncodeToString(digest[:])[:16]
}

func validEnvironmentName(value string) bool {
	if value == "" || len(value) > 248 {
		return false
	}
	for index, r := range value {
		if !(r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || index > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
