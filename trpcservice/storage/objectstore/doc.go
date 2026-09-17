// Package objectstore defines tenant-scoped immutable object content storage.
package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const keyPrefix = "artifacts/v1/"

// Object is immutable content addressed by a tenant-bound backend key. The
// logical ArtifactRef remains in SQL metadata and must never be replaced by
// ObjectKey in model, protocol, or audit payloads.
type Object struct {
	TenantID, ObjectKey, ContentDigest string
	Content                            []byte
	CreatedAt                          time.Time
}

type Store interface {
	PutObject(context.Context, Object) (Object, error)
	GetObject(context.Context, string, string) (Object, error)
	// DeleteObject is idempotent for an absent key and must refuse deletion
	// when the stored content does not match contentDigest.
	DeleteObject(context.Context, string, string, string) error
}

// StableKey returns an opaque tenant prefix and the stable artifact ID. It
// avoids putting a raw tenant ID, request ID, provider media ID, or filename in
// an object-store path.
func StableKey(tenantID, artifactID string) (string, error) {
	if strings.TrimSpace(tenantID) == "" || !validArtifactID(artifactID) {
		return "", runtime.ErrInvalidEnvelope
	}
	tenantDigest := sha256.Sum256([]byte(tenantID))
	return keyPrefix + base64.RawURLEncoding.EncodeToString(tenantDigest[:]) + "/" + artifactID, nil
}

func Validate(value Object) error {
	if strings.TrimSpace(value.TenantID) == "" || len(value.Content) == 0 || len(value.ContentDigest) != sha256.Size*2 {
		return runtime.ErrInvalidEnvelope
	}
	if _, err := hex.DecodeString(value.ContentDigest); err != nil {
		return runtime.ErrInvalidEnvelope
	}
	parts := strings.Split(value.ObjectKey, "/")
	if len(parts) != 4 || parts[0] != "artifacts" || parts[1] != "v1" || !validArtifactID(parts[3]) {
		return runtime.ErrInvalidEnvelope
	}
	expected, err := StableKey(value.TenantID, parts[3])
	if err != nil || value.ObjectKey != expected {
		return runtime.ErrTenantScope
	}
	digest := sha256.Sum256(value.Content)
	if value.ContentDigest != hex.EncodeToString(digest[:]) {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func ValidateKey(tenantID, objectKey string) error {
	parts := strings.Split(objectKey, "/")
	if strings.TrimSpace(tenantID) == "" || len(parts) != 4 || parts[0] != "artifacts" || parts[1] != "v1" || !validArtifactID(parts[3]) {
		return runtime.ErrTenantScope
	}
	expected, err := StableKey(tenantID, parts[3])
	if err != nil || objectKey != expected {
		return runtime.ErrTenantScope
	}
	return nil
}

func validArtifactID(value string) bool {
	if !strings.HasPrefix(value, "a1_") {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "a1_"))
	return err == nil && len(decoded) == sha256.Size
}
