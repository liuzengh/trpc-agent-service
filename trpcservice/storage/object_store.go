package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrObjectInvalid      = errors.New("invalid object request")
	ErrObjectNotFound     = errors.New("object not found")
	ErrObjectTooLarge     = errors.New("object is too large")
	ErrObjectChecksum     = errors.New("object checksum mismatch")
	ErrObjectUnavailable  = errors.New("object storage unavailable")
	ErrObjectUnauthorized = errors.New("object storage unauthorized")
)

const DefaultMaxObjectBytes int64 = 32 << 20

// ObjectStore transports artifact bytes independently from Artifact metadata.
// Implementations own provider buckets and derive keys from tenant context plus
// a server-generated artifact ID.
type ObjectStore interface {
	Put(context.Context, tenant.TenantContext, ObjectUpload) (ObjectInfo, error)
	Get(context.Context, tenant.TenantContext, string) (io.ReadCloser, ObjectInfo, error)
	Head(context.Context, tenant.TenantContext, string) (ObjectInfo, error)
	Delete(context.Context, tenant.TenantContext, string) error
	PresignedURL(context.Context, tenant.TenantContext, string, time.Duration) (string, error)
}

// ObjectStoreReadiness is optional transport health used by production
// composition. It is deliberately separate from byte operations so a
// readiness probe does not need a synthetic artifact.
type ObjectStoreReadiness interface {
	Ready(context.Context) error
}

type ObjectUpload struct {
	ArtifactID     string
	MIMEType       string
	ExpectedSize   int64
	ExpectedSHA256 string
	Body           io.Reader
}

type ObjectInfo struct {
	TenantID   string
	ArtifactID string
	ObjectKey  string
	MIMEType   string
	SizeBytes  int64
	SHA256     string
	ETag       string
}

func CanonicalObjectKey(tenantID, artifactID string) (string, error) {
	if !validObjectComponent(tenantID) || !validObjectComponent(artifactID) {
		return "", fmt.Errorf("%w: invalid object identity", ErrObjectInvalid)
	}
	return "tenants/" + tenantID + "/artifacts/" + artifactID, nil
}

func ValidateObjectUpload(upload ObjectUpload, maxBytes int64) error {
	if maxBytes < 1 || upload.Body == nil || upload.ExpectedSize < 0 {
		return fmt.Errorf("%w: body and bounded size are required", ErrObjectInvalid)
	}
	if upload.ExpectedSize > maxBytes {
		return ErrObjectTooLarge
	}
	if err := validateObjectText(upload.MIMEType, 128); err != nil {
		return fmt.Errorf("%w: mime type", ErrObjectInvalid)
	}
	if len(upload.ExpectedSHA256) != 64 || strings.TrimSpace(upload.ExpectedSHA256) != upload.ExpectedSHA256 {
		return fmt.Errorf("%w: sha256 must be 64 hexadecimal characters", ErrObjectInvalid)
	}
	for _, r := range upload.ExpectedSHA256 {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return fmt.Errorf("%w: sha256 must be 64 hexadecimal characters", ErrObjectInvalid)
		}
	}
	if _, err := CanonicalObjectKey("tenant", upload.ArtifactID); err != nil {
		return err
	}
	return nil
}

func validObjectComponent(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value || strings.Contains(value, "..") {
		return false
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func validateObjectText(value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\x00") {
		return ErrObjectInvalid
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return ErrObjectInvalid
		}
	}
	return nil
}
