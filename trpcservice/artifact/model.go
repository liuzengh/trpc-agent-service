package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusReady   Status = "ready"
	StatusFailed  Status = "failed"
	StatusExpired Status = "expired"
)

type Artifact struct {
	TenantID  string    `json:"tenant_id"`
	ID        string    `json:"artifact_id"`
	SessionID string    `json:"session_id"`
	MessageID string    `json:"message_id"`
	ObjectKey string    `json:"object_key"`
	MIMEType  string    `json:"mime_type"`
	SizeBytes int64     `json:"size_bytes"`
	SHA256    string    `json:"sha256"`
	Status    Status    `json:"status"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

var ErrInvalidArtifact = errors.New("invalid artifact")
var ErrInvalidTransition = errors.New("invalid artifact status transition")

func (a Artifact) Validate() error {
	for name, value := range map[string]string{"tenant_id": a.TenantID, "artifact_id": a.ID, "session_id": a.SessionID, "message_id": a.MessageID, "mime_type": a.MIMEType} {
		if strings.TrimSpace(value) == "" || len(value) > 256 {
			return fmt.Errorf("%w: %s is required and bounded", ErrInvalidArtifact, name)
		}
	}
	prefix := "tenants/" + a.TenantID + "/"
	clean := path.Clean(a.ObjectKey)
	if !strings.HasPrefix(a.ObjectKey, prefix) || clean != a.ObjectKey || strings.Contains(a.ObjectKey, "..") || strings.ContainsRune(a.ObjectKey, '\\') {
		return fmt.Errorf("%w: object key must be tenant-prefixed and relative", ErrInvalidArtifact)
	}
	if a.SizeBytes < 0 || !validStatus(a.Status) {
		return fmt.Errorf("%w: invalid size or status", ErrInvalidArtifact)
	}
	if a.SHA256 != "" {
		decoded, err := hex.DecodeString(a.SHA256)
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("%w: sha256 must be 64 hexadecimal characters", ErrInvalidArtifact)
		}
	}
	if a.Status == StatusReady && (a.SizeBytes < 0 || a.SHA256 == "") {
		return fmt.Errorf("%w: ready artifact requires hash", ErrInvalidArtifact)
	}
	return nil
}

func (a Artifact) Transition(next Status) (Artifact, error) {
	if err := a.Validate(); err != nil {
		return Artifact{}, err
	}
	if !validTransition(a.Status, next) {
		return Artifact{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, a.Status, next)
	}
	a.Status = next
	if err := a.Validate(); err != nil {
		return Artifact{}, err
	}
	return a, nil
}

func validTransition(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusReady || to == StatusFailed || to == StatusExpired
	case StatusReady:
		return to == StatusExpired
	case StatusFailed:
		return to == StatusExpired
	default:
		return false
	}
}

func validStatus(value Status) bool {
	switch value {
	case StatusPending, StatusReady, StatusFailed, StatusExpired:
		return true
	default:
		return false
	}
}
