package application

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

var (
	ErrCredentialIdempotencyConflict  = errors.New("profile credential idempotency conflict")
	ErrExecutionUnauthorized          = errors.New("execution authorization required")
	ErrExecutionDependencyUnavailable = errors.New("execution authorization dependency unavailable")
)

// OwnerAccess is the authorization fact required for credential writes.
type OwnerAccess interface {
	IsActiveOwner(context.Context, string, string) (bool, error)
}

// CredentialCipher receives only explicit secret buffers and scoped AAD.
type CredentialCipher interface {
	Encrypt(context.Context, []byte, []byte) ([]byte, error)
	Decrypt(context.Context, []byte, []byte) ([]byte, error)
	MAC(string, []byte) string
}

// CredentialStore serializes mutation of one Profile and its credential records.
// Callback failure rolls back config, credential writes, CAS, and receipt together.
type CredentialStore interface {
	WithinProfile(context.Context, string, string, func(CredentialTransaction) error) error
}

type CredentialTransaction interface {
	GetDraft(context.Context) (domain.ProfileDraft, error)
	SaveDraft(context.Context, domain.ProfileDraft, int64) error
	GetRevision(context.Context, int64) (domain.ProfileRevision, error)
	GetCredential(context.Context, string) (domain.ProfileCredential, error)
	InsertCredential(context.Context, domain.ProfileCredential) error
	UpdateCredential(context.Context, domain.ProfileCredential, int64) error
	FindReceipt(context.Context, string, string) (CredentialReceipt, bool, error)
	InsertReceipt(context.Context, CredentialReceipt) error
}

type CredentialReceipt struct {
	ActorUserID string
	Key         string
	RequestMAC  string
	Result      json.RawMessage
	CreatedAt   time.Time
}
