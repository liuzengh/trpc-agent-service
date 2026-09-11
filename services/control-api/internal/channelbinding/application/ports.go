// Package application owns tenant-scoped Channel commands and transactional
// orchestration. Database handles and transport identities stay in adapters.
package application

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

var (
	ErrPermissionDenied      = errors.New("CHANNEL_PERMISSION_DENIED")
	ErrAccountNotFound       = errors.New("CHANNEL_ACCOUNT_NOT_FOUND")
	ErrBindingNotFound       = errors.New("CHANNEL_BINDING_NOT_FOUND")
	ErrAccountAlreadyBound   = errors.New("CHANNEL_ACCOUNT_ALREADY_BOUND")
	ErrIdentityConflict      = errors.New("CHANNEL_ACCOUNT_IDENTITY_CONFLICT")
	ErrIdempotencyConflict   = errors.New("CHANNEL_IDEMPOTENCY_CONFLICT")
	ErrDependencyUnavailable = errors.New("CHANNEL_DEPENDENCY_UNAVAILABLE")
	ErrTargetNotFound        = errors.New("CHANNEL_TARGET_NOT_FOUND")
	ErrWorkloadDenied        = errors.New("CHANNEL_WORKLOAD_DENIED")
	ErrEpochMismatch         = errors.New("CHANNEL_SOURCE_EPOCH_MISMATCH")
	errPreparationChanged    = errors.New("channel preparation changed")
)

type Actor struct {
	TenantID string
	UserID   string
}
type TenantAccess interface {
	IsActiveMember(context.Context, string, string) (bool, error)
	IsActiveOwner(context.Context, string, string) (bool, error)
}
type PublishedTargetReader interface {
	ReadExact(context.Context, string, string, domain.TargetSelector) (domain.PublishedTarget, error)
}
type CredentialCipher interface {
	Encrypt(context.Context, []byte, []byte) (string, []byte, error)
	Decrypt(context.Context, string, []byte, []byte) ([]byte, error)
	SignRequest(context.Context, []byte) (string, string, error)
	VerifyRequest(context.Context, string, string, []byte) (bool, error)
}
type Aggregate struct {
	Account     domain.Account
	Credentials []domain.CredentialRecord
	Binding     *domain.Binding
	Route       domain.RouteState
}

func (a Aggregate) CredentialMetadata() []domain.CredentialMeta {
	out := make([]domain.CredentialMeta, 0, len(a.Credentials))
	for _, c := range a.Credentials {
		out = append(out, c.Meta)
	}
	return out
}

type ReceiptKey struct {
	TenantID  string
	Operation string
	ScopeID   string
	KeyHash   string
}
type Receipt struct {
	Key        ReceiptKey
	MACKeyID   string
	RequestMAC string
	Result     json.RawMessage
	CreatedBy  string
	CreatedAt  time.Time
}
type WriteScope struct {
	ScopeID string
	Actor   Actor
}

// Transaction methods operate only on Channel-owned aggregates. The adapter
// locks the catalog, calls the Tenant owner's transaction authorizer, then locks
// Account -> RouteState -> Binding. Save atomically persists route Outbox/floor,
// advances catalog at most once, and enforces complete snapshot count/size bounds.
type Transaction interface {
	FindReceipt(context.Context, ReceiptKey) (Receipt, bool, error)
	LoadAccount(context.Context, string) (Aggregate, error)
	LoadBinding(context.Context, string) (Aggregate, error)
	Save(context.Context, Aggregate) error
	SaveReceipt(context.Context, Receipt) error
}
type CommandStore interface {
	FindReceipt(context.Context, ReceiptKey) (Receipt, bool, error)
	WithWrite(context.Context, WriteScope, func(Transaction) error) error
}
type QueryStore interface {
	GetAccount(context.Context, string, string) (Aggregate, error)
	GetBinding(context.Context, string, string) (Aggregate, error)
}
type Dependencies struct {
	Commands     CommandStore
	Queries      QueryStore
	TenantAccess TenantAccess
	Targets      PublishedTargetReader
	Cipher       CredentialCipher
	ScopeID      string
	NewID        func(string) (string, error)
	Now          func() time.Time
}
type Service struct{ deps Dependencies }

func NewService(deps Dependencies) (*Service, error) {
	if deps.Commands == nil || deps.Queries == nil || deps.TenantAccess == nil || deps.Targets == nil || deps.Cipher == nil || !domain.ValidID(deps.ScopeID) || deps.NewID == nil {
		return nil, ErrDependencyUnavailable
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Service{deps}, nil
}

type AccountView struct {
	domain.Account
	Credentials []domain.CredentialStatus `json:"credentials"`
}
type CommandResult struct {
	Account         *AccountView    `json:"account,omitempty"`
	Binding         *domain.Binding `json:"binding,omitempty"`
	RouteGeneration int64           `json:"route_generation"`
	EventID         string          `json:"event_id,omitempty"`
	Distribution    string          `json:"distribution"`
}

func result(a Aggregate) CommandResult {
	view := AccountView{Account: a.Account, Credentials: domain.PublicCredentialStatus(a.CredentialMetadata())}
	view.Account.ScopeID = "" // The public result and its replay have identical redacted state.
	r := CommandResult{Account: &view, Binding: a.Binding, RouteGeneration: a.Route.Generation, Distribution: "NOT_EMITTED"}
	return r
}
func invalid(field string) error { return &domain.Error{Code: domain.InputInvalid, Field: field} }
