package application

import (
	"context"
	"encoding/json"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// PreflightRecord contains private task metadata, never plaintext credentials.
type PreflightRecord struct {
	BotSecretConfigured     bool                    `json:"bot_secret_configured,omitempty"`
	View                    channelv1.PreflightView `json:"view"`
	ScopeID                 string                  `json:"scope_id"`
	SourceEpoch             string                  `json:"source_epoch"`
	CredentialID            string                  `json:"credential_id"`
	BotTokenConfigured      bool                    `json:"bot_token_configured"`
	WebhookSecretConfigured bool                    `json:"webhook_secret_configured"`
	WebhookPath             string                  `json:"webhook_path"`
	LeaseEpoch              int64                   `json:"lease_epoch"`
	LeaseExpiresAt          *time.Time              `json:"lease_expires_at"`
	PrincipalID             string                  `json:"principal_id"`
	InstanceID              string                  `json:"instance_id"`
	InstanceEpoch           string                  `json:"instance_epoch"`
	ClaimTokenHash          string                  `json:"claim_token_hash"`
	GatewayPublicOrigin     *string                 `json:"gateway_public_origin,omitempty"`
	OriginStatus            string                  `json:"origin_status"`
	CompleteDigest          string                  `json:"complete_digest"`
	LastCheckedAt           time.Time               `json:"last_checked_at"`
}
type PreflightKey struct{ ID, TenantID, AccountID, RequestedBy string }
type PreflightRequest struct {
	Kind, Key, Digest, PrincipalID, InstanceID, InstanceEpoch string
	TenantID, AccountID, RequestedBy, PreflightID             string
	CreatedAt, ExpiresAt                                      time.Time
	Response                                                  json.RawMessage
}
type PreflightAccount struct {
	Account                                    domain.Account
	Credentials                                []domain.CredentialRecord
	TenantActive, RequesterOwner, SessionOwner bool
	RequesterOwners                            map[string]bool
}

// Transactions lock catalog -> named fence -> Identity/Tenant -> Account ->
// task. Request receipt lookup precedes account locking without taking row locks.
type PreflightTransaction interface {
	Now(context.Context) (time.Time, error)
	SourceEpoch() string
	LockAccount(context.Context, string, string, string, bool, ...string) (PreflightAccount, error)
	Load(context.Context, string) (PreflightRecord, bool, error)
	Save(context.Context, PreflightRecord) error
	FindRequest(context.Context, string, string) (PreflightRequest, bool, error)
	SaveRequest(context.Context, PreflightRequest) error
	Active(context.Context, string, string) ([]PreflightRecord, error)
	Counts(context.Context, string, string, time.Time) (accountCount, tenantCount, tenantActive int, err error)
	ClaimCount(context.Context, string, string, time.Time) (int, error)
}
type PreflightStore interface {
	WithTransaction(context.Context, string, func(PreflightTransaction) error) error
	Lookup(context.Context, string, string) (PreflightKey, bool, error)
	Candidate(context.Context, string, ...string) (PreflightKey, bool, error)
	ActiveAccount(context.Context, string, string, string) (PreflightKey, bool, error)
	MaintenanceCandidates(context.Context, string, int) ([]PreflightKey, error)
	Cleanup(context.Context) error
}
type PreflightDependencies struct {
	Store                PreflightStore
	Access               TenantAccess
	Accounts             QueryStore
	Cipher               CredentialCipher
	ScopeID, SourceEpoch string
	NewID                func(string) (string, error)
}

// CredentialConfigured keeps legacy Telegram records byte-compatible while
// storing WeCom configuration under its own purpose-specific metadata.
func (r PreflightRecord) CredentialConfigured() bool {
	if r.View.Provider == "wecom" {
		return r.BotSecretConfigured
	}
	return r.BotTokenConfigured
}
