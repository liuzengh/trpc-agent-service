// Package preflight owns short-lived diagnostic execution, independent of
// account runtime permits, admission, registrations and routing state.
package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrInvalid     = errors.New("preflight: invalid protocol")
	ErrDenied      = errors.New("preflight: authorization denied")
	ErrConflict    = errors.New("preflight: claim or configuration changed")
	ErrUnavailable = errors.New("preflight: control unavailable")
	ErrExpired     = errors.New("preflight: execution expired")
)

// Secret prevents accidental formatting/JSON logging. Only adapters may reveal it.
// Go strings cannot guarantee zeroization; the owner must keep their lifetime short.
type Secret struct{ value string }

func NewSecret(value string) Secret           { return Secret{value: value} }
func (s Secret) Reveal() string               { return s.value }
func (s Secret) String() string               { return "[REDACTED]" }
func (s Secret) GoString() string             { return "[REDACTED]" }
func (s Secret) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }

type ConfigSnapshot struct {
	Policy                                     string
	ScopeID, SourceEpoch, Digest, OriginStatus string
	PublicOrigin                               *string
}
type ClaimRequest struct {
	DiagnosticPolicy         string
	Config                   ConfigSnapshot
	InstanceEpoch, RequestID string
	Token                    Secret
}
type Credential struct {
	Purpose, ID string
	Version     int64
	Configured  bool
}
type Grant struct {
	EndpointProfile                                        string
	AllowConnectionProbe                                   bool
	ReceiveMode, DiagnosticPolicy, EffectiveConfigDigest   string
	PreflightID, ScopeID, SourceEpoch, TenantID, AccountID string
	Provider, ProviderAccountID, WebhookPath               string
	AccountRevision, ConnectionRevision, LeaseEpoch        int64
	Credential                                             Credential
	WebhookSecretConfigured                                bool
	ServerTime, LeaseExpiresAt, JobDeadlineAt              time.Time
	ConfigDigest                                           string
	// Request is the exact local authenticated claim request; never serialized wholesale.
	Request ClaimRequest
}
type Check struct {
	ID      string          `json:"id"`
	Status  string          `json:"status"`
	Code    string          `json:"code"`
	Details json.RawMessage `json:"details"`
}
type Result struct {
	Config     ConfigSnapshot
	ObservedAt time.Time
	Checks     []Check
}

// ProbeResult contains only bounded, declassified fields; never remote URLs,
// provider free-text errors, names, usernames or other bot identifiers.
type ProbeResult struct {
	IdentityCode   string
	IdentityMatch  *bool
	WebhookCode    string
	Presence       *bool
	Relation       string
	PendingUpdates *int64
	HasLastError   *bool
	LastErrorAt    *time.Time
}
type ProbeRequest struct {
	EndpointProfile  string
	Token            Secret
	ExpectedIdentity string
	ExpectedWebhook  *string
}
type Control interface {
	Claim(context.Context, ClaimRequest) (*Grant, error)
	ResolveCredential(context.Context, Grant) (Secret, error)
	Complete(context.Context, Grant, Result) error
}
type TelegramProbe interface {
	Inspect(context.Context, ProbeRequest) (ProbeResult, error)
}

// WeComProbe creates one explicitly authorized subscription, then releases it.
type WeComProbe interface {
	InspectConnection(context.Context, Secret, string) (ConnectionProbeResult, error)
}
type ConnectionProbeResult struct {
	Code          string
	Authenticated *bool
}
