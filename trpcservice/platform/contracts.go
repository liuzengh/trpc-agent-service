// Package platform contains the stable domain and adapter contracts shared by
// the gateway, workers, channels, and storage implementations.
package platform

import (
	"context"
	"errors"
	"time"
)

// TenantContext is the trusted tenant identity attached by an ingress
// middleware. It is deliberately not constructible from a client request.
type TenantContext struct {
	TenantID    string
	UserID      string
	Role        Role
	Assignments []TenantAssignment
}

// AllowsTenant reports whether this trusted identity may access tenantID.
// Contexts created by older in-process callers may omit Assignments; those
// contexts remain scoped to their active TenantID only.
func (t TenantContext) AllowsTenant(tenantID string) bool {
	if tenantID == "" {
		return false
	}
	if len(t.Assignments) == 0 {
		return t.TenantID == tenantID
	}
	for _, assignment := range t.Assignments {
		if assignment.TenantID == tenantID {
			return true
		}
	}
	return false
}

func (t TenantContext) AssignmentFor(tenantID string) (TenantAssignment, bool) {
	if len(t.Assignments) == 0 && t.TenantID == tenantID {
		return TenantAssignment{TenantID: tenantID, Role: t.Role}, true
	}
	for _, assignment := range t.Assignments {
		if assignment.TenantID == tenantID {
			return assignment, true
		}
	}
	return TenantAssignment{}, false
}

// TenantIDFromContext is a convenience for ports that only need the boundary
// identifier. The boolean is false when middleware did not establish trust.
func TenantIDFromContext(ctx context.Context) (string, bool) {
	tenant, ok := TenantContextFromContext(ctx)
	return tenant.TenantID, ok
}

type tenantContextKey struct{}

// WithTenantContext is intended for trusted server middleware only.
func WithTenantContext(ctx context.Context, tenant TenantContext) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenant)
}

// TenantContextFromContext returns the server-injected tenant identity.
func TenantContextFromContext(ctx context.Context) (TenantContext, bool) {
	if ctx == nil {
		return TenantContext{}, false
	}
	value, ok := ctx.Value(tenantContextKey{}).(TenantContext)
	return value, ok && value.TenantID != ""
}

type Tenant struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	CreatedAt   time.Time   `json:"created_at"`
	AuditPolicy AuditPolicy `json:"audit_policy"`
}

type AuditContentMode string

const (
	AuditMetadataOnly    AuditContentMode = "metadata_only"
	AuditRedactedSummary AuditContentMode = "redacted_summary"
)

type AuditPolicy struct {
	RetentionDays       int              `json:"retention_days"`
	ContentMode         AuditContentMode `json:"content_mode"`
	HighRiskFailureMode string           `json:"high_risk_failure_mode"`
}

func DefaultAuditPolicy() AuditPolicy {
	return AuditPolicy{RetentionDays: 90, ContentMode: AuditMetadataOnly, HighRiskFailureMode: "fail_closed"}
}

func (p AuditPolicy) Normalize() (AuditPolicy, error) {
	defaults := DefaultAuditPolicy()
	if p.RetentionDays == 0 {
		p.RetentionDays = defaults.RetentionDays
	}
	if p.ContentMode == "" {
		p.ContentMode = defaults.ContentMode
	}
	if p.HighRiskFailureMode == "" {
		p.HighRiskFailureMode = defaults.HighRiskFailureMode
	}
	if p.RetentionDays < 1 || p.RetentionDays > 3650 {
		return AuditPolicy{}, errors.New("invalid_audit_retention_days")
	}
	if p.ContentMode != AuditMetadataOnly && p.ContentMode != AuditRedactedSummary {
		return AuditPolicy{}, errors.New("invalid_audit_content_mode")
	}
	if p.HighRiskFailureMode != "fail_closed" {
		return AuditPolicy{}, errors.New("invalid_high_risk_failure_mode")
	}
	return p, nil
}

func normalizedAuditPolicy(policy AuditPolicy) AuditPolicy {
	normalized, err := policy.Normalize()
	if err != nil {
		return DefaultAuditPolicy()
	}
	return normalized
}

type AgentApp struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type DeploymentStatus string
type DeploymentRolloutStatus string

const (
	DeploymentDraft     DeploymentStatus = "draft"
	DeploymentPublished DeploymentStatus = "published"
	DeploymentActive    DeploymentStatus = "active"
	DeploymentPaused    DeploymentStatus = "paused"
)

const (
	DeploymentRolloutIdle       DeploymentRolloutStatus = "idle"
	DeploymentRolloutInProgress DeploymentRolloutStatus = "rolling"
	DeploymentRolloutCompleted  DeploymentRolloutStatus = "completed"
)

type Deployment struct {
	ID                string                  `json:"id"`
	TenantID          string                  `json:"tenant_id"`
	AgentAppID        string                  `json:"agent_app_id"`
	VersionID         string                  `json:"version_id,omitempty"`
	Status            DeploymentStatus        `json:"status"`
	DesiredReplicas   int                     `json:"desired_replicas"`
	RolloutStatus     DeploymentRolloutStatus `json:"rollout_status"`
	CurrentVersionID  string                  `json:"current_version_id,omitempty"`
	TargetVersionID   string                  `json:"target_version_id,omitempty"`
	PreviousVersionID string                  `json:"previous_version_id,omitempty"`
	GrayPercentage    int                     `json:"gray_percentage"`
	CreatedAt         time.Time               `json:"created_at"`
}

type DeploymentVersion struct {
	ID           string         `json:"id"`
	TenantID     string         `json:"tenant_id"`
	AgentAppID   string         `json:"agent_app_id"`
	DeploymentID string         `json:"deployment_id"`
	Number       int            `json:"number"`
	Config       map[string]any `json:"config"`
	CreatedAt    time.Time      `json:"created_at"`
	Active       bool           `json:"-"`
}

// DeploymentVersionRef is the authoritative identity used by runtime code.
// Version IDs are only unique inside a tenant's deployment namespace.
type DeploymentVersionRef struct {
	TenantID  string
	VersionID string
}

func versionRefKey(ref DeploymentVersionRef) string {
	return ref.TenantID + "\x00" + ref.VersionID
}

type DeploymentRollbackPreview struct {
	TenantID          string `json:"tenant_id"`
	AgentAppID        string `json:"agent_app_id"`
	DeploymentID      string `json:"deployment_id"`
	CurrentVersionID  string `json:"current_version_id"`
	PreviousVersionID string `json:"previous_version_id"`
	ActiveExecutions  int64  `json:"active_executions"`
	ExpectedResult    string `json:"expected_result"`
}

type GatewayRequest struct {
	TenantID        string             `json:"-"`
	AppID           string             `json:"app_id"`
	SessionID       string             `json:"session_id"`
	UserID          string             `json:"-"`
	Channel         string             `json:"-"`
	ExternalSubject string             `json:"-"`
	Input           string             `json:"input"`
	RequestID       string             `json:"-"`
	TraceID         string             `json:"-"`
	TraceParent     string             `json:"-"`
	DeploymentID    string             `json:"-"`
	VersionID       string             `json:"-"`
	Version         *DeploymentVersion `json:"-"`
	PolicyRevision  uint64             `json:"-"`
	FencingToken    uint64             `json:"-"`
}

type GatewayResponse struct {
	SessionID   string `json:"session_id"`
	Output      string `json:"output"`
	UsageTokens int64  `json:"-"`
	UsageKnown  bool   `json:"-"`
}

type Gateway interface {
	Handle(context.Context, GatewayRequest) (GatewayResponse, error)
}

type Worker interface {
	Execute(context.Context, GatewayRequest) (GatewayResponse, error)
}

type Session struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id,omitempty"`
	UserID   string `json:"user_id,omitempty"`
	Sequence uint64 `json:"sequence"`
}

type SessionEvent struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	SessionID      string    `json:"session_id"`
	Sequence       uint64    `json:"sequence"`
	IdempotencyKey string    `json:"idempotency_key"`
	Type           string    `json:"type"`
	Payload        []byte    `json:"payload"`
	OccurredAt     time.Time `json:"occurred_at"`
	FencingToken   uint64    `json:"fencing_token,omitempty"`
}

type RunnerRequest struct {
	TenantID         string
	AppID            string
	SessionID        string
	UserID           string
	Channel          string
	ProviderAccount  string
	ConversationType string
	ExternalSubject  string
	Input            string
	RequestID        string
	TraceID          string
	TraceParent      string
	DeploymentID     string
	VersionID        string
	Version          *DeploymentVersion `json:"version,omitempty"`
	PolicyRevision   uint64
	FencingToken     uint64
}

type RunnerResponse struct {
	Output      string
	UsageTokens int64
	UsageKnown  bool
}

// RunnerAdapter is the platform boundary around trpc-agent-go's runner.Runner.
type RunnerAdapter interface {
	Run(context.Context, RunnerRequest) (RunnerResponse, error)
}

type ChannelMessage struct {
	AppID            string
	SessionID        string
	MessageID        string
	UserID           string
	ConversationType string
	ConversationID   string
	Text             string
	ProviderSequence uint64
	AttachmentName   string
	AttachmentSize   int
	ReceivedAt       time.Time
}

type ChannelReply struct {
	MessageID string
	Text      string
}

type ChannelCallback struct {
	Channel    string
	BindingID  string
	Body       []byte
	Signature  string
	Timestamp  string
	Nonce      string
	Credential ChannelCredential
	Scope      string
}

type ChannelCredential struct {
	TenantID string
	Channel  string
	Secret   string
	Token    string
}

type ChannelSignatureVerifier interface {
	Verify(ctx context.Context, credential ChannelCredential, body []byte, signature string) error
}

type ChannelDelivery struct {
	MessageID   string
	Status      string
	Code        string
	Attempts    int
	LastAttempt time.Time
}

// ChannelAdapter converts an external IM callback into platform messages and
// sends replies back through the same tenant-scoped binding.
type ChannelAdapter interface {
	Receive(context.Context, ChannelCallback) (ChannelMessage, error)
	Send(context.Context, ChannelBinding, ChannelReply) (ChannelDelivery, error)
}

// StorageAdapter is intentionally small: concrete backends may add specialized
// Memory, Summary, Artifact, or Knowledge methods without changing this core.
type StorageAdapter interface {
	GetSession(context.Context, string, string) (Session, error)
	AppendSessionEvent(context.Context, SessionEvent) error
	ListSessionEvents(context.Context, string, string, uint64) ([]SessionEvent, error)
}

type AuditEvent struct {
	ID             string        `json:"id"`
	TenantID       string        `json:"tenant_id"`
	Channel        string        `json:"channel,omitempty"`
	UserID         string        `json:"user_id,omitempty"`
	SessionID      string        `json:"session_id,omitempty"`
	AgentName      string        `json:"agent_name,omitempty"`
	ToolName       string        `json:"tool_name,omitempty"`
	Decision       string        `json:"decision"`
	Latency        time.Duration `json:"latency"`
	ErrorType      string        `json:"error_type,omitempty"`
	Cost           float64       `json:"cost"`
	TraceID        string        `json:"trace_id"`
	RequestID      string        `json:"request_id,omitempty"`
	OccurredAt     time.Time     `json:"occurred_at"`
	PolicyRevision uint64        `json:"policy_revision,omitempty"`
	Checkpoint     string        `json:"checkpoint,omitempty"`
	Rule           string        `json:"rule,omitempty"`
	Reason         string        `json:"reason,omitempty"`
	Content        string        `json:"content,omitempty"`
}

type AuditSink interface {
	Record(context.Context, AuditEvent) error
}

var (
	ErrNotFound          = errors.New("platform: not found")
	ErrDuplicateEvent    = errors.New("platform: duplicate session event")
	ErrStaleFencingToken = errors.New("platform: stale fencing token")
)
