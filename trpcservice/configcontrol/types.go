package configcontrol

import (
	"context"
	"errors"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

const NotifyChannel = "trpc_config_control"

type ReleaseKind string

const (
	ReleaseFull     ReleaseKind = "full"
	ReleaseCanary   ReleaseKind = "canary"
	ReleasePromote  ReleaseKind = "promote"
	ReleaseRollback ReleaseKind = "rollback"
)

type ReleaseStatus string

const (
	ReleasePreparing     ReleaseStatus = "preparing"
	ReleaseActivePending ReleaseStatus = "active_pending_ack"
	ReleaseVerified      ReleaseStatus = "verified"
	ReleaseFailed        ReleaseStatus = "failed"
	ReleaseCancelled     ReleaseStatus = "cancelled"
)

type NodeAckStatus string

const (
	NodePending  NodeAckStatus = "pending"
	NodePrepared NodeAckStatus = "prepared"
	NodeApplied  NodeAckStatus = "applied"
	NodeFailed   NodeAckStatus = "failed"
)

var (
	ErrRevisionNotFound   = errors.New("config control: revision not found")
	ErrRevisionConflict   = errors.New("config control: revision conflict")
	ErrGenerationConflict = errors.New("config control: generation conflict")
	ErrReleaseConflict    = errors.New("config control: release conflict")
	ErrInvalidTransition  = errors.New("config control: invalid release transition")
	ErrNodeNotRegistered  = errors.New("config control: node is not registered for release")
	ErrReleaseNotFound    = errors.New("config control: release not found")
	ErrTenantNotFound     = errors.New("config control: tenant state not found")
	ErrControlUnavailable = errors.New("config control: persistent store unavailable")
)

type Revision struct {
	TenantID     string              `json:"tenant_id"`
	Revision     string              `json:"revision"`
	Tenant       config.TenantConfig `json:"config"`
	ConfigSHA256 string              `json:"config_sha256"`
	CreatedBy    string              `json:"created_by,omitempty"`
	ChangeReason string              `json:"change_reason,omitempty"`
	CreatedAt    time.Time           `json:"created_at"`
}

type RevisionInput struct {
	TenantID     string
	Revision     string
	Tenant       config.TenantConfig
	CreatedBy    string
	ChangeReason string
}

type RevisionSummary struct {
	TenantID     string    `json:"tenant_id"`
	Revision     string    `json:"revision"`
	ConfigSHA256 string    `json:"config_sha256"`
	CreatedBy    string    `json:"created_by,omitempty"`
	ChangeReason string    `json:"change_reason,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type TenantState struct {
	TenantID       string    `json:"tenant_id"`
	ActiveRevision string    `json:"active_revision"`
	CanaryRevision string    `json:"canary_revision,omitempty"`
	RolloutPercent int       `json:"rollout_percent"`
	Generation     int64     `json:"generation"`
	UpdatedBy      string    `json:"updated_by,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type ReleaseNode struct {
	ReleaseID        string        `json:"release_id"`
	NodeID           string        `json:"node_id"`
	BootID           string        `json:"boot_id"`
	Status           NodeAckStatus `json:"status"`
	LoadedRevision   string        `json:"loaded_revision,omitempty"`
	LoadedGeneration int64         `json:"loaded_generation,omitempty"`
	ErrorMessage     string        `json:"error_message,omitempty"`
	PreparedAt       *time.Time    `json:"prepared_at,omitempty"`
	AppliedAt        *time.Time    `json:"applied_at,omitempty"`
	UpdatedAt        time.Time     `json:"updated_at"`
}

type Release struct {
	ReleaseID            string        `json:"release_id"`
	TenantID             string        `json:"tenant_id"`
	Kind                 ReleaseKind   `json:"kind"`
	SourceActiveRevision string        `json:"source_active_revision"`
	SourceCanaryRevision string        `json:"source_canary_revision,omitempty"`
	TargetRevision       string        `json:"target_revision"`
	TargetRolloutPercent int           `json:"target_rollout_percent"`
	ExpectedGeneration   int64         `json:"expected_generation"`
	ResultingGeneration  int64         `json:"resulting_generation,omitempty"`
	Status               ReleaseStatus `json:"status"`
	RequestedBy          string        `json:"requested_by,omitempty"`
	ChangeReason         string        `json:"change_reason,omitempty"`
	ErrorMessage         string        `json:"error_message,omitempty"`
	CreatedAt            time.Time     `json:"created_at"`
	ActivatedAt          *time.Time    `json:"activated_at,omitempty"`
	VerifiedAt           *time.Time    `json:"verified_at,omitempty"`
	FailedAt             *time.Time    `json:"failed_at,omitempty"`
	Nodes                []ReleaseNode `json:"nodes,omitempty"`
}

type CreateReleaseRequest struct {
	TenantID             string
	Kind                 ReleaseKind
	TargetRevision       string
	TargetRolloutPercent int
	ExpectedGeneration   int64
	ExpectedActive       string
	ExpectedCanary       string
	RequestedBy          string
	ChangeReason         string
}

type NodeIdentity struct {
	NodeID string
	BootID string
}

type NodeHeartbeat struct {
	NodeID           string    `json:"node_id"`
	BootID           string    `json:"boot_id"`
	LastSeenAt       time.Time `json:"last_seen_at"`
	Ready            bool      `json:"ready"`
	LoadedGeneration int64     `json:"loaded_generation"`
	ActiveRevision   string    `json:"active_revision,omitempty"`
	CanaryRevision   string    `json:"canary_revision,omitempty"`
	ErrorMessage     string    `json:"error_message,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Store is the durable control-plane contract. Implementations must make
// CreateRelease's generation check and state transition transactional. The
// in-memory implementation intentionally mirrors these semantics for tests
// and local/demo mode only.
type Store interface {
	Bootstrap(context.Context, []config.TenantConfig, string, string) error
	EnsureTenant(context.Context, config.TenantConfig, string, string) (TenantState, error)
	PutRevision(context.Context, RevisionInput) (Revision, error)
	GetRevision(context.Context, string, string) (Revision, error)
	ListRevisions(context.Context, string) ([]RevisionSummary, error)
	GetState(context.Context, string) (TenantState, error)
	ListStates(context.Context) ([]TenantState, error)
	CreateRelease(context.Context, CreateReleaseRequest) (Release, error)
	GetRelease(context.Context, string) (Release, error)
	ListReleases(context.Context, string) ([]Release, error)
	ListPendingReleases(context.Context) ([]Release, error)
	AckPrepared(context.Context, string, NodeIdentity, string, int64, error) error
	ActivateIfReady(context.Context, string) (Release, error)
	AckApplied(context.Context, string, NodeIdentity, string, int64, error) error
	FailRelease(context.Context, string, string) error
	Heartbeat(context.Context, NodeHeartbeat) error
	ListHeartbeats(context.Context) ([]NodeHeartbeat, error)
	Close()
}

// NotificationSource is optional. PostgreSQL implementations use a dedicated
// LISTEN connection for prompt refresh; Controller polling remains the
// correctness fallback when the connection is unavailable or reconnecting.
type NotificationSource interface {
	Listen(context.Context, func()) error
}

// ConfigLoader is the small surface used by the controller to publish a
// revision into the process-local registry without coupling this package to
// the HTTP or worker layers.
type ConfigLoader interface {
	PublishControlState(TenantState, config.TenantConfig, *config.TenantConfig) error
	SetConfigReady(bool)
}
