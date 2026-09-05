package storage

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type CoordinationBackend string

const (
	BackendRedis    CoordinationBackend = "redis"
	BackendPostgres CoordinationBackend = "postgres"
)

type Epoch uint64

type OperationGuard struct {
	Backend    CoordinationBackend
	Epoch      Epoch
	OwnerID    string
	FenceToken uint64
}

type EpochAuthority interface {
	GetEpoch(context.Context, string, string) (Epoch, error)
	BumpEpoch(context.Context, string, string) (Epoch, error)
	ValidateEpoch(context.Context, string, string, Epoch) error
}

type Lease struct {
	TenantID   string
	SessionID  string
	ResourceID string
	OwnerID    string
	FenceToken uint64
	ExpiresAt  time.Time
	Backend    CoordinationBackend
	Epoch      Epoch
}

type SessionRepository interface {
	Get(context.Context, tenant.TenantContext, string) (session.Session, error)
	Create(context.Context, tenant.TenantContext, session.Session) error
	AppendEvent(context.Context, tenant.TenantContext, int64, session.SessionEvent) (session.Session, error)
	AcquireLease(context.Context, tenant.TenantContext, string, time.Duration) (Lease, error)
}

type ClaimStatus string

const (
	ClaimAcquired  ClaimStatus = "acquired"
	ClaimInFlight  ClaimStatus = "in_flight"
	ClaimCompleted ClaimStatus = "completed"
)

type DedupKey struct {
	TenantID          string
	Channel           string
	BindingID         string
	ExternalMessageID string
}

type Claim struct {
	Key         DedupKey
	Status      ClaimStatus
	OwnerID     string
	Attempt     int
	FenceToken  uint64
	ResponseRef string
	ClaimedAt   time.Time
	ExpiresAt   time.Time
	Backend     CoordinationBackend
	Epoch       Epoch
}

type IdempotencyRepository interface {
	Claim(context.Context, tenant.TenantContext, string, time.Duration) (Claim, error)
	Complete(context.Context, tenant.TenantContext, DedupKey, string, uint64) error
	Fail(context.Context, tenant.TenantContext, DedupKey, uint64, bool) error
}

type ClaimStore interface {
	Claim(context.Context, tenant.TenantContext, DedupKey, time.Duration, string) (Claim, error)
	Complete(context.Context, tenant.TenantContext, DedupKey, string, string, OperationGuard) error
	Fail(context.Context, tenant.TenantContext, DedupKey, string, OperationGuard, bool) error
}

type LeaseStore interface {
	Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (Lease, error)
	Renew(context.Context, tenant.TenantContext, Lease, time.Duration) (Lease, error)
	Release(context.Context, tenant.TenantContext, Lease) error
	Validate(context.Context, tenant.TenantContext, Lease) error
}

type MemoryRepository interface {
	Put(context.Context, tenant.TenantContext, memory.Memory) error
	Search(context.Context, tenant.TenantContext, string, int) ([]memory.Memory, error)
}

type SummaryRepository interface {
	UpsertIfNewer(context.Context, tenant.TenantContext, session.Summary) error
}

type ArtifactRepository interface {
	Create(context.Context, tenant.TenantContext, artifact.Artifact) error
	PresignedURL(context.Context, tenant.TenantContext, string, time.Duration) (string, error)
}

// ArtifactMetadataRepository is the production metadata boundary for an
// artifact whose bytes live in ObjectStore. It intentionally remains
// separate from AtomicCompletionCoordinator and never stores object bytes.
type ArtifactMetadataRepository interface {
	ArtifactRepository
	Get(context.Context, tenant.TenantContext, string) (artifact.Artifact, error)
	MarkReady(context.Context, tenant.TenantContext, string, ObjectInfo) error
	MarkFailed(context.Context, tenant.TenantContext, string) error
	Reconcile(context.Context, tenant.TenantContext, string) (artifact.Artifact, error)
}

// BindingMetadataRepository is the durable P1-04 configuration boundary.
type BindingMetadataRepository interface {
	CreateBinding(context.Context, tenant.TenantContext, tenant.ChannelBinding) error
	GetBinding(context.Context, tenant.TenantContext, string) (tenant.ChannelBinding, error)
	ResolveBinding(context.Context, string, string) (tenant.ChannelBinding, error)
	UpdateBinding(context.Context, tenant.TenantContext, tenant.ChannelBinding, int64) error
}

type IdentityMetadataRepository interface {
	ResolveIdentity(context.Context, tenant.TenantContext) (tenant.Identity, error)
}

type BindingAuditRepository interface {
	AppendBindingEvent(context.Context, tenant.TenantContext, BindingAuditEvent) error
}

type BindingAuditEvent struct {
	TenantID            string
	AuditID             string
	BindingID           string
	Channel             string
	Operation           string
	Success             bool
	ErrorType           string
	IdentityFingerprint string
	SecretFingerprint   string
	Version             int64
	CreatedAt           time.Time
}
type AuditRepository interface {
	Append(context.Context, tenant.TenantContext, audit.AuditLog) error
}

type OutboxStatus string

const (
	OutboxPending    OutboxStatus = "pending"
	OutboxProcessing OutboxStatus = "processing"
	OutboxCompleted  OutboxStatus = "completed"
	OutboxRetry      OutboxStatus = "retry"
	OutboxDead       OutboxStatus = "dead"
)

type OutboxMessage struct {
	TenantID    string
	ID          string
	Kind        string
	AggregateID string
	DedupKey    string
	Payload     []byte
	Status      OutboxStatus
	Attempt     int
	NextAttempt time.Time
	LockedBy    string
	LockedUntil time.Time
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type OutboxRepository interface {
	Enqueue(context.Context, tenant.TenantContext, OutboxMessage) error
	ClaimBatch(context.Context, tenant.TenantContext, string, int) ([]OutboxMessage, error)
	MarkCompleted(context.Context, tenant.TenantContext, string, string) error
	MarkRetry(context.Context, tenant.TenantContext, string, string, time.Time, string) error
	MoveToDLQ(context.Context, tenant.TenantContext, string, string, string) error
}
