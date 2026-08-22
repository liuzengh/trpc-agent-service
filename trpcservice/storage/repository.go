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

type Lease struct {
	SessionID  string
	OwnerID    string
	FenceToken uint64
	ExpiresAt  time.Time
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
}

type IdempotencyRepository interface {
	Claim(context.Context, tenant.TenantContext, string, time.Duration) (Claim, error)
	Complete(context.Context, tenant.TenantContext, DedupKey, string, uint64) error
	Fail(context.Context, tenant.TenantContext, DedupKey, uint64, bool) error
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
