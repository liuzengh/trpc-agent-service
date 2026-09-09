// Package storagemigration implements resumable tenant storage backfills.
package storagemigration

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Domain identifies one independently routed runtime data family.
type Domain string

const (
	DomainSession   Domain = "session"
	DomainMemory    Domain = "memory"
	DomainArtifact  Domain = "artifact"
	DomainKnowledge Domain = "knowledge"
)

// Status is the durable migration lifecycle.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusRetrying  Status = "retrying"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

var (
	ErrConflict = errors.New("storage migration: job already exists")
	ErrNotFound = errors.New("storage migration: job not found")
	ErrClaim    = errors.New("storage migration: stale claim")
)

// Job contains SecretRefs but never resolved credential values. API metadata
// intentionally projects only identity, counters, status, and timestamps.
type Job struct {
	TenantID, JobID, AppID string
	ConfigVersion          tenant.ConfigVersion
	Domain                 Domain
	Source, Target         tenant.BackendConfig
	SourceRouteHash        string
	Status                 Status
	Checkpoint             []byte
	SourceRows, CopiedRows int64
	Attempts               int
	ClaimOwner, ClaimToken string
	LeaseUntil             time.Time
	LastErrorType          string
	CreatedBy              string
	CreatedAt, UpdatedAt   time.Time
	CompletedAt            *time.Time
}

// Progress advances exactly one claimed batch.
type Progress struct {
	Checkpoint             []byte
	SourceRows, CopiedRows int64
	Done                   bool
}

// Store is the multi-node job and checkpoint contract.
type Store interface {
	Create(context.Context, Job) (Job, error)
	List(context.Context, string) ([]Job, error)
	Get(context.Context, string, string) (Job, error)
	Claim(context.Context, string, time.Duration) (Job, bool, error)
	Save(context.Context, Job, Progress) error
	Fail(context.Context, Job, error, time.Time, int) error
	Cancel(context.Context, string, string) error
}

func validDomain(domain Domain) bool {
	return domain == DomainSession || domain == DomainMemory || domain == DomainArtifact || domain == DomainKnowledge
}
