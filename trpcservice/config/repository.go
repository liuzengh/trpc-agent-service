package config

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrNotFound        = errors.New("config snapshot not found")
	ErrInvalid         = errors.New("invalid config snapshot")
	ErrTenantScope     = errors.New("config tenant scope mismatch")
	ErrVersionConflict = errors.New("config version conflict")
	ErrReleaseNotFound = errors.New("config release not found")
	ErrReleaseConflict = errors.New("config release conflict")
)

type ValidateInput struct {
	TenantID string
	Payload  ConfigV1
}
type PublishInput struct {
	TenantID              string
	ExpectedTenantVersion int64
	Payload               ConfigV1
	Metadata              tenant.ChangeMetadata
}
type PublishResult struct {
	Snapshot Snapshot
	Tenant   tenant.Tenant
}
type RollbackInput struct {
	TenantID                             string
	ExpectedTenantVersion, TargetVersion int64
	Metadata                             tenant.ChangeMetadata
}

// Release is an immutable target set with a mutable, CAS-protected rollout
// percentage. A target is selected per tenant, never per user or session.
type Release struct {
	ReleaseID  string          `json:"release_id"`
	State      ReleaseState    `json:"state"`
	Percentage int             `json:"percentage"`
	Salt       string          `json:"salt"`
	Version    int64           `json:"version"`
	Targets    []ReleaseTarget `json:"targets"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

type ReleaseState string

const (
	ReleaseActive     ReleaseState = "active"
	ReleaseRolledBack ReleaseState = "rolled_back"
)

type ReleaseTarget struct {
	TenantID               string `json:"tenant_id"`
	BaselineConfigVersion  int64  `json:"baseline_config_version"`
	CandidateConfigVersion int64  `json:"candidate_config_version"`
	Allowlisted            bool   `json:"allowlisted"`
}

type StageInput struct {
	TenantID              string
	ExpectedTenantVersion int64
	Payload               ConfigV1
	Metadata              tenant.ChangeMetadata
}

type ReleaseCreateInput struct {
	ReleaseID  string                `json:"release_id"`
	Percentage int                   `json:"percentage"`
	Salt       string                `json:"salt"`
	Targets    []ReleaseTarget       `json:"targets"`
	Metadata   tenant.ChangeMetadata `json:"-"`
}

type ReleaseUpdateInput struct {
	ReleaseID       string
	ExpectedVersion int64
	Percentage      int
	Metadata        tenant.ChangeMetadata
}

type ReleaseRollbackInput struct {
	ReleaseID       string
	ExpectedVersion int64
	Metadata        tenant.ChangeMetadata
}

type Repository interface {
	Validate(context.Context, ValidateInput) error
	Publish(context.Context, PublishInput) (PublishResult, error)
	Stage(context.Context, StageInput) (Snapshot, error)
	Get(context.Context, string, int64) (Snapshot, error)
	GetCurrent(context.Context, string) (Snapshot, error)
	Rollback(context.Context, RollbackInput) (PublishResult, error)
	CreateRelease(context.Context, ReleaseCreateInput) (Release, error)
	GetRelease(context.Context, string) (Release, error)
	UpdateRelease(context.Context, ReleaseUpdateInput) (Release, error)
	RollbackRelease(context.Context, ReleaseRollbackInput) (Release, error)
	// SelectEffective returns the snapshot for a newly admitted request. The
	// baseline is supplied by verified channel routing; zero uses the tenant's
	// current active configuration.
	SelectEffective(context.Context, string, int64) (Snapshot, error)
	ResolveExecutionBinding(context.Context, tenant.Context) (tenant.ExecutionBinding, error)
	ResolveExecutionBindingAt(context.Context, tenant.Context, int64) (tenant.ExecutionBinding, error)
}
