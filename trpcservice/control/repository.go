package control

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Repository interface {
	Ready(context.Context) error
	InitializeTenants(context.Context, []tenant.Tenant, []string) error
	GetPolicy(context.Context, string) (TenantPolicy, error)
	PutPolicy(context.Context, TenantPolicy, int64) (TenantPolicy, error)
	GetPlacement(context.Context, string) (TenantPlacement, error)
	PutPlacement(context.Context, TenantPlacement, int64) (TenantPlacement, error)
	RegisterNode(context.Context, NodeRecord) error
	HeartbeatNode(context.Context, string, string, NodeState, int, time.Time) error
	GetNode(context.Context, string) (NodeRecord, error)
	ListNodes(context.Context) ([]NodeRecord, error)
	CreateAssignment(context.Context, NodeAssignment) (NodeAssignment, bool, error)
	GetAssignment(context.Context, string) (NodeAssignment, error)
	ListAssignments(context.Context) ([]NodeAssignment, error)
	PutAssignment(context.Context, NodeAssignment, int64) (NodeAssignment, error)
	AppendAudit(context.Context, AuditRecord) error
	QueryAudit(context.Context, AuditQuery) ([]AuditRecord, string, error)
	AppendMetric(context.Context, MetricEvent) error
	QueryMetrics(context.Context, MetricQuery) ([]MetricEvent, string, error)
	SetTenantDegraded(context.Context, string, string) error
	ClearTenantDegraded(context.Context, string) error
	CreateConfirmation(context.Context, Confirmation, time.Duration) error
	ApproveConfirmation(context.Context, string, string, string, string) (Confirmation, error)
	ConsumeConfirmation(context.Context, Confirmation) error
	AcquireLease(context.Context, string, string, time.Duration) (bool, error)
	RenewLease(context.Context, string, string, time.Duration) (bool, error)
	ReleaseLease(context.Context, string, string) error
	Close() error
}

// InitializingRepository makes Catalog initialization part of the safety-
// critical readiness check. It does not connect during construction, so HTTP
// liveness can start even while control Redis is unavailable.
type InitializingRepository struct {
	Repository
	tenants     []tenant.Tenant
	tools       []string
	initialize  sync.Mutex
	initialized bool
}

func NewInitializingRepository(repository Repository, tenants []tenant.Tenant, defaultTools []string) *InitializingRepository {
	return &InitializingRepository{
		Repository: repository,
		tenants:    append([]tenant.Tenant(nil), tenants...),
		tools:      append([]string{}, defaultTools...),
	}
}

func (r *InitializingRepository) Ready(ctx context.Context) error {
	if r == nil || r.Repository == nil {
		return ErrUnavailable
	}
	if err := r.Repository.Ready(ctx); err != nil {
		return err
	}
	r.initialize.Lock()
	defer r.initialize.Unlock()
	if r.initialized {
		return nil
	}
	if err := r.Repository.InitializeTenants(ctx, r.tenants, r.tools); err != nil {
		return err
	}
	r.initialized = true
	return nil
}
