package controlplane

import (
	"context"
	"errors"
)

// ErrNotFound is returned when a tenant-scoped control-plane object is absent.
var ErrNotFound = errors.New("control-plane object not found")

// Repository provides the read model needed by Gateway and Worker routing.
// Administrative mutations are added through a separate service so request
// paths cannot accidentally modify published revisions.
type Repository interface {
	GetTenant(ctx context.Context, tenantID string) (Tenant, error)
	GetAgentApp(ctx context.Context, tenantID string, appID string) (AgentApp, error)
	GetRevision(ctx context.Context, tenantID string, revisionID string) (AgentRevision, error)
	GetStableRevision(ctx context.Context, tenantID string, appID string) (AgentRevision, error)
	GetChannelBindingByCallbackKey(ctx context.Context, callbackKey string) (ChannelBinding, error)
	ListBackendBindings(ctx context.Context, tenantID string, appID string) ([]BackendBinding, error)
	Ready(ctx context.Context) error
	Close() error
}
