package controlplane

import (
	"context"
	"errors"
)

// ErrNotFound is returned when a tenant-scoped control-plane object is absent.
var ErrNotFound = errors.New("control-plane object not found")
var ErrConflict = errors.New("control-plane version conflict")

// Repository provides the read model needed by Gateway and Worker routing.
// Administrative mutations are added through a separate service so request
// paths cannot accidentally modify published revisions.
type Repository interface {
	GetTenant(ctx context.Context, tenantID string) (Tenant, error)
	GetAgentApp(ctx context.Context, tenantID string, appID string) (AgentApp, error)
	GetRevision(ctx context.Context, tenantID string, revisionID string) (AgentRevision, error)
	GetStableRevision(ctx context.Context, tenantID string, appID string) (AgentRevision, error)
	GetChannelBinding(ctx context.Context, tenantID string, bindingID string) (ChannelBinding, error)
	GetChannelBindingByCallbackKey(ctx context.Context, callbackKey string) (ChannelBinding, error)
	ListBackendBindings(ctx context.Context, tenantID string, appID string) ([]BackendBinding, error)
	GetBackendBinding(ctx context.Context, tenantID string, bindingID string) (BackendBinding, error)
	GetBackendMigration(ctx context.Context, tenantID string, migrationID string) (BackendMigration, error)
	GetActiveBackendMigration(ctx context.Context, tenantID string, appID string, resourceType string) (BackendMigration, error)
	AdjustBackendMigrationRepair(ctx context.Context, tenantID string, migrationID string, delta int64) error
	Ready(ctx context.Context) error
	Close() error
}

// MutableRepository is used only by the authenticated Admin service.
type MutableRepository interface {
	Repository
	CreateTenant(ctx context.Context, tenant Tenant) error
	CreateAgentApp(ctx context.Context, app AgentApp) error
	CreateRevision(ctx context.Context, revision AgentRevision) error
	PublishRevision(
		ctx context.Context,
		tenantID string,
		appID string,
		revisionID string,
		expectedVersion int64,
	) (AgentApp, error)
	UpdateRolloutPolicy(
		ctx context.Context,
		tenantID string,
		appID string,
		rolloutPolicy []byte,
		expectedVersion int64,
	) (AgentApp, error)
	CreateChannelBinding(ctx context.Context, binding ChannelBinding) error
	UpdateChannelBinding(
		ctx context.Context,
		tenantID string,
		bindingID string,
		config []byte,
		status string,
		expectedVersion int64,
	) (ChannelBinding, error)
	CreateBackendBinding(ctx context.Context, binding BackendBinding) error
	CreateBackendMigration(ctx context.Context, migration BackendMigration) error
	TransitionBackendMigration(
		ctx context.Context,
		tenantID string,
		migrationID string,
		nextState string,
		expectedVersion int64,
		checkpoint []byte,
		verification []byte,
	) (BackendMigration, error)
}
