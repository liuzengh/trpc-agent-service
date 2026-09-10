package tenant

import "context"

// Repository is the narrow control-plane contract needed by the first
// implementation. Implementations must keep tenant_id as an explicit scope.
type Repository interface {
	Create(context.Context, CreateInput) (*Tenant, error)
	Get(context.Context, string) (*Tenant, error)
	UpdateConfiguration(context.Context, UpdateConfigurationInput) (*Tenant, error)
	TransitionStatus(context.Context, TransitionStatusInput) (*Tenant, StatusChangeEvent, error)
}
