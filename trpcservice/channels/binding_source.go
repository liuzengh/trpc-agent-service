package channels

import "context"

// BindingSource supplies authoritative bindings to long-connection adapters.
// Implementations must preserve the exact tenant/application/binding scope.
type BindingSource interface {
	ResolveBinding(ctx context.Context, tenantID, appID, bindingID string) (Binding, error)
	ListActiveChannelBindings(ctx context.Context, channel Channel) ([]Binding, error)
}

// ConnectionStatusReporter is an optional control-plane capability used by
// long-connection adapters to publish safe readiness metadata.
type ConnectionStatusReporter interface {
	RecordChannelConnection(context.Context, string, string, string, ConnectionStatus, error) error
}
