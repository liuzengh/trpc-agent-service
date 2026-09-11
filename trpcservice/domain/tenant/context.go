package tenant

import "context"

type tenantKey struct{}

// WithTenantID injects the tenant id into the context, carrying it across
// Gateway -> Worker -> Runner boundaries.
func WithTenantID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, tenantKey{}, id)
}

// TenantIDFromContext extracts the tenant id from the context, reporting
// whether a non-empty value was present.
func TenantIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tenantKey{}).(string)
	return id, ok && id != ""
}
