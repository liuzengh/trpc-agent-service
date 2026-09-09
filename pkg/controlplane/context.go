package controlplane

import "context"

// normalizeContext keeps public control-plane boundaries total for callers
// that accidentally pass a nil context. database/sql and callback
// implementations require a non-nil context; a background context preserves
// the legacy no-deadline behavior without panicking at the boundary.
func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
