// Package secret resolves tenant-scoped secret references.
package secret

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// SecretProvider resolves a secret only after the worker has established the
// tenant application scope. Implementations must not persist or log values.
type SecretProvider interface {
	ResolveSecret(ctx context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error)
}
