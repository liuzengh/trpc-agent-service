package profile

import (
	"context"

	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

// SessionServiceResolver resolves the official trpc-agent-go Session service
// for one exact, immutable execution profile. It belongs to the Profile
// boundary because the snapshot, rather than a mutable tenant pointer, is the
// routing authority for the lifetime of a turn.
type SessionServiceResolver interface {
	Resolve(context.Context, ExecutionProfileSnapshot) (agentsession.Service, error)
}
