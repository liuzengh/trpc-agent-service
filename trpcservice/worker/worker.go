// Package worker assembles and executes tenant-specific Agent runners.
package worker

import (
	"context"

	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// Event is the Worker view of a reply.Event. Channel adapters must import
// package reply instead of this package.
type Event = reply.Event

// Executor runs one debounced batch while the caller owns the session lock.
type Executor interface {
	Execute(
		context.Context,
		tenant.Snapshot,
		string,
		[]storage.UserEvent,
	) (<-chan Event, error)
}
