// Package gateway contains ingress and task dispatch contracts.
package gateway

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Dispatcher interface {
	Dispatch(context.Context, DispatchRequest) (ExecutionHandle, error)
}

type Executor interface {
	Execute(context.Context, runtime.ExecutionEnvelope) error
}

type ExecutionHandle struct {
	RequestID   string
	Status      string
	ReplyCursor string
}

type DispatchRequest struct {
	Tenant      tenant.Context
	RequestID   string
	SessionID   string
	UserID      string
	PayloadRef  string
	TraceParent string
	// ConfigVersion is set only by a durable trusted ingress boundary. It
	// freezes a configuration selected when the callback was accepted.
	ConfigVersion int64
}
