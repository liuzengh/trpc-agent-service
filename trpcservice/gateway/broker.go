package gateway

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type BrokerDispatcher struct {
	Tasks    TaskStore
	Bindings BindingResolver
}

func (d BrokerDispatcher) Dispatch(ctx context.Context, in DispatchRequest) (ExecutionHandle, error) {
	if d.Tasks == nil || d.Bindings == nil {
		return ExecutionHandle{}, runtime.ErrInvariantViolation
	}
	if err := in.Tenant.Validate(); err != nil {
		return ExecutionHandle{}, err
	}
	binding, err := resolveBinding(ctx, d.Bindings, in.Tenant, in.ConfigVersion)
	if err != nil {
		return ExecutionHandle{}, err
	}
	prepared, err := d.Tasks.PrepareDispatch(ctx, PrepareDispatchRequest{
		Tenant: in.Tenant, Binding: binding, RequestID: in.RequestID, SessionID: in.SessionID,
		UserID: in.UserID, PayloadRef: in.PayloadRef, TraceParent: in.TraceParent,
	})
	if err != nil {
		return ExecutionHandle{}, err
	}
	if !prepared.Accepted {
		return ExecutionHandle{RequestID: in.RequestID, Status: string(runtime.OutcomeDenied)}, nil
	}
	return ExecutionHandle{RequestID: in.RequestID, Status: "accepted"}, nil
}

var _ Dispatcher = BrokerDispatcher{}
