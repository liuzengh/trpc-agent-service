package gateway

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type contractBindings struct{ binding tenant.ExecutionBinding }

func (s contractBindings) ResolveExecutionBinding(context.Context, tenant.Context) (tenant.ExecutionBinding, error) {
	return s.binding, nil
}

type contractTasks struct {
	accepted bool
	request  PrepareDispatchRequest
}

func (s *contractTasks) PrepareDispatch(_ context.Context, request PrepareDispatchRequest) (PreparedDispatch, error) {
	s.request = request
	return PreparedDispatch{Accepted: s.accepted, Envelope: runtime.ExecutionEnvelope{TenantID: request.Tenant.TenantID, AgentAppID: request.Tenant.AgentAppID, RequestID: request.RequestID}}, nil
}
func (*contractTasks) GetExecution(context.Context, ExecutionKey) (ExecutionStatus, error) {
	panic("unexpected call")
}
func (*contractTasks) RequestCancel(context.Context, CancelRequest) (CancelResult, error) {
	panic("unexpected call")
}
func (*contractTasks) ParkInput(context.Context, ParkRequest) (ParkResult, error) {
	panic("unexpected call")
}

type contractExecutor struct{ calls int }

func (s *contractExecutor) Execute(context.Context, runtime.ExecutionEnvelope) error {
	s.calls++
	return nil
}

func TestDispatcherContractUsesTrustedBindingAndSharedPrepareSemantics(t *testing.T) {
	trusted := tenant.Context{TenantID: "tenant", TenantVersion: 2, AgentAppID: "app", SubjectID: "subject", Channel: "fake", TrustedSource: "binding:test"}
	binding := tenant.ExecutionBinding{AgentAppVersion: 3, AgentAppRevision: 4, AgentContentDigest: "digest", ConfigVersion: 5, PolicyVersion: 6}
	request := DispatchRequest{Tenant: trusted, RequestID: "request", SessionID: "session", UserID: "user", PayloadRef: "payload://request", TraceParent: "trace"}
	for _, test := range []struct {
		name string
		new  func(*contractTasks, *contractExecutor) Dispatcher
	}{
		{name: "local", new: func(tasks *contractTasks, executor *contractExecutor) Dispatcher {
			return LocalDispatcher{Tasks: tasks, Bindings: contractBindings{binding}, Executor: executor}
		}},
		{name: "broker", new: func(tasks *contractTasks, _ *contractExecutor) Dispatcher {
			return BrokerDispatcher{Tasks: tasks, Bindings: contractBindings{binding}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tasks := &contractTasks{}
			executor := &contractExecutor{}
			dispatcher := test.new(tasks, executor)
			handle, err := dispatcher.Dispatch(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if handle.RequestID != request.RequestID || handle.Status != string(runtime.OutcomeDenied) {
				t.Fatalf("handle=%#v", handle)
			}
			if tasks.request.Tenant != trusted || tasks.request.Binding != binding || tasks.request.RequestID != request.RequestID || tasks.request.PayloadRef != request.PayloadRef {
				t.Fatalf("prepared request=%#v", tasks.request)
			}
			if executor.calls != 0 {
				t.Fatalf("denied request executed %d times", executor.calls)
			}
		})
	}
}

func TestDispatcherContractRejectsUntrustedTenantBeforePrepare(t *testing.T) {
	for _, dispatcher := range []Dispatcher{
		LocalDispatcher{Tasks: &contractTasks{}, Bindings: contractBindings{}, Executor: &contractExecutor{}},
		BrokerDispatcher{Tasks: &contractTasks{}, Bindings: contractBindings{}},
	} {
		if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{}); err == nil {
			t.Fatal("untrusted request was accepted")
		}
	}
}
