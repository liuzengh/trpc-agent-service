package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestTaskStoreContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewTaskStore().PrepareDispatch(ctx, gateway.PrepareDispatchRequest{Tenant: tenant.Context{}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestTaskStoreCancelIsDurableIntentNotPrematureTerminal(t *testing.T) {
	store := NewTaskStore()
	prepared, err := store.PrepareDispatch(context.Background(), gateway.PrepareDispatchRequest{
		Tenant:  tenant.Context{TenantID: "tenant", TenantVersion: 1, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "test"},
		Binding: gatewayTestBinding(), RequestID: "request", SessionID: "session", UserID: "user", PayloadRef: "payload://request",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.RequestCancel(context.Background(), gateway.CancelRequest{TenantID: "tenant", RequestID: "request", ExpectedVersion: 0, ActorID: "user", ReasonCode: "requested"})
	if err != nil || !result.Accepted || result.Version != 1 || result.CancelVersion != 1 {
		t.Fatalf("cancel=%#v err=%v", result, err)
	}
	status, err := store.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: "tenant", RequestID: "request"})
	if err != nil || status.Outcome != runtime.OutcomeQueued || !status.CancelRequested || status.CancelVersion != 1 {
		t.Fatalf("status=%#v err=%v", status, err)
	}
	retry, err := store.RequestCancel(context.Background(), gateway.CancelRequest{TenantID: "tenant", RequestID: "request", ExpectedVersion: 0, ActorID: "user", ReasonCode: "requested"})
	if err != nil || !retry.Accepted || retry.Version != result.Version || retry.CancelVersion != result.CancelVersion {
		t.Fatalf("cancel retry=%#v err=%v", retry, err)
	}
	if prepared.Envelope.RequestID != "request" {
		t.Fatalf("prepared=%#v", prepared)
	}
}

func gatewayTestBinding() tenant.ExecutionBinding {
	return tenant.ExecutionBinding{AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1}
}

func TestTaskStoreFreezesBindingBudgetInEnvelope(t *testing.T) {
	store := NewTaskStore()
	binding := gatewayTestBinding()
	binding.ExecutionBudget = runtime.ExecutionBudget{MaxLLMCalls: 2, ExecutionTimeoutSeconds: 30}
	prepared, err := store.PrepareDispatch(context.Background(), gateway.PrepareDispatchRequest{
		Tenant:  tenant.Context{TenantID: "tenant", TenantVersion: 1, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "test"},
		Binding: binding, RequestID: "budget-request", SessionID: "session", UserID: "user", PayloadRef: "payload://budget-request",
	})
	if err != nil || prepared.Envelope.ExecutionBudget != binding.ExecutionBudget {
		t.Fatalf("prepared=%#v err=%v", prepared, err)
	}
}
