package gateway_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	gateway "github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaymemory "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type controlPrincipalResolver struct{ principal gateway.Principal }

func (r controlPrincipalResolver) Resolve(*http.Request) (gateway.Principal, error) {
	return r.principal, nil
}

func controlFixture(t *testing.T) (*gatewaymemory.TaskStore, runtime.ExecutionEnvelope) {
	t.Helper()
	store := gatewaymemory.NewTaskStore()
	prepared, err := store.PrepareDispatch(context.Background(), gateway.PrepareDispatchRequest{
		Tenant:    tenant.Context{TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", SubjectID: "subject", Channel: "test", TrustedSource: "test"},
		Binding:   tenant.ExecutionBinding{AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1},
		RequestID: "request-1", SessionID: "session-1", UserID: "user-1", PayloadRef: "payload-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, prepared.Envelope
}

func TestControlHandlerStatusAndDurableCancel(t *testing.T) {
	store, envelope := controlFixture(t)
	h := gateway.ControlHandler{Tasks: store, Principals: controlPrincipalResolver{principal: gateway.Principal{
		Authenticated: true, TenantID: "tenant-a", SubjectID: "operator", CanRead: true, CanCancel: true,
	}}}
	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/agent-runs/"+envelope.RequestID, nil))
	if get.Code != http.StatusOK {
		t.Fatalf("status code=%d", get.Code)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/agent-runs/"+envelope.RequestID+"/cancel", bytes.NewBufferString(`{"expected_version":0,"reason_code":"user_requested"}`))
	request.Header.Set("traceparent", "00-test")
	cancel := httptest.NewRecorder()
	h.ServeHTTP(cancel, request)
	if cancel.Code != http.StatusAccepted {
		t.Fatalf("cancel code=%d", cancel.Code)
	}
	status, err := store.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: "tenant-a", RequestID: envelope.RequestID})
	if err != nil || !status.CancelRequested || status.Outcome != runtime.OutcomeQueued {
		t.Fatalf("status=%#v err=%v", status, err)
	}
}

func TestControlHandlerFailsClosedOnAuthAndTenantScope(t *testing.T) {
	store, envelope := controlFixture(t)
	unauthenticated := gateway.ControlHandler{Tasks: store, Principals: controlPrincipalResolver{principal: gateway.Principal{}}}
	rec := httptest.NewRecorder()
	unauthenticated.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/agent-runs/"+envelope.RequestID, nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated code=%d", rec.Code)
	}
	forbidden := gateway.ControlHandler{Tasks: store, Principals: controlPrincipalResolver{principal: gateway.Principal{Authenticated: true, TenantID: "tenant-a", SubjectID: "operator", CanRead: true}}}
	rec = httptest.NewRecorder()
	forbidden.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-b/agent-runs/"+envelope.RequestID, nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant code=%d", rec.Code)
	}
}
