package gateway_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type readinessStub struct{ ready bool }

func (r readinessStub) Ready() bool { return r.ready }

func TestNewHTTPHandlerRejectsPartialComposition(t *testing.T) {
	if _, err := gateway.NewHTTPHandler(gateway.HTTPHandlerOptions{}); err != runtime.ErrInvariantViolation {
		t.Fatalf("partial composition err=%v", err)
	}
	ready := readinessStub{ready: true}
	if !ready.Ready() {
		t.Fatal("readiness stub broken")
	}
}

func TestHTTPHandlerGatesNewRunsWhileDraining(t *testing.T) {
	fixture := newRunFixture()
	codec, err := gateway.NewCursorCodec([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := gateway.NewHTTPHandler(gateway.HTTPHandlerOptions{Submitter: fixture.submitter, Tasks: fixture.tasks,
		Events: &terminalBridgeStore{}, Principals: controlPrincipalResolver{principal: gateway.Principal{Authenticated: true,
			TenantID: "tenant-a", SubjectID: "principal-a", CanRun: true}}, Routes: staticRunRoute{route: fixture.route}, Cursors: codec,
		Readiness: readinessStub{}, MaxBody: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/agent-runs", strings.NewReader(`{"agent_app_id":"app","text":"hello"}`))
	request.Header.Set("Idempotency-Key", "draining-key")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining code=%d body=%q", response.Code, response.Body.String())
	}
}
