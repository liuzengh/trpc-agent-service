package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/lifecycle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
)

func TestStage1HandlerServesPackagedConsoleAndKeepsStage0Endpoints(t *testing.T) {
	admin := platform.NewAdminHandler(platform.NewInMemoryControlPlane(), platform.DevelopmentIdentity{
		ID: "developer", Assignments: []platform.TenantAssignment{{TenantID: "tenant-a", Role: platform.RolePlatformAdmin}},
	})
	handler := NewStage1Handler(platform.EchoRunner{}, platform.TenantContext{TenantID: "baseline"}, lifecycle.New(), admin)
	for _, path := range []string{"/", "/deployments"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `<div id="root"></div>`) {
			t.Fatalf("GET %s = %d, body %q", path, response.Code, response.Body.String())
		}
	}
	for _, path := range []string{"/healthz", "/version"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s = %d", path, response.Code)
		}
	}
}
