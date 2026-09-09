package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

func TestHandlerPublishListConflictAndScope(t *testing.T) {
	service, _ := NewService(repository.NewMemoryStore())
	handler, _ := NewHandler(service)
	call := func(method, path string, body []byte) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, strings.NewReader(string(body)))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := call(http.MethodPost, "/v1/tenants/tenant-a/configs/validate", tenantYAML("tenant-a", 1)); response.Code != http.StatusOK {
		t.Fatalf("validate=%d %s", response.Code, response.Body.String())
	}
	if response := call(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", tenantYAML("tenant-a", 1)); response.Code != http.StatusCreated || strings.Contains(response.Body.String(), "config_yaml") {
		t.Fatalf("publish=%d %s", response.Code, response.Body.String())
	}
	if response := call(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=0", tenantYAML("tenant-a", 1)); response.Code != http.StatusConflict {
		t.Fatalf("conflict=%d %s", response.Code, response.Body.String())
	}
	if response := call(http.MethodGet, "/v1/tenants/tenant-b/configs", nil); response.Code != http.StatusOK || response.Body.String() != "[]\n" {
		t.Fatalf("cross tenant list=%d %s", response.Code, response.Body.String())
	}
	if response := call(http.MethodPost, "/v1/tenants/tenant-b/configs/validate", tenantYAML("tenant-a", 1)); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("scope=%d %s", response.Code, response.Body.String())
	}
}
