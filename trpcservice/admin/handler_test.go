package admin

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

const testAdminToken = "123456789012345678901234"

func TestAdminHandlerRequiresAuthorizationAndCreatesTenant(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.BootstrapData{})
	service, _ := New(repository)
	handler, _ := NewHandler(service, testAdminToken)

	unauthorized := httptest.NewRequest(http.MethodPost, "/admin/tenants", bytes.NewBufferString(`{}`))
	unauthorizedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorizedRecorder.Code)
	}

	request := httptest.NewRequest(http.MethodPost, "/admin/tenants", bytes.NewBufferString(`{
        "tenant_id":"tenant-a",
        "display_name":"Tenant A",
        "region":"local",
        "secret_namespace":"tenant/a"
    }`))
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if _, err := repository.GetTenant(request.Context(), "tenant-a"); err != nil {
		t.Fatalf("tenant not stored: %v", err)
	}
}
