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

func TestAdminHandlerEnforcesTenantRBAC(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	service, _ := New(repository)
	const tenantToken = "tenant-admin-token-12345678901234567890"
	handler, err := NewHandlerWithPrincipals(service, []Principal{{
		Name: "tutorial-admin", Token: tenantToken,
		Role: RoleTenantAdmin, TenantIDs: []string{"tutorial-tenant"},
	}})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	allowed := httptest.NewRequest(http.MethodPost, "/admin/apps", bytes.NewBufferString(`{
        "app_id":"rbac-app","tenant_id":"tutorial-tenant","name":"RBAC App"
    }`))
	allowed.Header.Set("Authorization", "Bearer "+tenantToken)
	allowedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(allowedRecorder, allowed)
	if allowedRecorder.Code != http.StatusCreated {
		t.Fatalf("allowed status=%d body=%s", allowedRecorder.Code, allowedRecorder.Body.String())
	}
	denied := httptest.NewRequest(http.MethodPost, "/admin/apps", bytes.NewBufferString(`{
        "app_id":"other-app","tenant_id":"other-tenant","name":"Other App"
    }`))
	denied.Header.Set("Authorization", "Bearer "+tenantToken)
	deniedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deniedRecorder, denied)
	if deniedRecorder.Code != http.StatusForbidden {
		t.Fatalf("denied status=%d body=%s", deniedRecorder.Code, deniedRecorder.Body.String())
	}
}
