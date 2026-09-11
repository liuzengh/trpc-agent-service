package web

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

func TestConsoleBackendProfileLifecycle(t *testing.T) {
	handler := testConsoleHandler(t)

	list := handler.request(t, http.MethodGet, "/api/v1/backend-profiles", "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "platform-postgres") {
		t.Fatalf("initial backend list = %d: %s", list.Code, list.Body.String())
	}

	create := handler.request(t, http.MethodPost, "/api/v1/backend-profiles", "{\"profile_id\":\"tenant-s3-a\",\"display_name\":\"Tenant S3 A\",\"driver\":\"s3\",\"domains\":[\"artifact\"],\"connection_ref\":\"env:TENANT_S3_A\"}")
	if create.Code != http.StatusCreated {
		t.Fatalf("create backend = %d: %s", create.Code, create.Body.String())
	}

	immutable := handler.request(t, http.MethodPut, "/api/v1/backend-profiles/tenant-s3-a", "{\"display_name\":\"Tenant S3 A\",\"driver\":\"s3\",\"status\":\"active\"}")
	if immutable.Code != http.StatusBadRequest {
		t.Fatalf("immutable backend update = %d, want 400", immutable.Code)
	}

	update := handler.request(t, http.MethodPut, "/api/v1/backend-profiles/tenant-s3-a", "{\"display_name\":\"Tenant S3 Archive\",\"status\":\"active\"}")
	if update.Code != http.StatusOK || !strings.Contains(update.Body.String(), "Tenant S3 Archive") {
		t.Fatalf("update backend = %d: %s", update.Code, update.Body.String())
	}

	missingStatus := handler.request(t, http.MethodPut, "/api/v1/backend-profiles/tenant-s3-a", "{\"display_name\":\"Tenant S3 Archive\"}")
	if missingStatus.Code != http.StatusBadRequest {
		t.Fatalf("backend update without status = %d, want 400", missingStatus.Code)
	}

	deleteResponse := handler.request(t, http.MethodDelete, "/api/v1/backend-profiles/tenant-s3-a", "")
	if deleteResponse.Code != http.StatusNoContent {
		t.Fatalf("delete backend = %d: %s", deleteResponse.Code, deleteResponse.Body.String())
	}
	missing := handler.request(t, http.MethodDelete, "/api/v1/backend-profiles/tenant-s3-a", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("delete missing backend = %d, want 404", missing.Code)
	}
}

func TestConsoleTenantBackendPolicyRoundTripAndAuthorization(t *testing.T) {
	handler := testConsoleHandler(t)
	update := handler.request(t, http.MethodPut, "/api/v1/tenant-backend-policy?tenant=example", "{\"tenant_id\":\"example\",\"profile_ids\":[\"platform-postgres\"]}")
	if update.Code != http.StatusOK {
		t.Fatalf("update tenant backend policy = %d: %s", update.Code, update.Body.String())
	}
	var response tenantBackendPolicyResponse
	if err := json.Unmarshal(update.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode policy response: %v", err)
	}
	if response.TenantID != "example" || len(response.Profiles) != 1 || response.Profiles[0].ProfileID != "platform-postgres" {
		t.Fatalf("updated policy = %#v", response)
	}

	tenantAdmin := identity.SessionUser{PlatformUserID: "tenant-admin", Tenants: []identity.TenantRole{{
		TenantID: "example", Role: identity.RoleAdmin, Status: identity.TenantActive,
	}}}
	read := handler.requestAs(t, tenantAdmin, http.MethodGet, "/api/v1/tenant-backend-policy?tenant=example", "")
	if read.Code != http.StatusOK || !strings.Contains(read.Body.String(), "platform-postgres") {
		t.Fatalf("tenant admin policy read = %d: %s", read.Code, read.Body.String())
	}

	member := tenantMember(identity.TenantRole{TenantID: "example", Role: identity.RoleMember, Status: identity.TenantActive})
	forbidden := handler.requestAs(t, member, http.MethodGet, "/api/v1/tenant-backend-policy?tenant=example", "")
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("member policy read = %d, want 403", forbidden.Code)
	}

	conflict := handler.request(t, http.MethodPut, "/api/v1/tenant-backend-policy?tenant=example", "{\"tenant_id\":\"other\",\"profile_ids\":[\"platform-postgres\"]}")
	if conflict.Code != http.StatusBadRequest {
		t.Fatalf("tenant mismatch policy update = %d, want 400", conflict.Code)
	}
}

func TestConsoleBackendProfileEndpointsValidateAvailabilityAndMethods(t *testing.T) {
	unavailable := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.BackendProfiles = nil
	})
	if response := unavailable.request(t, http.MethodGet, "/api/v1/backend-profiles", ""); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured backend profile list = %d, want 503", response.Code)
	}
	if response := unavailable.request(t, http.MethodGet, "/api/v1/tenant-backend-policy?tenant=example", ""); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured tenant policy = %d, want 503", response.Code)
	}

	handler := testConsoleHandler(t)
	if response := handler.request(t, http.MethodPatch, "/api/v1/backend-profiles", "{}"); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("backend collection PATCH = %d, want 405", response.Code)
	}
	if response := handler.request(t, http.MethodGet, "/api/v1/backend-profiles/", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("empty backend profile ID = %d, want 400", response.Code)
	}
	if response := handler.request(t, http.MethodPost, "/api/v1/backend-profiles/platform-postgres", "{}"); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("backend item POST = %d, want 405", response.Code)
	}
	if response := handler.request(t, http.MethodGet, "/api/v1/tenant-backend-policy", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("tenant policy without tenant = %d, want 400", response.Code)
	}
}
