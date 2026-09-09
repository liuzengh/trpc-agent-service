package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/console"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/operations"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

// Static assets contain no tenant data. All data requests use the same
// authenticated, scoped control-plane routes as non-browser clients.
func registerOperationsConsole(mux *http.ServeMux, db *sql.DB, auth *adminauth.Authenticator, shutdown *health.Coordinator) {
	mux.Handle("/console/", console.NewHandler())
	mux.HandleFunc("/console", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, "/console/", http.StatusTemporaryRedirect)
	})
	protect := func(name string, next http.Handler) http.Handler {
		return noStoreAdminResponses(auth.Middleware(adminauth.RequireMethods(
			map[string]adminauth.Permission{http.MethodGet: adminauth.PermissionTenantRead},
			shutdown.Middleware(telemetry.HTTPMiddleware(name, next)),
		)))
	}
	mux.Handle("/api/v1/operations/me", protect("admin.operations.identity", http.HandlerFunc(operationsIdentity)))
	mux.Handle("/api/v1/operations/", protect("admin.operations.read", operations.NewHandler(db)))
}

func operationsIdentity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	principal, err := adminauth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAdminAuthorizationError(w, err)
		return
	}
	permissions := make([]adminauth.Permission, 0)
	for _, permission := range []adminauth.Permission{
		adminauth.PermissionTenantRead, adminauth.PermissionTenantCreate,
		adminauth.PermissionTenantWrite, adminauth.PermissionTenantDelete,
		adminauth.PermissionAgentWrite, adminauth.PermissionAgentPublish,
		adminauth.PermissionAgentDeploy, adminauth.PermissionToolApprovalRead,
		adminauth.PermissionToolApprovalGrant, adminauth.PermissionExecutionReconcile,
		adminauth.PermissionOutboxReplay,
	} {
		if principal.Has(permission) {
			permissions = append(permissions, permission)
		}
	}
	tenantIDs := make([]string, 0, len(principal.TenantIDs()))
	for id := range principal.TenantIDs() {
		tenantIDs = append(tenantIDs, id)
	}
	sort.Strings(tenantIDs)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		ID          string                 `json:"id"`
		Role        adminauth.Role         `json:"role"`
		TenantIDs   []string               `json:"tenantIds"`
		Permissions []adminauth.Permission `json:"permissions"`
	}{principal.ID, principal.Role, tenantIDs, permissions})
}
