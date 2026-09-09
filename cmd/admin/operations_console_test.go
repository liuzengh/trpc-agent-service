package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
)

func TestOperationsConsoleUsesExistingPrincipalAndPermissions(t *testing.T) {
	const bootstrap = "bootstrap-console-authentication-test-only"
	const auditor = "auditor-console-authentication-test-only"
	auth, err := adminauth.NewAuthenticator(bootstrap, `[{"id":"reader","token":"`+auditor+`","role":"auditor","tenantIds":["tenant-b","tenant-a"]}]`)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	registerOperationsConsole(mux, nil, auth, health.NewCoordinator())
	for _, test := range []struct {
		path, method, token string
		status              int
	}{
		{"/console/", http.MethodGet, "", http.StatusOK},
		{"/api/v1/operations/me", http.MethodGet, "", http.StatusUnauthorized},
		{"/api/v1/operations/me", http.MethodGet, auditor, http.StatusOK},
		{"/api/v1/operations/me", http.MethodPost, bootstrap, http.StatusMethodNotAllowed},
		{"/api/v1/operations/apps?tenantId=tenant-a", http.MethodGet, "", http.StatusUnauthorized},
		{"/api/v1/operations/apps?tenantId=tenant-c", http.MethodGet, auditor, http.StatusForbidden},
	} {
		r := httptest.NewRequest(test.method, test.path, nil)
		if test.token != "" {
			r.Header.Set("Authorization", "Bearer "+test.token)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != test.status {
			t.Fatalf("%s %s status %d, want %d: %s", test.method, test.path, w.Code, test.status, w.Body.String())
		}
		if strings.HasPrefix(test.path, "/api/") && w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("operations response must not be cached")
		}
		if strings.Contains(w.Body.String(), bootstrap) || strings.Contains(w.Body.String(), auditor) {
			t.Fatal("credential escaped into response")
		}
		if test.path == "/api/v1/operations/me" && w.Code == http.StatusOK {
			var body struct {
				ID          string   `json:"id"`
				Role        string   `json:"role"`
				TenantIDs   []string `json:"tenantIds"`
				Permissions []string `json:"permissions"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.ID != "reader" || body.Role != "auditor" || len(body.Permissions) != 1 || body.Permissions[0] != "tenant.read" || strings.Join(body.TenantIDs, ",") != "tenant-a,tenant-b" {
				t.Fatalf("identity does not match authenticated scope: %+v", body)
			}
		}
	}
}
