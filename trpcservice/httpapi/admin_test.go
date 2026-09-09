package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAdminOperationalLookupsAreAuthorizedAndBounded(t *testing.T) {
	task := func(context.Context, string, string) (map[string]any, error) {
		return map[string]any{"task_id": "task", "state": "processing", "attempt": 2, "error_code": "", "node_id": "worker", "assignment_state": "running", "persist_attempt": 1}, nil
	}
	outbound := func(context.Context, string) (map[string]any, error) {
		return map[string]any{"task_id": "task", "state": "retry_wait", "attempt": 1, "error_code": "send_failed"}, nil
	}
	reconciler := func(context.Context) (map[string]any, error) {
		return map[string]any{"is_leader": true, "node_id": "gateway"}, nil
	}
	tasks := func(context.Context, int) ([]AdminTask, error) {
		return []AdminTask{{TaskID: "task", TenantID: "tenant", BindingID: "binding", State: "processing"}}, nil
	}
	handler := adminHandlerWithLookups(nil, nil, control.NewAdminAuthenticator("secret"), task, tasks, outbound, reconciler, tenant.Catalog{})
	for _, path := range []string{"/api/v1/admin/tasks", "/api/v1/admin/tasks/binding/message", "/api/v1/admin/outbound/task", "/api/v1/admin/reconciler"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"payload", "raw_payload", "redis_key", "text", "credential"} {
			if strings.Contains(rec.Body.String(), forbidden) {
				t.Fatalf("%s leaked forbidden field %q", path, forbidden)
			}
		}
	}
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/admin/reconciler", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}
}

func TestAdminCatalogIsReadOnlyAndOmitsCredentialReferences(t *testing.T) {
	catalog := tenant.Catalog{
		Tenants: []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "wecom-a", Channel: "wecom_aibot", ExternalAccountID: "bot-a",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
			BotIDRef: "env:BOT_ID", BotSecretRef: "env:BOT_SECRET",
		}},
	}
	handler := adminHandlerWithLookups(nil, nil, control.NewAdminAuthenticator("secret"), nil, nil, nil, nil, catalog)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/catalog", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("catalog status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, forbidden := range []string{"credential_ref", "bot_id_ref", "bot_secret_ref", "BOT_SECRET"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("catalog leaked forbidden field %q", forbidden)
		}
	}
	if !strings.Contains(rec.Body.String(), `"id":"wecom-a"`) {
		t.Fatalf("catalog omitted binding summary: %s", rec.Body.String())
	}
}

func TestAdminUIContainsDashboardViews(t *testing.T) {
	rec := httptest.NewRecorder()
	adminUIHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("admin UI status=%d", rec.Code)
	}
	for _, label := range []string{"租户", "Bindings", "任务", "最近消息", "审计"} {
		if !strings.Contains(rec.Body.String(), label) {
			t.Fatalf("admin UI omitted %q", label)
		}
	}
}
