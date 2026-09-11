package web

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestConsoleCreatesBackendProfileAsActive(t *testing.T) {
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.BackendProfiles = storage.NewMemoryBackendProfileStore()
	})
	withStatus := handler.request(t, http.MethodPost, "/api/v1/backend-profiles", `{
		"profile_id":"session-redis-a",
		"display_name":"会话 Redis A",
		"driver":"redis",
		"domains":["session"],
		"connection_ref":"env:SESSION_REDIS_URL",
		"status":"disabled"
	}`)
	if withStatus.Code != http.StatusBadRequest {
		t.Fatalf("create backend with status = %d, want 400", withStatus.Code)
	}

	recorder := handler.request(t, http.MethodPost, "/api/v1/backend-profiles", `{
		"profile_id":"session-redis-a",
		"display_name":"会话 Redis A",
		"driver":"redis",
		"domains":["session"],
		"connection_ref":"env:SESSION_REDIS_URL"
	}`)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create backend status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var created storage.BackendProfile
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created backend: %v", err)
	}
	if created.Status != storage.BackendProfileActive {
		t.Fatalf("created backend status = %q, want active", created.Status)
	}
	if !created.Capabilities.MultiNode || !created.Capabilities.MemoryConsoleBrowsing {
		t.Fatalf("created backend capabilities = %#v", created.Capabilities)
	}
}

func TestConsoleBackendDriverCatalogAndDomainValidation(t *testing.T) {
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.BackendProfiles = storage.NewMemoryBackendProfileStore()
	})
	catalog := handler.request(t, http.MethodGet, "/api/v1/backend-drivers", "")
	if catalog.Code != http.StatusOK ||
		!strings.Contains(catalog.Body.String(), `"driver":"inmemory"`) ||
		!strings.Contains(catalog.Body.String(), `"domains":["session","memory","artifact"]`) ||
		!strings.Contains(catalog.Body.String(), `"memory_console_browsing":true`) {
		t.Fatalf("backend driver catalog = %d: %s", catalog.Code, catalog.Body.String())
	}

	missingDomains := handler.request(t, http.MethodPost, "/api/v1/backend-profiles", `{
		"profile_id":"postgres-session-only",
		"display_name":"会话 PostgreSQL",
		"driver":"postgres"
	}`)
	if missingDomains.Code != http.StatusBadRequest {
		t.Fatalf("create backend without domains = %d, want 400: %s", missingDomains.Code, missingDomains.Body.String())
	}

	unsupportedDomain := handler.request(t, http.MethodPost, "/api/v1/backend-profiles", `{
		"profile_id":"redis-files",
		"display_name":"错误 Redis",
		"driver":"redis",
		"domains":["artifact"],
		"connection_ref":"env:REDIS_URL"
	}`)
	if unsupportedDomain.Code != http.StatusBadRequest {
		t.Fatalf("create Redis artifact backend = %d, want 400: %s", unsupportedDomain.Code, unsupportedDomain.Body.String())
	}
}
