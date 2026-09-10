package web

import (
	"encoding/json"
	"net/http"
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
}
