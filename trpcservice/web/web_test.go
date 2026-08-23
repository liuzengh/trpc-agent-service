package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/prometheus/client_golang/prometheus"
)

func TestAdminAPIControlPlaneLifecycle(t *testing.T) {
	profile := tenant.Tenant{ID: "tenant", Name: "Tenant", Enabled: true, Agent: tenant.AgentProfile{ID: "default", Version: "1"}}
	repository := store.NewMemoryRepository()
	if err := repository.SeedTenants(context.Background(), []tenant.Tenant{profile}); err != nil {
		t.Fatal(err)
	}
	server := New(config.Config{HTTPAddr: "127.0.0.1:0", Tenants: []tenant.Tenant{profile}}, repository, prometheus.NewRegistry(), func() ReadyStatus {
		return ReadyStatus{Ready: false, Role: "gateway"}
	})

	assertRequest(t, server, http.MethodGet, "/healthz", nil, http.StatusOK)
	assertRequest(t, server, http.MethodGet, "/readyz", nil, http.StatusServiceUnavailable)
	assertRequest(t, server, http.MethodGet, "/api/v1/tenants", nil, http.StatusOK)
	assertRequest(t, server, http.MethodPost, "/api/v1/agents", map[string]any{
		"tenant_id": "tenant", "id": "agent", "name": "Agent",
	}, http.StatusCreated)
	assertRequest(t, server, http.MethodPost, "/api/v1/agents/agent/versions", map[string]any{
		"tenant_id": "tenant", "version": "1.0.0", "profile": map[string]any{"instruction": "help"},
	}, http.StatusCreated)
	assertRequest(t, server, http.MethodPost, "/api/v1/agents/agent:publish", map[string]any{
		"tenant_id": "tenant", "version": "1.0.0",
	}, http.StatusOK)
	assertRequest(t, server, http.MethodPost, "/api/v1/channel-bindings", map[string]any{
		"tenant_id": "tenant", "binding": map[string]any{"id": "feishu", "type": "feishu", "credential_ref": "FEISHU", "enabled": true},
	}, http.StatusCreated)
	assertRequest(t, server, http.MethodPost, "/api/v1/backend-profiles", map[string]any{
		"tenant_id": "tenant", "id": "postgres", "backend": map[string]any{"session": "postgres"},
	}, http.StatusCreated)
	assertRequest(t, server, http.MethodPost, "/api/v1/agents/agent:publish", map[string]any{
		"tenant_id": "tenant", "version": "missing",
	}, http.StatusConflict)
}

func TestAdminAPIRejectsInvalidInput(t *testing.T) {
	repository := store.NewMemoryRepository()
	server := New(config.Config{}, repository, nil, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", bytes.NewBufferString(`{"unknown":true}`))
	server.mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	assertRequest(t, server, http.MethodPost, "/api/v1/agents", map[string]any{"id": "missing"}, http.StatusBadRequest)
	assertRequest(t, server, http.MethodPost, "/api/v1/channel-bindings", map[string]any{}, http.StatusBadRequest)
	assertRequest(t, server, http.MethodPost, "/api/v1/backend-profiles", map[string]any{}, http.StatusBadRequest)
}

func assertRequest(t *testing.T, server *Server, method, path string, body any, want int) {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	server.mux.ServeHTTP(recorder, request)
	if recorder.Code != want {
		t.Fatalf("%s %s status=%d want=%d body=%s", method, path, recorder.Code, want, recorder.Body.String())
	}
}
