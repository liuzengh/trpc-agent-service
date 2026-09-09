package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestAdminRequiresBasicAuth(t *testing.T) {
	handler := newTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if response.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("WWW-Authenticate header is missing")
	}
}

func TestTenantCRUDRoutes(t *testing.T) {
	handler := newTestHandler(t)
	value := testTenant()

	response := serveJSON(t, handler, http.MethodPost, "/api/v1/tenants", value)
	if response.Code != http.StatusOK {
		t.Fatalf("POST tenant status = %d, body = %s", response.Code, response.Body.String())
	}

	request := authorizedRequest(t, http.MethodGet, "/api/v1/tenants/tenant-a", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET tenant status = %d, body = %s", response.Code, response.Body.String())
	}
	var got tenant.Tenant
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode tenant response: %v", err)
	}
	if got.ID != value.ID {
		t.Fatalf("tenant ID = %q, want %q", got.ID, value.ID)
	}
}

func TestAppBindingAndActivationRoutes(t *testing.T) {
	store := tenant.NewMemoryStore()
	cache := &recordingCache{}
	handler, err := NewHandler(store, cache, "admin", "test-password")
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	if err := store.UpsertTenant(context.Background(), testTenant()); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	app := testApp()
	response := serveJSON(t, handler, http.MethodPost, "/api/v1/tenants/tenant-a/apps", app)
	if response.Code != http.StatusOK {
		t.Fatalf("POST app status = %d, body = %s", response.Code, response.Body.String())
	}
	var storedApp tenant.AgentApp
	if err := json.NewDecoder(response.Body).Decode(&storedApp); err != nil {
		t.Fatalf("decode app response: %v", err)
	}
	if storedApp.Version != 1 {
		t.Fatalf("app version = %d, want 1", storedApp.Version)
	}

	binding := testBinding()
	response = serveJSON(t, handler, http.MethodPost, "/api/v1/apps/app-a/bindings", binding)
	if response.Code != http.StatusOK {
		t.Fatalf("POST binding status = %d, body = %s", response.Code, response.Body.String())
	}
	if cache.invalidations != 1 {
		t.Fatalf("cache invalidations = %d, want 1", cache.invalidations)
	}

	request := authorizedRequest(t, http.MethodPost, "/api/v1/apps/app-a/versions/1/activate", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("activate app status = %d, body = %s", response.Code, response.Body.String())
	}
}

// TestActivateAppInvalidatesCachedSnapshots kills the R2 attack: activating a
// new app version must evict the cached data-plane snapshot so the next
// request resolves the new version instead of the stale one.
func TestActivateAppInvalidatesCachedSnapshots(t *testing.T) {
	store := tenant.NewMemoryStore()
	cache, err := tenant.NewConfigCache(store, time.Minute)
	if err != nil {
		t.Fatalf("NewConfigCache() error = %v", err)
	}
	handler, err := NewHandler(store, cache, "admin", "test-password")
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	ctx := context.Background()
	if err := store.UpsertTenant(ctx, testTenant()); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := store.UpsertApp(ctx, testApp()); err != nil {
		t.Fatalf("seed app v1: %v", err)
	}
	if err := store.UpsertBinding(ctx, testBinding()); err != nil {
		t.Fatalf("seed binding: %v", err)
	}

	// Warm the cache with version 1.
	snapshot, err := cache.ResolveBinding(ctx, "webui", "binding-a")
	if err != nil || snapshot.App.Version != 1 {
		t.Fatalf("warm ResolveBinding() = v%d, %v; want v1", snapshot.App.Version, err)
	}

	// Publish and activate version 2 with a different app name.
	appV2 := testApp()
	appV2.AppName = "tenant-a-support-v2"
	if _, err := store.UpsertApp(ctx, appV2); err != nil {
		t.Fatalf("seed app v2: %v", err)
	}
	request := authorizedRequest(t, http.MethodPost, "/api/v1/apps/app-a/versions/2/activate", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("activate status = %d, body = %s", response.Code, response.Body.String())
	}

	snapshot, err = cache.ResolveBinding(ctx, "webui", "binding-a")
	if err != nil {
		t.Fatalf("ResolveBinding() error = %v", err)
	}
	if snapshot.App.Version != 2 || snapshot.App.AppName != "tenant-a-support-v2" {
		t.Fatalf("post-activate snapshot = v%d %q, want v2 tenant-a-support-v2",
			snapshot.App.Version, snapshot.App.AppName)
	}
}

func TestAdminRejectsUnknownJSONField(t *testing.T) {
	handler := newTestHandler(t)
	body := bytes.NewBufferString(`{"tenant_id":"tenant-a","name":"A","unexpected":true}`)
	request := authorizedRequest(t, http.MethodPost, "/api/v1/tenants", body)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestMigrationRoutes(t *testing.T) {
	migrator := &fakeMigrator{status: storage.MigrationStatus{
		ID: "migration-1", AppID: "app-a", FromBackend: "redis",
		ToBackend: "mysql", Phase: storage.PhasePrepared,
	}}
	handler, err := NewHandler(
		tenant.NewMemoryStore(), nil, "admin", "test-password", WithMigrator(migrator),
	)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	response := serveJSON(
		t,
		handler,
		http.MethodPost,
		"/api/v1/apps/app-a/migrations",
		map[string]string{"from": "redis", "to": "mysql"},
	)
	if response.Code != http.StatusOK || migrator.startCalls != 1 {
		t.Fatalf("start migration response = %d %s", response.Code, response.Body.String())
	}
	request := authorizedRequest(
		t, http.MethodPost, "/api/v1/migrations/migration-1/advance", nil,
	)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || migrator.status.Phase != storage.PhaseDualWrite {
		t.Fatalf("advance response = %d %s", response.Code, response.Body.String())
	}
	request = authorizedRequest(
		t, http.MethodPost, "/api/v1/migrations/migration-1/rollback", nil,
	)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || migrator.status.Phase != storage.PhaseRolledBack {
		t.Fatalf("rollback response = %d %s", response.Code, response.Body.String())
	}
}

func TestStartMigrationConflictReturns409(t *testing.T) {
	migrator := &fakeMigrator{startErr: fmt.Errorf(
		"%w: migration source backend %q does not match the app's effective session backend %q",
		storage.ErrMigrationConflict, "redis", "mysql",
	)}
	handler, err := NewHandler(
		tenant.NewMemoryStore(), nil, "admin", "test-password", WithMigrator(migrator),
	)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	response := serveJSON(
		t,
		handler,
		http.MethodPost,
		"/api/v1/apps/app-a/migrations",
		map[string]string{"from": "redis", "to": "mysql"},
	)
	if response.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409, body = %s", response.Code, response.Body.String())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte("effective session backend")) {
		t.Fatalf("conflict body = %s", response.Body.String())
	}
}

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	handler, err := NewHandler(tenant.NewMemoryStore(), nil, "admin", "test-password")
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func serveJSON(t *testing.T, handler http.Handler, method, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	request := authorizedRequest(t, method, path, bytes.NewReader(data))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func authorizedRequest(t *testing.T, method, path string, body io.Reader) *http.Request {
	t.Helper()
	var request *http.Request
	if body == nil {
		request = httptest.NewRequest(method, path, nil)
	} else {
		request = httptest.NewRequest(method, path, body)
	}
	request.SetBasicAuth("admin", "test-password")
	return request
}

type recordingCache struct {
	invalidations int
}

type fakeMigrator struct {
	status     storage.MigrationStatus
	startCalls int
	startErr   error
}

func (f *fakeMigrator) Start(context.Context, string, string, string) (string, error) {
	f.startCalls++
	if f.startErr != nil {
		return "", f.startErr
	}
	return f.status.ID, nil
}
func (f *fakeMigrator) Advance(context.Context, string) (storage.MigrationPhase, error) {
	f.status.Phase = storage.PhaseDualWrite
	return f.status.Phase, nil
}
func (f *fakeMigrator) Rollback(context.Context, string) error {
	f.status.Phase = storage.PhaseRolledBack
	return nil
}
func (f *fakeMigrator) Status(context.Context, string) (storage.MigrationStatus, error) {
	return f.status, nil
}
func (f *fakeMigrator) Resume(context.Context) error { return nil }

func (c *recordingCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return tenant.Snapshot{}, tenant.ErrNotFound
}

func (c *recordingCache) Invalidate(string, string) {
	c.invalidations++
}

func testTenant() tenant.Tenant {
	return tenant.Tenant{
		ID:       "tenant-a",
		Name:     "Tenant A",
		IsActive: true,
		Quota: tenant.Quota{
			DailyTokenLimit: 1000,
			RatePerMinute:   30,
		},
		Policy: tenant.Policy{AuditLevel: "full"},
	}
}

func testApp() tenant.AgentApp {
	return tenant.AgentApp{
		ID:       "app-a",
		TenantID: "tenant-a",
		AppName:  "tenant-a-support",
		Model: tenant.ModelConfig{
			Provider:  "openai-compatible",
			Model:     "test-model",
			APIKeyRef: "env:MODEL_KEY_TENANT_A",
		},
		Tools: []string{"search"},
		Backends: tenant.BackendSelection{
			Session: "redis",
			Memory:  "pgvector",
		},
	}
}

func testBinding() tenant.ChannelBinding {
	return tenant.ChannelBinding{
		ID:       "binding-a",
		TenantID: "tenant-a",
		AppID:    "app-a",
		Channel:  "webui",
		RouteKey: "binding-a",
		Config:   map[string]string{"session_key": "env:WEBUI_SESSION_KEY_TENANT_A"},
		IsActive: true,
	}
}
