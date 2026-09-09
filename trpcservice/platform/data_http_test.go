package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestMemoryPostReturnsPersistedRecord(t *testing.T) {
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RoleOperator}}})
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/api/v1/admin/memory/session-one", "application/json", bytes.NewBufferString(`{"key":"name","value":"Ada"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var created MemoryRecord
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated || created.ID == "" || created.UpdatedAt.IsZero() {
		t.Fatalf("created=%#v status=%d", created, response.StatusCode)
	}
}

func TestMigrationFailureUsesSanitizedMessage(t *testing.T) {
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	defer handler.Close()
	handler.ConfigureMigration("127.0.0.1:1", filepath.Join(t.TempDir(), "missing-parent", "destination.db"), filepath.Join(t.TempDir(), "checkpoint.json"))
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/api/v1/admin/migrations", "application/json", bytes.NewBufferString(`{"dry_run":false,"batch_size":1}`))
	if err != nil {
		t.Fatal(err)
	}
	var job migrationResult
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d job=%#v", response.StatusCode, job)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err = server.Client().Get(server.URL + "/api/v1/admin/migrations/" + job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if job.Status == "failed" {
			if job.Message != "migration failed" {
				t.Fatalf("message=%q", job.Message)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("migration did not fail: %#v", job)
}

func TestBackendSelectionFailureUsesSanitizedMessage(t *testing.T) {
	invalidPath := filepath.Join(t.TempDir(), "missing-parent", "store.db")
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	defer handler.Close()
	handler.ConfigureBackendCatalog("", invalidPath)
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/api/v1/admin/storage/backend", "application/json", bytes.NewBufferString(`{"backend":"sqlite"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var apiErr struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&apiErr); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadRequest || apiErr.Error.Code != "invalid_backend" {
		t.Fatalf("status=%d code=%q", response.StatusCode, apiErr.Error.Code)
	}
	if strings.Contains(apiErr.Error.Message, invalidPath) || apiErr.Error.Message != "backend is not available" {
		t.Fatalf("message=%q", apiErr.Error.Message)
	}
}

func TestMigrationIsRejectedAfterHandlerClose(t *testing.T) {
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	handler.ConfigureMigration("127.0.0.1:1", filepath.Join(t.TempDir(), "destination.db"), filepath.Join(t.TempDir(), "checkpoint.json"))
	server := httptest.NewServer(handler)
	defer server.Close()
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Post(server.URL+"/api/v1/admin/migrations", "application/json", bytes.NewBufferString(`{"dry_run":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var apiErr struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&apiErr); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || apiErr.Error.Code != "service_closing" {
		t.Fatalf("status=%d code=%q", response.StatusCode, apiErr.Error.Code)
	}
}

func TestDataManagementAPIUsesTrustedTenantAndBackendSelection(t *testing.T) {
	redisServer := miniredis.RunT(t)
	h := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "admin", Name: "Admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", TenantName: "A", Role: RolePlatformAdmin}, {TenantID: "tenant-b", TenantName: "B", Role: RoleViewer}}})
	h.ConfigureBackendCatalog(redisServer.Addr(), "")
	ts := httptest.NewServer(h)
	defer ts.Close()
	client := ts.Client()
	resp, err := client.Get(ts.URL + "/api/v1/admin/storage/backend")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health=%d", resp.StatusCode)
	}
	resp.Body.Close()
	body, _ := json.Marshal(map[string]string{"backend": "redis"})
	resp, err = client.Post(ts.URL+"/api/v1/admin/storage/backend", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("select=%d", resp.StatusCode)
	}
	resp.Body.Close()
	store, releaseStore, err := h.acquireStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseStore()
	if err := store.AppendSessionEvent(context.Background(), SessionEvent{TenantID: "tenant-a", SessionID: "s", IdempotencyKey: "k", Type: "message", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	resp, err = client.Get(ts.URL + "/api/v1/admin/sessions/s/events")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp, err = client.Post(ts.URL+"/api/v1/admin/storage/backend", "application/json", bytes.NewReader([]byte(`{"backend":"redis"}`)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestMigrationAPIReportsProgressAndCompletion(t *testing.T) {
	redisServer := miniredis.RunT(t)
	source := NewRedisStore(redisServer.Addr())
	if err := source.AppendSessionEvent(context.Background(), SessionEvent{TenantID: "tenant-a", SessionID: "s", IdempotencyKey: "one", Type: "message", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	source.Close()
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	defer handler.Close()
	handler.ConfigureMigration(redisServer.Addr(), filepath.Join(t.TempDir(), "destination.db"), filepath.Join(t.TempDir(), "checkpoint.json"))
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/api/v1/admin/migrations", "application/json", bytes.NewBufferString(`{"dry_run":false,"batch_size":1}`))
	if err != nil {
		t.Fatal(err)
	}
	var job migrationResult
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || job.Status != "running" {
		t.Fatalf("job=%#v status=%d", job, response.StatusCode)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err = server.Client().Get(server.URL + "/api/v1/admin/migrations/" + job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if job.Status == "completed" {
			if job.ProcessedSessions != 1 || job.SourceCount != job.DestinationCount {
				t.Fatalf("job=%#v", job)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("migration did not complete: %#v", job)
}

func TestMigrationAPICutsOverSelectionAndKeepsRedisReadOnly(t *testing.T) {
	redisServer := miniredis.RunT(t)
	destinationPath := filepath.Join(t.TempDir(), "destination.db")
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	control := NewInMemoryControlPlane()
	selection := backendSelection{Backend: "redis", Address: redisServer.Addr()}
	if err := control.saveBackendSelection(context.Background(), "tenant-a", selection); err != nil {
		t.Fatal(err)
	}
	source := NewRedisStore(redisServer.Addr())
	if err := source.AppendSessionEvent(context.Background(), SessionEvent{TenantID: "tenant-a", SessionID: "s", IdempotencyKey: "one", Type: "message", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	source.Close()
	handler := NewAdminHandler(control, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	defer handler.Close()
	handler.ConfigureBackendCatalog(redisServer.Addr(), destinationPath)
	handler.ConfigureMigration(redisServer.Addr(), destinationPath, checkpointPath)
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/api/v1/admin/migrations", "application/json", bytes.NewBufferString(`{"dry_run":false,"batch_size":1,"cutover":true}`))
	if err != nil {
		t.Fatal(err)
	}
	var job migrationResult
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("start status=%d job=%#v", response.StatusCode, job)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err = server.Client().Get(server.URL + "/api/v1/admin/migrations/" + job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if job.Status == "failed" {
			t.Fatalf("migration failed: %#v", job)
		}
		if job.Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "completed" || !job.Matched {
		t.Fatalf("migration did not complete: %#v", job)
	}
	selections, err := control.loadBackendSelections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := selections["tenant-a"]; got.Backend != "sqlite" || got.Address != destinationPath || got.MigrationID != "" {
		t.Fatalf("cutover selection=%#v", got)
	}
	retired := NewRedisStore(redisServer.Addr())
	defer retired.Close()
	if err := retired.PutMemory(context.Background(), MemoryRecord{TenantID: "tenant-a", SessionID: "s", Key: "late", Value: "write"}); !errors.Is(err, ErrTenantMigrating) {
		t.Fatalf("retired Redis accepted write: %v", err)
	}
	store, release, err := handler.acquireStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	events, err := store.ListSessionEvents(context.Background(), "tenant-a", "s", 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("destination events=%#v err=%v", events, err)
	}
}

func TestMigrationAPIRecoversPersistentCrashLock(t *testing.T) {
	redisServer := miniredis.RunT(t)
	control := NewInMemoryControlPlane()
	selection := backendSelection{Backend: "redis", Address: redisServer.Addr()}
	if err := control.saveBackendSelection(context.Background(), "tenant-a", selection); err != nil {
		t.Fatal(err)
	}
	const migrationID = "migration-crashed"
	if err := control.beginBackendMigration(context.Background(), "tenant-a", migrationID, selection); err != nil {
		t.Fatal(err)
	}
	redisStore := NewRedisStore(redisServer.Addr())
	if _, err := redisStore.freezeMigration(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	redisStore.Close()
	handler := NewAdminHandler(control, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	defer handler.Close()
	handler.ConfigureMigration(redisServer.Addr(), filepath.Join(t.TempDir(), "destination.db"), filepath.Join(t.TempDir(), "checkpoint.json"))
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/api/v1/admin/migrations/" + migrationID)
	if err != nil {
		t.Fatal(err)
	}
	var state migrationResult
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || state.Status != "recovery_required" {
		t.Fatalf("recovery state=%#v status=%d", state, response.StatusCode)
	}
	request, err := http.NewRequest(http.MethodDelete, server.URL+"/api/v1/admin/migrations/"+migrationID, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("recovery status=%d", response.StatusCode)
	}
	redisStore = NewRedisStore(redisServer.Addr())
	defer redisStore.Close()
	if err := redisStore.PutMemory(context.Background(), MemoryRecord{TenantID: "tenant-a", SessionID: "s", Key: "after", Value: "recovered"}); err != nil {
		t.Fatal(err)
	}
	selections, err := control.loadBackendSelections(context.Background())
	if err != nil || selections["tenant-a"].MigrationID != "" {
		t.Fatalf("selection=%#v err=%v", selections["tenant-a"], err)
	}
}
