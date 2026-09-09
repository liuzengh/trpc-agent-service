package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type closeTrackingStore struct {
	DataStore
	closed chan struct{}
}

func (s *closeTrackingStore) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func TestBackendSelectionWaitsForAcquiredLease(t *testing.T) {
	tracked := &closeTrackingStore{DataStore: NewInMemoryStore(), closed: make(chan struct{})}
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	defer handler.Close()
	if err := handler.selectBackend(context.Background(), "tenant-a", backendSelection{Backend: "inmemory"}, tracked); err != nil {
		t.Fatal(err)
	}
	_, release, err := handler.acquireStore(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/api/v1/admin/storage/backend", "application/json", bytes.NewReader([]byte(`{"backend":"inmemory"}`)))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
	select {
	case <-tracked.closed:
		t.Fatal("backend closed while a request lease was held")
	default:
	}
	release()
	select {
	case <-tracked.closed:
	case <-time.After(time.Second):
		t.Fatal("retired backend was not closed after lease release")
	}
}

func TestConcurrentBackendSelectionsPersistOneFinalSelection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backends.json")
	handler := NewAdminHandler(nil, DevelopmentIdentity{ID: "admin", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RolePlatformAdmin}}})
	defer handler.Close()
	if err := handler.ConfigureBackendSelections(path); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			selection := backendSelection{Backend: "inmemory"}
			if i == 0 {
				selection.Address = "first"
			}
			if err := handler.selectBackend(context.Background(), "tenant-a", selection, NewInMemoryStore()); err != nil {
				t.Errorf("selectBackend: %v", err)
			}
		}(i)
	}
	wg.Wait()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]backendSelection
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	selection, ok := persisted["tenant-a"]
	if !ok || selection.Backend != "inmemory" {
		t.Fatalf("persisted=%#v", persisted)
	}
	handler.mu.Lock()
	live := handler.backends.selections["tenant-a"]
	handler.mu.Unlock()
	if live != selection {
		t.Fatalf("live=%#v persisted=%#v", live, selection)
	}
}
