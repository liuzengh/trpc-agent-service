package platform

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type contextRecordingStore struct {
	DataStore
	contexts []context.Context
}

func (s *contextRecordingStore) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	s.contexts = append(s.contexts, ctx)
	return s.DataStore.AppendSessionEvent(ctx, event)
}

type blockingFailureStore struct {
	DataStore
	entered chan struct{}
	once    sync.Once
}

type unavailableControlPlanePersistence struct{}

func (unavailableControlPlanePersistence) Load(context.Context) (controlPlaneSnapshot, int64, error) {
	return controlPlaneSnapshot{}, 0, errors.New("database unavailable")
}

func (unavailableControlPlanePersistence) Save(context.Context, controlPlaneSnapshot, int64) (int64, error) {
	return 0, errors.New("database unavailable")
}

func (unavailableControlPlanePersistence) Close() error { return nil }

func TestRoutedRunReportsControlPlaneUnavailableWhenBackendSelectionCannotLoad(t *testing.T) {
	platform := activeTestPlatform(t)
	platform.persistence = unavailableControlPlanePersistence{}
	handler := NewAdminHandler(platform, DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}}})
	defer handler.Close()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/run", strings.NewReader(`{"app_id":"app-one","session_id":"session-one","input":"hello"}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	assertAPIError(t, response.Result(), http.StatusServiceUnavailable, "control_plane_unavailable")
}

func (s *blockingFailureStore) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	if event.Type == "run.failed" {
		s.once.Do(func() { s.entered <- struct{}{} })
		<-ctx.Done()
		return ctx.Err()
	}
	return s.DataStore.AppendSessionEvent(ctx, event)
}

func TestFailureEventUsesBoundedServiceContext(t *testing.T) {
	store := &contextRecordingStore{DataStore: NewInMemoryStore()}
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}}})
	handler.ConfigureDataStore(store)
	handler.ConfigureRuntime(fakeRunner{err: errors.New("runner down")}, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/run", strings.NewReader(`{"app_id":"app-one","session_id":"session-one","input":"hello"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(store.contexts) != 2 {
		t.Fatalf("contexts=%d", len(store.contexts))
	}
	if _, bounded := store.contexts[1].Deadline(); !bounded {
		t.Fatal("failure event context is not bounded")
	}
}

func TestFailureEventContextCancelsOnHandlerClose(t *testing.T) {
	store := &blockingFailureStore{DataStore: NewInMemoryStore(), entered: make(chan struct{}, 1)}
	handler := NewAdminHandler(activeTestPlatform(t), DevelopmentIdentity{ID: "operator", Assignments: []TenantAssignment{{TenantID: "tenant-one", Role: RoleOperator}}})
	handler.ConfigureDataStore(store)
	handler.ConfigureRuntime(fakeRunner{err: errors.New("runner down")}, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/run", strings.NewReader(`{"app_id":"app-one","session_id":"session-one","input":"hello"}`))
	response := httptest.NewRecorder()
	requestDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(requestDone)
	}()

	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("failure event write did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- handler.Close() }()
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("failure event write did not observe shutdown cancellation")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler close did not finish")
	}
}
