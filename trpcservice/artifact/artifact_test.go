package artifact

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestServiceLoadArtifactRejectsSessionMismatch(t *testing.T) {
	storage := &fakeStorage{}
	metadata := &fakeMetadata{}
	service := newTestService(t, storage, metadata)

	_, err := service.LoadArtifact(context.Background(), frameworkartifact.SessionInfo{
		AppName:   testAppName(t, testScope()),
		UserID:    "other-user",
		SessionID: "session-1",
	}, "report.txt", nil)
	if err == nil {
		t.Fatal("LoadArtifact succeeded for another user")
	}
	if metadata.findCalls != 0 {
		t.Fatalf("metadata lookup calls = %d, want 0", metadata.findCalls)
	}
	if storage.loadCalls != 0 {
		t.Fatalf("storage load calls = %d, want 0", storage.loadCalls)
	}
}

func TestServiceLoadArtifactRejectsUnregisteredMetadata(t *testing.T) {
	storage := &fakeStorage{}
	metadata := &fakeMetadata{findErr: ErrNotFound}
	service := newTestService(t, storage, metadata)

	_, err := service.LoadArtifact(context.Background(), testSessionInfo(t, testScope()), "report.txt", nil)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadArtifact error = %v, want ErrNotFound", err)
	}
	if storage.loadCalls != 0 {
		t.Fatalf("storage load calls = %d, want 0", storage.loadCalls)
	}
}

func TestServiceListArtifactKeysUsesMetadata(t *testing.T) {
	storage := &fakeStorage{}
	metadata := &fakeMetadata{keys: []string{"first.txt", "second.txt"}}
	service := newTestService(t, storage, metadata)

	got, err := service.ListArtifactKeys(context.Background(), testSessionInfo(t, testScope()))
	if err != nil {
		t.Fatalf("ListArtifactKeys: %v", err)
	}
	if !reflect.DeepEqual(got, metadata.keys) {
		t.Fatalf("ListArtifactKeys = %v, want %v", got, metadata.keys)
	}
	if storage.listKeysCalls != 0 {
		t.Fatalf("storage list keys calls = %d, want 0", storage.listKeysCalls)
	}
}

func TestServiceRejectsUserNamespace(t *testing.T) {
	storage := &fakeStorage{}
	metadata := &fakeMetadata{}
	service := newTestService(t, storage, metadata)

	if _, err := service.SaveArtifact(
		context.Background(),
		testSessionInfo(t, testScope()),
		"user:profile.txt",
		&frameworkartifact.Artifact{},
	); err == nil {
		t.Fatal("SaveArtifact succeeded for the user namespace")
	}
	if storage.saveCalls != 0 {
		t.Fatalf("storage save calls = %d, want 0", storage.saveCalls)
	}
	if metadata.reserveCalls != 0 {
		t.Fatalf("metadata reserve calls = %d, want 0", metadata.reserveCalls)
	}
}

func TestServiceSaveArtifactUsesTenantScopedStorageAppName(t *testing.T) {
	storage := &fakeStorage{}
	metadata := &fakeMetadata{}
	service := newTestService(t, storage, metadata)

	if _, err := service.SaveArtifact(
		context.Background(),
		testSessionInfo(t, testScope()),
		"report.txt",
		&frameworkartifact.Artifact{},
	); err != nil {
		t.Fatalf("SaveArtifact: %v", err)
	}
	if got, want := storage.savedInfo.AppName, testAppName(t, testScope()); got != want {
		t.Fatalf("storage app name = %q, want %q", got, want)
	}

	otherScope := tenant.Scope{TenantID: "tenant-2", AppID: "app-2"}
	if got, want := storage.savedInfo.AppName, testAppName(t, otherScope); got == want {
		t.Fatalf("storage app name = %q, collides with other scope", got)
	}
}

func TestServiceSaveArtifactEncodesStoragePathComponents(t *testing.T) {
	storage := &fakeStorage{}
	metadata := &fakeMetadata{}
	firstAccess := Access{
		Scope:              testScope(),
		ConfigVersion:      "v1",
		SessionPrincipalID: "a/b",
		SessionID:          "c",
	}
	secondAccess := Access{
		Scope:              testScope(),
		ConfigVersion:      "v1",
		SessionPrincipalID: "a",
		SessionID:          "b/c",
	}
	firstService, err := NewService(storage, metadata, firstAccess)
	if err != nil {
		t.Fatalf("NewService first: %v", err)
	}
	secondService, err := NewService(storage, metadata, secondAccess)
	if err != nil {
		t.Fatalf("NewService second: %v", err)
	}
	firstInfo := frameworkartifact.SessionInfo{
		AppName:   testAppName(t, testScope()),
		UserID:    firstAccess.SessionPrincipalID,
		SessionID: firstAccess.SessionID,
	}
	secondInfo := frameworkartifact.SessionInfo{
		AppName:   testAppName(t, testScope()),
		UserID:    secondAccess.SessionPrincipalID,
		SessionID: secondAccess.SessionID,
	}
	if _, err := firstService.SaveArtifact(
		context.Background(), firstInfo, "dir/report.txt", &frameworkartifact.Artifact{},
	); err != nil {
		t.Fatalf("save first artifact: %v", err)
	}
	if _, err := secondService.SaveArtifact(
		context.Background(), secondInfo, "dir/report.txt", &frameworkartifact.Artifact{},
	); err != nil {
		t.Fatalf("save second artifact: %v", err)
	}
	if len(storage.savedRequests) != 2 {
		t.Fatalf("storage requests = %d, want 2", len(storage.savedRequests))
	}
	firstRequest := storage.savedRequests[0]
	secondRequest := storage.savedRequests[1]
	if firstRequest.info.UserID == secondRequest.info.UserID &&
		firstRequest.info.SessionID == secondRequest.info.SessionID {
		t.Fatal("distinct session components map to the same storage components")
	}
	for _, value := range []string{
		firstRequest.info.UserID,
		firstRequest.info.SessionID,
		firstRequest.filename,
	} {
		if strings.Contains(value, "/") {
			t.Fatalf("storage path component %q contains a separator", value)
		}
	}
}

func TestServiceSaveArtifactReportsAbandonFailure(t *testing.T) {
	metadataErr := errors.New("metadata unavailable")
	abandonErr := errors.New("abandon unavailable")
	storage := &fakeStorage{}
	metadata := &fakeMetadata{createErr: metadataErr, abandonErr: abandonErr}
	service := newTestService(t, storage, metadata)

	_, err := service.SaveArtifact(
		context.Background(), testSessionInfo(t, testScope()), "report.txt", &frameworkartifact.Artifact{},
	)
	if !errors.Is(err, metadataErr) || !errors.Is(err, abandonErr) {
		t.Fatalf("SaveArtifact error = %v, want metadata and abandon errors", err)
	}
	if metadata.abandonCalls != 1 {
		t.Fatalf("abandon calls = %d, want 1", metadata.abandonCalls)
	}
	if storage.deleteVersionCalls != 1 {
		t.Fatalf("compensation delete version calls = %d, want 1", storage.deleteVersionCalls)
	}
}

func TestExecutionResolverBindsArtifactServiceToTrustedSession(t *testing.T) {
	storage := &fakeStorage{}
	metadata := &fakeMetadata{}
	resolver, err := NewExecutionResolver(func(context.Context, worker.Execution) (frameworkartifact.Service, error) {
		return storage, nil
	}, metadata)
	if err != nil {
		t.Fatalf("NewExecutionResolver: %v", err)
	}
	exec := testArtifactExecution()
	service, err := resolver.ResolveArtifact(context.Background(), exec)
	if err != nil {
		t.Fatalf("ResolveArtifact: %v", err)
	}
	if service == nil {
		t.Fatal("ResolveArtifact returned nil for configured backend")
	}
	info := frameworkartifact.SessionInfo{
		AppName:   testAppName(t, testScope()),
		UserID:    "user-1",
		SessionID: "session-1",
	}
	if _, err := service.SaveArtifact(context.Background(), info, "report.txt", &frameworkartifact.Artifact{}); err != nil {
		t.Fatalf("SaveArtifact: %v", err)
	}
	if metadata.reserveCalls != 1 {
		t.Fatalf("metadata reserve calls = %d, want 1", metadata.reserveCalls)
	}
	if _, err := service.LoadArtifact(context.Background(), frameworkartifact.SessionInfo{
		AppName: info.AppName, UserID: "other-user", SessionID: info.SessionID,
	}, "report.txt", nil); err == nil {
		t.Fatal("LoadArtifact accepted another session principal")
	}
}

func newTestService(t *testing.T, storage frameworkartifact.Service, metadata MetadataStore) *Service {
	t.Helper()
	service, err := NewService(storage, metadata, Access{
		Scope:              testScope(),
		ConfigVersion:      "v1",
		SessionPrincipalID: "user-1",
		SessionID:          "session-1",
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

func testScope() tenant.Scope {
	return tenant.Scope{TenantID: "tenant-1", AppID: "app-1"}
}

func testSessionInfo(t *testing.T, scope tenant.Scope) frameworkartifact.SessionInfo {
	t.Helper()
	return frameworkartifact.SessionInfo{
		AppName:   testAppName(t, scope),
		UserID:    "user-1",
		SessionID: "session-1",
	}
}

func testAppName(t *testing.T, scope tenant.Scope) string {
	t.Helper()
	appName, err := scope.Key("runner")
	if err != nil {
		t.Fatalf("scope key: %v", err)
	}
	return appName
}

type fakeStorage struct {
	saveCalls          int
	loadCalls          int
	listKeysCalls      int
	deleteCalls        int
	deleteVersionCalls int
	savedInfo          frameworkartifact.SessionInfo
	savedRequests      []savedRequest
	deleteErr          error
}

type savedRequest struct {
	info     frameworkartifact.SessionInfo
	filename string
}

func (s *fakeStorage) SaveArtifact(
	_ context.Context,
	sessionInfo frameworkartifact.SessionInfo,
	filename string,
	_ *frameworkartifact.Artifact,
) (int, error) {
	s.saveCalls++
	s.savedInfo = sessionInfo
	s.savedRequests = append(s.savedRequests, savedRequest{info: sessionInfo, filename: filename})
	return 0, nil
}

func (s *fakeStorage) SaveArtifactVersion(
	_ context.Context,
	sessionInfo frameworkartifact.SessionInfo,
	filename string,
	_ int,
	_ *frameworkartifact.Artifact,
) error {
	s.saveCalls++
	s.savedInfo = sessionInfo
	s.savedRequests = append(s.savedRequests, savedRequest{info: sessionInfo, filename: filename})
	return nil
}

func (s *fakeStorage) LoadArtifact(
	context.Context,
	frameworkartifact.SessionInfo,
	string,
	*int,
) (*frameworkartifact.Artifact, error) {
	s.loadCalls++
	return &frameworkartifact.Artifact{}, nil
}

func (s *fakeStorage) ListArtifactKeys(context.Context, frameworkartifact.SessionInfo) ([]string, error) {
	s.listKeysCalls++
	return []string{"unfiltered.txt"}, nil
}

func (s *fakeStorage) DeleteArtifact(context.Context, frameworkartifact.SessionInfo, string) error {
	s.deleteCalls++
	return s.deleteErr
}

func (s *fakeStorage) DeleteArtifactVersion(context.Context, frameworkartifact.SessionInfo, string, int) error {
	s.deleteVersionCalls++
	return s.deleteErr
}

func (*fakeStorage) ListVersions(context.Context, frameworkartifact.SessionInfo, string) ([]int, error) {
	return nil, nil
}

func (*fakeStorage) ObjectKey(frameworkartifact.SessionInfo, string, int) (string, error) {
	return "object-key", nil
}

type fakeMetadata struct {
	reserveCalls int
	abandonCalls int
	findCalls    int
	keys         []string
	findErr      error
	createErr    error
	abandonErr   error
}

func (m *fakeMetadata) ReserveArtifact(_ context.Context, access Access, filename, mimeType string, size int64) (Record, error) {
	m.reserveCalls++
	return Record{
		ID: "artifact-reservation", TenantID: access.Scope.TenantID, AppID: access.Scope.AppID,
		SessionPrincipalID: access.SessionPrincipalID, SessionID: access.SessionID, Filename: filename,
		Version: 0, MIMEType: mimeType, Size: size, Status: StatusPending,
	}, nil
}

func (*fakeMetadata) BindArtifactObject(context.Context, Record, string) error { return nil }

func (m *fakeMetadata) PublishArtifact(context.Context, Record) error { return m.createErr }

func (m *fakeMetadata) AbandonArtifact(context.Context, Record) error {
	m.abandonCalls++
	return m.abandonErr
}

func testArtifactExecution() worker.Execution {
	return worker.Execution{
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-1",
			AppID:              "app-1",
			ConfigVersion:      "v1",
			SessionPrincipalID: "user-1",
			SessionID:          "session-1",
			UserID:             "user-1",
			TraceID:            "artifact-test",
		},
		Config: tenant.AppConfig{
			TenantID: "tenant-1",
			AppID:    "app-1",
			Version:  "v1",
			BackendConfig: tenant.BackendConfig{
				Name: "backends",
				Artifact: tenant.BackendRef{
					Kind:     tenant.BackendObject,
					Provider: "cos",
					Name:     "artifacts",
				},
			},
		},
	}
}

func (m *fakeMetadata) FindArtifact(context.Context, Access, string, *int) (Record, error) {
	m.findCalls++
	if m.findErr != nil {
		return Record{}, m.findErr
	}
	return Record{Version: 0, ConfigVersion: "v1"}, nil
}

func (m *fakeMetadata) ListArtifactKeys(context.Context, Access) ([]string, error) {
	return m.keys, nil
}

func (*fakeMetadata) ListArtifactVersions(context.Context, Access, string) ([]int, error) {
	return nil, nil
}

func (*fakeMetadata) MarkArtifactsDeleted(_ context.Context, access Access, filename string) ([]Record, error) {
	return []Record{{
		ID: "deleted-artifact", TenantID: access.Scope.TenantID, AppID: access.Scope.AppID,
		SessionPrincipalID: access.SessionPrincipalID, SessionID: access.SessionID, Filename: filename,
		Version: 0, ObjectKey: "object-key", ConfigVersion: access.ConfigVersion, Status: StatusDeleted,
	}}, nil
}

var _ frameworkartifact.Service = (*fakeStorage)(nil)
var _ MetadataStore = (*fakeMetadata)(nil)
