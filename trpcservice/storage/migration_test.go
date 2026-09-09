package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestSessionMigratorFullFlowAndRollback(t *testing.T) {
	ctx := context.Background()
	source := sessioninmemory.NewSessionService()
	target := sessioninmemory.NewSessionService()
	factory := newBackendFactory(backendConstructors{
		redisSession: func(tenant.AgentApp) (session.Service, error) {
			return source, nil
		},
		mysqlSession: func(tenant.AgentApp) (session.Service, error) {
			return target, nil
		},
	})
	t.Cleanup(func() { _ = factory.Close() })
	app := factoryApp("app-a", 1, "redis", "pgvector")
	app.AppName = "tenant-a-support"
	keys := make([]session.Key, 50)
	for index := range keys {
		keys[index] = session.Key{
			AppName: app.AppName, UserID: "user", SessionID: fmt.Sprintf("session-%02d", index),
		}
		current, err := source.CreateSession(ctx, keys[index], session.StateMap{
			"index": []byte{byte(index)},
		})
		if err != nil {
			t.Fatalf("CreateSession(%d) error = %v", index, err)
		}
		if err := source.AppendEvent(ctx, current, &event.Event{
			ID: keys[index].SessionID,
			Response: &model.Response{
				ID:     "response-" + keys[index].SessionID,
				Object: model.ObjectTypeChatCompletion,
				Done:   true,
				Choices: []model.Choice{{
					Message: model.NewUserMessage("message"),
				}},
			},
		}); err != nil {
			t.Fatalf("AppendEvent(%d) error = %v", index, err)
		}
	}
	if current, err := source.GetSession(ctx, keys[0]); err != nil ||
		current == nil || len(current.Events) != 1 {
		t.Fatalf("source fixture session = %#v, %v", current, err)
	}
	store := newMemoryMigrationStore()
	locker := &countingMigrationLocker{}
	migrator, err := NewSessionMigrator(
		store,
		&staticAppResolver{app: app},
		&staticSessionCatalog{keys: keys},
		locker,
		factory,
	)
	if err != nil {
		t.Fatalf("NewSessionMigrator() error = %v", err)
	}
	id, err := migrator.Start(ctx, app.ID, "redis", "mysql")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	wantPhases := []MigrationPhase{
		PhaseDualWrite,
		PhaseBackfill,
		PhaseVerify,
		PhaseCutRead,
		PhaseStopOldWrite,
		PhaseDone,
	}
	for _, want := range wantPhases {
		got, err := migrator.Advance(ctx, id)
		if err != nil {
			t.Fatalf("Advance(%s) error = %v", want, err)
		}
		if got != want {
			t.Fatalf("Advance() phase = %q, want %q", got, want)
		}
	}
	for _, key := range keys {
		got, err := target.GetSession(ctx, key)
		if err != nil || got == nil || len(got.Events) != 1 {
			t.Fatalf("target session %q = %#v, %v", key.SessionID, got, err)
		}
	}
	if locker.calls != len(keys) {
		t.Fatalf("migration lock calls = %d, want %d", locker.calls, len(keys))
	}
	status, err := migrator.Status(ctx, id)
	if err != nil || status.Phase != PhaseDone {
		t.Fatalf("Status() = %#v, %v", status, err)
	}
	if err := target.UpdateSessionState(ctx, keys[0], session.StateMap{
		"post_cutover": []byte("mysql"),
	}); err != nil {
		t.Fatalf("mark target session: %v", err)
	}
	factory.ClearSessionRoute(app.ID)
	if err := migrator.Resume(ctx); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	resumed, err := factory.SessionService(app)
	if err != nil {
		t.Fatalf("SessionService(resumed) error = %v", err)
	}
	resumedSession, err := resumed.GetSession(ctx, keys[0])
	if err != nil || string(resumedSession.State["post_cutover"]) != "mysql" {
		t.Fatalf("resumed route did not read MySQL: %#v, %v", resumedSession, err)
	}

	// A second migration whose source does not match the app's effective
	// backend (mysql after the first migration completed) must be rejected
	// instead of silently reverting routing to redis.
	if _, err := migrator.Start(ctx, app.ID, "redis", "mysql"); !errors.Is(err, ErrMigrationConflict) {
		t.Fatalf("Start() after a completed migration = %v, want ErrMigrationConflict", err)
	}

	// Rollback of an in-flight migration restores the source route.
	appB := factoryApp("app-b", 1, "redis", "pgvector")
	appB.AppName = "tenant-b-support"
	rollbackKey := session.Key{
		AppName: appB.AppName, UserID: "user", SessionID: "session-00",
	}
	if _, err := source.CreateSession(ctx, rollbackKey, nil); err != nil {
		t.Fatalf("seed app-b source session: %v", err)
	}
	migratorB, err := NewSessionMigrator(
		store, &staticAppResolver{app: appB}, &staticSessionCatalog{}, nil, factory,
	)
	if err != nil {
		t.Fatalf("NewSessionMigrator(app-b) error = %v", err)
	}
	rollbackID, err := migratorB.Start(ctx, appB.ID, "redis", "mysql")
	if err != nil {
		t.Fatalf("Start(app-b) error = %v", err)
	}
	if _, err := migratorB.Advance(ctx, rollbackID); err != nil {
		t.Fatalf("Advance(dual_write) error = %v", err)
	}
	if err := migratorB.Rollback(ctx, rollbackID); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	rollbackStatus, err := migratorB.Status(ctx, rollbackID)
	if err != nil || rollbackStatus.Phase != PhaseRolledBack {
		t.Fatalf("rollback Status() = %#v, %v", rollbackStatus, err)
	}
	rolledBackService, err := factory.SessionService(appB)
	if err != nil {
		t.Fatalf("SessionService(app-b) error = %v", err)
	}
	if _, err := rolledBackService.GetSession(ctx, rollbackKey); err != nil {
		t.Fatalf("rolled-back route did not read the source backend: %v", err)
	}
}

// TestFactoryFollowsDurableMigrationAcrossReplicas kills the R1 attack: a
// replica that never ran the migration must resolve its session route from
// the durable migration store and read the cut-over backend.
func TestFactoryFollowsDurableMigrationAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	source := sessioninmemory.NewSessionService()
	target := sessioninmemory.NewSessionService()
	newReplica := func() *BackendFactory {
		return newBackendFactory(backendConstructors{
			redisSession: func(tenant.AgentApp) (session.Service, error) { return source, nil },
			mysqlSession: func(tenant.AgentApp) (session.Service, error) { return target, nil },
		})
	}
	replicaA := newReplica()
	replicaB := newReplica()
	t.Cleanup(func() { _ = replicaA.Close() })
	t.Cleanup(func() { _ = replicaB.Close() })
	store := newMemoryMigrationStore()
	// Only replica B consults the durable route source, like a replica that
	// never received a local SetSessionRoute call.
	replicaB.SetRouteSource(NewMigrationRouteSource(store))

	app := factoryApp("app-a", 1, "redis", "pgvector")
	app.AppName = "tenant-a-support"
	key := session.Key{AppName: app.AppName, UserID: "user", SessionID: "session-01"}
	if _, err := source.CreateSession(ctx, key, session.StateMap{
		"value": []byte("redis"),
	}); err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	migrator, err := NewSessionMigrator(
		store,
		&staticAppResolver{app: app},
		&staticSessionCatalog{keys: []session.Key{key}},
		nil,
		replicaA,
	)
	if err != nil {
		t.Fatalf("NewSessionMigrator() error = %v", err)
	}
	id, err := migrator.Start(ctx, app.ID, "redis", "mysql")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	for _, want := range []MigrationPhase{
		PhaseDualWrite, PhaseBackfill, PhaseVerify, PhaseCutRead,
	} {
		if got, err := migrator.Advance(ctx, id); err != nil || got != want {
			t.Fatalf("Advance() = %q, %v; want %q", got, err, want)
		}
	}

	// Remove the source copy: replica B must still read the session, proving
	// it resolved the cut-read route from durable state instead of redis.
	if err := source.DeleteSession(ctx, key); err != nil {
		t.Fatalf("delete source session: %v", err)
	}
	serviceB, err := replicaB.SessionService(app)
	if err != nil {
		t.Fatalf("replicaB SessionService() error = %v", err)
	}
	got, err := serviceB.GetSession(ctx, key)
	if err != nil || got == nil {
		t.Fatalf("replicaB did not read the migrated backend: %#v, %v", got, err)
	}
}

func TestRoutedSessionServiceDualWritesAndCutsRead(t *testing.T) {
	ctx := context.Background()
	source := sessioninmemory.NewSessionService()
	target := sessioninmemory.NewSessionService()
	factory := newBackendFactory(backendConstructors{
		redisSession: func(tenant.AgentApp) (session.Service, error) { return source, nil },
		mysqlSession: func(tenant.AgentApp) (session.Service, error) { return target, nil },
	})
	t.Cleanup(func() { _ = factory.Close() })
	app := factoryApp("app-a", 1, "redis", "pgvector")
	app.AppName = "tenant-a-support"
	if err := factory.SetSessionRoute(app.ID, SessionRoute{
		Reader: "redis", Writers: []string{"redis", "mysql"},
	}); err != nil {
		t.Fatalf("SetSessionRoute(dual) error = %v", err)
	}
	routed, err := factory.SessionService(app)
	if err != nil {
		t.Fatalf("SessionService() error = %v", err)
	}
	key := session.Key{AppName: app.AppName, UserID: "user", SessionID: "session"}
	if _, err := routed.CreateSession(ctx, key, session.StateMap{"value": []byte("dual")}); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	if got, _ := target.GetSession(ctx, key); got == nil {
		t.Fatal("dual write did not reach target")
	}
	if err := factory.SetSessionRoute(app.ID, SessionRoute{
		Reader: "mysql", Writers: []string{"mysql"},
	}); err != nil {
		t.Fatalf("SetSessionRoute(target) error = %v", err)
	}
	targetOnly, err := factory.SessionService(app)
	if err != nil {
		t.Fatalf("SessionService(target) error = %v", err)
	}
	if got, _ := targetOnly.GetSession(ctx, key); got == nil ||
		string(got.State["value"]) != "dual" {
		t.Fatalf("cut-read session = %#v", got)
	}
}

func TestSessionMigratorValidation(t *testing.T) {
	if _, err := NewSessionMigrator(nil, nil, nil, nil, nil); err == nil {
		t.Fatal("NewSessionMigrator() accepted nil dependencies")
	}
	factory := newBackendFactory(backendConstructors{})
	migrator, err := NewSessionMigrator(
		newMemoryMigrationStore(),
		&staticAppResolver{},
		&staticSessionCatalog{},
		nil,
		factory,
	)
	if err != nil {
		t.Fatalf("NewSessionMigrator() error = %v", err)
	}
	if _, err := migrator.Start(context.Background(), "app", "mysql", "redis"); err == nil {
		t.Fatal("Start() accepted unsupported direction")
	}
}

type memoryMigrationStore struct {
	mu       sync.Mutex
	statuses map[string]MigrationStatus
}

func newMemoryMigrationStore() *memoryMigrationStore {
	return &memoryMigrationStore{statuses: make(map[string]MigrationStatus)}
}
func (m *memoryMigrationStore) Create(_ context.Context, status MigrationStatus) error {
	m.mu.Lock()
	m.statuses[status.ID] = status
	m.mu.Unlock()
	return nil
}
func (m *memoryMigrationStore) Get(_ context.Context, id string) (MigrationStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status, ok := m.statuses[id]
	if !ok {
		return MigrationStatus{}, tenant.ErrNotFound
	}
	status.Detail = cloneDetail(status.Detail)
	return status, nil
}
func (m *memoryMigrationStore) Update(_ context.Context, status MigrationStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.statuses[status.ID]; !ok {
		return tenant.ErrNotFound
	}
	status.Detail = cloneDetail(status.Detail)
	m.statuses[status.ID] = status
	return nil
}
func (m *memoryMigrationStore) ListActive(context.Context) ([]MigrationStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []MigrationStatus
	for _, status := range m.statuses {
		if status.Phase != PhaseRolledBack {
			status.Detail = cloneDetail(status.Detail)
			result = append(result, status)
		}
	}
	return result, nil
}

func cloneDetail(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

type staticAppResolver struct {
	app tenant.AgentApp
	err error
}

func (s *staticAppResolver) GetCurrentApp(context.Context, string) (tenant.AgentApp, error) {
	return s.app, s.err
}

type staticSessionCatalog struct {
	keys []session.Key
	err  error
}

func (s *staticSessionCatalog) ListSessionKeys(context.Context, string) ([]session.Key, error) {
	return append([]session.Key(nil), s.keys...), s.err
}

type countingMigrationLocker struct {
	calls int
}

func (c *countingMigrationLocker) WithSessionLock(
	_ context.Context,
	_ string,
	operation func() error,
) error {
	c.calls++
	return operation()
}
