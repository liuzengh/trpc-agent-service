package assembly

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/session"
	frameworkpostgres "trpc.group/trpc-go/trpc-agent-go/storage/postgres"
)

func TestManagedSessionProviderResolvesTenantStoragePolicy(t *testing.T) {
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, testBackendProfileResolver{
		"session-memory": {Driver: SessionDriverInMemory},
	}, noopSessionSummarizer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	configuration := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support",
		Storage: config.StoragePolicy{Session: config.BackendProfileRef{ProfileID: "session-memory"}},
	}
	service, err := provider.Session(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: configuration.AppName(), UserID: "customer-1", SessionID: "conversation-1"}
	created, err := service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := service.GetSession(context.Background(), key)
	if err != nil || loaded == nil || loaded.ID != created.ID {
		t.Fatalf("Session() round trip = %#v, %v", loaded, err)
	}
}

type staticSessionMigrationRouteSource struct {
	route platformstorage.SessionMigrationRoute
}

func (s staticSessionMigrationRouteSource) ActiveSessionMigrationRoute(context.Context, string, string) (platformstorage.SessionMigrationRoute, bool, error) {
	return s.route, true, nil
}

func TestManagedSessionProviderRoutesActiveMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration-target.db")
	resolver, err := credential.NewEnvironmentSecretResolver(func(name string) string {
		if name == "SESSION_MIGRATION_SQLITE_PATH" {
			return path
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	source := platformstorage.SessionBackendRef{Driver: SessionDriverInMemory}
	target := platformstorage.SessionBackendRef{Driver: SessionDriverSQLite, ConnectionRef: "env:SESSION_MIGRATION_SQLITE_PATH"}
	provider, err := NewManagedSessionProvider(
		"postgres://unused",
		resolver,
		testBackendProfileResolver{},
		noopSessionSummarizer{},
		staticSessionMigrationRouteSource{route: platformstorage.SessionMigrationRoute{
			MigrationID: "migration-1", TenantID: "tenant-a", AppCode: "support",
			Reader: source, PrimaryWriter: source, ReplicaWriters: []platformstorage.SessionBackendRef{target},
		}},
		WithSessionMigrationRepairs(&memorySessionRepairStore{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	configuration := config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}
	routed, err := provider.Session(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: configuration.AppName(), UserID: "customer-1", SessionID: "conversation-migrating"}
	if _, err := routed.CreateSession(context.Background(), key, session.StateMap{"mode": []byte("brief")}); err != nil {
		t.Fatalf("CreateSession() through migration route: %v", err)
	}

	for name, backend := range map[string]platformstorage.SessionBackendRef{"source": source, "target": target} {
		service, err := provider.SessionServiceFor(context.Background(), configuration, backend)
		if err != nil {
			t.Fatalf("resolve %s backend: %v", name, err)
		}
		loaded, err := service.GetSession(context.Background(), key)
		if err != nil || loaded == nil || string(loaded.SnapshotState()["mode"]) != "brief" {
			t.Fatalf("%s backend session = %#v, %v", name, loaded, err)
		}
	}
}

func TestManagedSessionProviderSQLiteRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	resolver, err := credential.NewEnvironmentSecretResolver(func(name string) string {
		if name == "SESSION_SQLITE_PATH" {
			return path
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, testBackendProfileResolver{}, noopSessionSummarizer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	configuration := config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}
	service, err := provider.SessionServiceFor(context.Background(), configuration, platformstorage.SessionBackendRef{
		Driver: SessionDriverSQLite, ConnectionRef: "env:SESSION_SQLITE_PATH",
	})
	if err != nil {
		t.Fatal(err)
	}
	key := frameworkSessionKey(platformstorage.Session{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/session-1", SubjectID: "user-1",
	})
	created, err := service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := service.GetSession(context.Background(), key)
	if err != nil || loaded == nil || loaded.ID != created.ID {
		t.Fatalf("SQLite Session round trip = %#v, %v", loaded, err)
	}
}

func TestManagedSessionProviderSecretRotationUsesFreshService(t *testing.T) {
	firstPath := filepath.Join(t.TempDir(), "first.db")
	secondPath := filepath.Join(t.TempDir(), "second.db")
	currentPath := firstPath
	resolver, err := credential.NewEnvironmentSecretResolver(func(name string) string {
		if name == "SESSION_SQLITE_PATH" {
			return currentPath
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, testBackendProfileResolver{}, noopSessionSummarizer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	configuration := config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}
	backend := platformstorage.SessionBackendRef{Driver: SessionDriverSQLite, ConnectionRef: "env:SESSION_SQLITE_PATH"}

	first, err := provider.SessionServiceFor(context.Background(), configuration, backend)
	if err != nil {
		t.Fatal(err)
	}
	currentPath = secondPath
	second, err := provider.SessionServiceFor(context.Background(), configuration, backend)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("secret rotation reused the old Session service")
	}
	provider.mu.Lock()
	serviceCount := len(provider.services)
	provider.mu.Unlock()
	if serviceCount != 2 {
		t.Fatalf("cached Session services = %d, want 2 physical connections", serviceCount)
	}
}

func TestManagedSessionProviderDoesNotReopenAfterClose(t *testing.T) {
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, testBackendProfileResolver{}, noopSessionSummarizer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	_, err = provider.SessionServiceFor(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support",
	}, platformstorage.SessionBackendRef{Driver: SessionDriverInMemory})
	if !errors.Is(err, ErrManagedProviderClosed) {
		t.Fatalf("SessionServiceFor() after Close() = %v, want ErrManagedProviderClosed", err)
	}
}

type rejectingDDLClient struct {
	operations int
}

func (c *rejectingDDLClient) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	c.operations++
	return nil, errors.New("unexpected runtime PostgreSQL DDL")
}

func (c *rejectingDDLClient) Query(context.Context, frameworkpostgres.HandlerFunc, string, ...any) error {
	c.operations++
	return errors.New("unexpected runtime PostgreSQL query during construction")
}

func (c *rejectingDDLClient) Transaction(context.Context, frameworkpostgres.TxFunc) error {
	c.operations++
	return errors.New("unexpected runtime PostgreSQL transaction during construction")
}

func (*rejectingDDLClient) Close() error { return nil }

func TestManagedSessionProviderDoesNotRunPostgresDDLAtRuntime(t *testing.T) {
	originalBuilder := frameworkpostgres.GetClientBuilder()
	t.Cleanup(func() { frameworkpostgres.SetClientBuilder(originalBuilder) })

	client := &rejectingDDLClient{}
	frameworkpostgres.SetClientBuilder(func(context.Context, ...frameworkpostgres.ClientBuilderOpt) (frameworkpostgres.Client, error) {
		return client, nil
	})

	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, testBackendProfileResolver{}, noopSessionSummarizer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	_, err = provider.SessionServiceFor(context.Background(), config.TenantConfig{
		TenantID: "tenant-a",
		AppCode:  "support",
	}, platformstorage.SessionBackendRef{Driver: SessionDriverPostgres})
	if err != nil {
		t.Fatalf("construct PostgreSQL Session backend: %v", err)
	}
	if client.operations != 0 {
		t.Fatalf("runtime Session construction performed %d PostgreSQL operations, want 0", client.operations)
	}
}
