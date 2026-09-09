package storage

import (
	"context"
	"io"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestBackendFactoryRealRedisModule(t *testing.T) {
	server := miniredis.RunT(t)
	factory := NewBackendFactory(FactoryConfig{
		RedisURL: "redis://" + server.Addr(),
	}, nil)
	t.Cleanup(func() { _ = factory.Close() })
	app := factoryApp("app-a", 1, "redis", "mem0")
	app.AppName = "tenant-a-support"

	service, err := factory.SessionService(app)
	if err != nil {
		t.Fatalf("SessionService(real Redis module) error = %v", err)
	}
	key := session.Key{
		AppName:   app.AppName,
		UserID:    "user-1",
		SessionID: "session-1",
	}
	if _, err := service.CreateSession(context.Background(), key, session.StateMap{
		"tenant": []byte(`"tenant-a"`),
	}); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	got, err := service.GetSession(context.Background(), key)
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if got == nil || string(got.State["tenant"]) != `"tenant-a"` {
		t.Fatalf("GetSession() = %#v, want tenant-a state", got)
	}
}

func TestBackendFactorySelectsCachesAndCloses(t *testing.T) {
	redisSession := &trackingSession{Service: sessioninmemory.NewSessionService()}
	mysqlSession := &trackingSession{Service: sessioninmemory.NewSessionService()}
	pgMemory := &trackingMemory{Service: memoryinmemory.NewMemoryService()}
	mem0Closer := &trackingCloser{}
	mem0Ingestor := &fakeIngestor{}
	var redisCalls, mysqlCalls, pgCalls, mem0Calls int
	factory := newBackendFactory(backendConstructors{
		redisSession: func(tenant.AgentApp) (session.Service, error) {
			redisCalls++
			return redisSession, nil
		},
		mysqlSession: func(tenant.AgentApp) (session.Service, error) {
			mysqlCalls++
			return mysqlSession, nil
		},
		pgvector: func(tenant.AgentApp) (MemoryBackend, io.Closer, error) {
			pgCalls++
			return MemoryBackend{Service: pgMemory}, pgMemory, nil
		},
		mem0: func(tenant.AgentApp) (MemoryBackend, io.Closer, error) {
			mem0Calls++
			return MemoryBackend{Ingestor: mem0Ingestor}, mem0Closer, nil
		},
	})
	appA := factoryApp("app-a", 1, "redis", "pgvector")
	appB := factoryApp("app-b", 1, "mysql", "mem0")

	sessionA, err := factory.SessionService(appA)
	if err != nil {
		t.Fatalf("SessionService(redis) error = %v", err)
	}
	sessionAAgain, err := factory.SessionService(appA)
	if err != nil {
		t.Fatalf("SessionService(redis cached) error = %v", err)
	}
	if sessionA != sessionAAgain || redisCalls != 1 {
		t.Fatalf("Redis session cache miss: calls=%d", redisCalls)
	}
	if _, err := factory.SessionService(appB); err != nil {
		t.Fatalf("SessionService(mysql) error = %v", err)
	}
	memoryA, err := factory.MemoryBackend(appA)
	if err != nil || memoryA.Service == nil {
		t.Fatalf("MemoryBackend(pgvector) = %#v, %v", memoryA, err)
	}
	memoryAAgain, err := factory.MemoryBackend(appA)
	if err != nil {
		t.Fatalf("MemoryBackend(pgvector cached) error = %v", err)
	}
	if memoryA.Service != memoryAAgain.Service || pgCalls != 1 {
		t.Fatalf("pgvector memory cache miss: calls=%d", pgCalls)
	}
	memoryB, err := factory.MemoryBackend(appB)
	if err != nil || memoryB.Ingestor == nil || memoryB.Service != nil {
		t.Fatalf("MemoryBackend(mem0) = %#v, %v", memoryB, err)
	}
	if redisCalls != 1 || mysqlCalls != 1 || pgCalls != 1 || mem0Calls != 1 {
		t.Fatalf("constructor calls = redis:%d mysql:%d pg:%d mem0:%d",
			redisCalls, mysqlCalls, pgCalls, mem0Calls)
	}

	if err := factory.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if redisSession.closes != 1 || mysqlSession.closes != 1 ||
		pgMemory.closes != 1 || mem0Closer.closes != 1 {
		t.Fatalf("close counts = redis:%d mysql:%d pg:%d mem0:%d",
			redisSession.closes, mysqlSession.closes, pgMemory.closes, mem0Closer.closes)
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := factory.SessionService(appA); err == nil {
		t.Fatal("SessionService() succeeded after Close")
	}
}

func TestBackendFactoryTenantIsolation(t *testing.T) {
	factory := newBackendFactory(backendConstructors{
		redisSession: func(tenant.AgentApp) (session.Service, error) {
			return sessioninmemory.NewSessionService(), nil
		},
		mysqlSession: func(tenant.AgentApp) (session.Service, error) {
			return sessioninmemory.NewSessionService(), nil
		},
		pgvector: func(tenant.AgentApp) (MemoryBackend, io.Closer, error) {
			service := memoryinmemory.NewMemoryService()
			return MemoryBackend{Service: service}, service, nil
		},
	})
	t.Cleanup(func() { _ = factory.Close() })
	appA := factoryApp("app-a", 1, "redis", "pgvector")
	appA.TenantID = "tenant-a"
	appA.AppName = "tenant-a-support"
	appB := factoryApp("app-b", 1, "mysql", "pgvector")
	appB.TenantID = "tenant-b"
	appB.AppName = "tenant-b-support"

	sessionA, err := factory.SessionService(appA)
	if err != nil {
		t.Fatalf("SessionService(appA) error = %v", err)
	}
	sessionB, err := factory.SessionService(appB)
	if err != nil {
		t.Fatalf("SessionService(appB) error = %v", err)
	}
	keyA := session.Key{AppName: appA.AppName, UserID: "same-user", SessionID: "same-session"}
	if _, err := sessionA.CreateSession(context.Background(), keyA, nil); err != nil {
		t.Fatalf("CreateSession(appA) error = %v", err)
	}
	keyB := session.Key{AppName: appB.AppName, UserID: "same-user", SessionID: "same-session"}
	got, err := sessionB.GetSession(context.Background(), keyB)
	if err != nil {
		t.Fatalf("GetSession(appB) error = %v", err)
	}
	if got != nil {
		t.Fatalf("tenant B read tenant A session: %#v", got)
	}

	memoryA, err := factory.MemoryBackend(appA)
	if err != nil {
		t.Fatalf("MemoryBackend(appA) error = %v", err)
	}
	memoryB, err := factory.MemoryBackend(appB)
	if err != nil {
		t.Fatalf("MemoryBackend(appB) error = %v", err)
	}
	userA := memory.UserKey{AppName: appA.AppName, UserID: "same-user"}
	if err := memoryA.Service.AddMemory(context.Background(), userA, "tenant A secret", nil); err != nil {
		t.Fatalf("AddMemory(appA) error = %v", err)
	}
	userB := memory.UserKey{AppName: appB.AppName, UserID: "same-user"}
	memories, err := memoryB.Service.ReadMemories(context.Background(), userB, 10)
	if err != nil {
		t.Fatalf("ReadMemories(appB) error = %v", err)
	}
	if len(memories) != 0 {
		t.Fatalf("tenant B read tenant A memories: %#v", memories)
	}
}

func TestBackendFactoryValidation(t *testing.T) {
	factory := newBackendFactory(backendConstructors{})
	if _, err := factory.SessionService(tenant.AgentApp{}); err == nil {
		t.Fatal("SessionService() accepted missing app identity")
	}
	app := factoryApp("app-a", 1, "unknown", "unknown")
	if _, err := factory.SessionService(app); err == nil {
		t.Fatal("SessionService() accepted unknown backend")
	}
	if _, err := factory.MemoryBackend(app); err == nil {
		t.Fatal("MemoryBackend() accepted unknown backend")
	}
	if got := normalizeRedisURL("127.0.0.1:6379"); got != "redis://127.0.0.1:6379" {
		t.Fatalf("normalizeRedisURL() = %q", got)
	}
	if got := normalizeRedisURL("rediss://host:6380"); got != "rediss://host:6380" {
		t.Fatalf("normalizeRedisURL(rediss) = %q", got)
	}
}

func factoryApp(id string, version int, sessionBackend, memoryBackend string) tenant.AgentApp {
	return tenant.AgentApp{
		ID:       id,
		TenantID: "tenant-a",
		AppName:  "tenant-a-" + id,
		Version:  version,
		Backends: tenant.BackendSelection{
			Session: sessionBackend,
			Memory:  memoryBackend,
		},
	}
}

type trackingSession struct {
	session.Service
	closes int
}

func (s *trackingSession) Close() error {
	s.closes++
	return s.Service.Close()
}

type trackingMemory struct {
	memory.Service
	closes int
}

func (m *trackingMemory) Close() error {
	m.closes++
	return m.Service.Close()
}

type trackingCloser struct {
	closes int
	err    error
}

func (c *trackingCloser) Close() error {
	c.closes++
	return c.err
}

type fakeIngestor struct{}

func (*fakeIngestor) IngestSession(
	context.Context,
	*session.Session,
	...session.IngestOption,
) error {
	return nil
}

var _ session.Ingestor = (*fakeIngestor)(nil)
