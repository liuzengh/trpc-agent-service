package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniserver "github.com/alicebob/miniredis/v2/server"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRedisBackendNormalizesPrefixAndRejectsEmpty(t *testing.T) {
	backend, err := NewRedisBackend("redis://127.0.0.1:6379/0", "  demo:: ")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if got, want := backend.Prefix(), "demo:official-v1"; got != want {
		t.Fatalf("Prefix() = %q, want %q", got, want)
	}
	for _, prefix := range []string{"", " : ", ":::"} {
		if _, err := NewRedisBackend("redis://127.0.0.1:6379/0", prefix); err == nil {
			t.Fatalf("NewRedisBackend(%q) unexpectedly succeeded", prefix)
		}
	}
}

func TestRedisBackendLazilyInitializesAndRecovers(t *testing.T) {
	server := miniredis.RunT(t)
	backend, err := NewRedisBackend("redis://"+server.Addr()+"/0", "backend-test:")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if backend.Session() != nil || backend.Memory() != nil {
		t.Fatal("services initialized before Ready")
	}

	server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := backend.Ready(ctx); err == nil {
		t.Fatal("Ready unexpectedly succeeded while Redis was stopped")
	}
	if backend.Session() != nil || backend.Memory() != nil {
		t.Fatal("failed Ready retained partially initialized services")
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Ready(ctx); err != nil {
		t.Fatalf("Ready after Redis recovery: %v", err)
	}
	if backend.Session() == nil || backend.Memory() == nil {
		t.Fatal("Ready did not initialize both official services")
	}
	if tools := backend.Memory().Tools(); len(tools) != 0 {
		t.Fatalf("official memory Tools() = %d, want empty", len(tools))
	}
	if err := backend.Ready(ctx); err != nil {
		t.Fatalf("second Ready: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	if err := backend.Ready(ctx); err == nil {
		t.Fatal("Ready unexpectedly succeeded after Close")
	}
}

func TestRedisBackendOfficialServicesVisibleAcrossInstances(t *testing.T) {
	server := miniredis.RunT(t)
	first, err := NewRedisBackend("redis://"+server.Addr()+"/0", "cross-instance")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRedisBackend("redis://"+server.Addr()+"/0", "cross-instance")
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()
	ctx := context.Background()
	if err := first.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Ready(ctx); err != nil {
		t.Fatal(err)
	}

	userKey := memory.UserKey{AppName: "app", UserID: "user"}
	if err := first.Memory().AddMemory(ctx, userKey, "official redis", []string{"test"}); err != nil {
		t.Fatal(err)
	}
	entries, err := second.Memory().SearchMemories(ctx, userKey, "official")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != "official redis" {
		t.Fatalf("unexpected memories: %#v", entries)
	}

	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	if _, err := first.Session().CreateSession(ctx, key, session.StateMap{"turn": []byte("one")}); err != nil {
		t.Fatal(err)
	}
	sess, err := second.Session().GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if sess == nil || string(sess.State["turn"]) != "one" {
		t.Fatalf("unexpected session: %#v", sess)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Session().GetSession(ctx, key); err != nil {
		t.Fatalf("second session service after first close: %v", err)
	}
	if err := second.Memory().AddMemory(ctx, userKey, "still available", nil); err != nil {
		t.Fatalf("second memory service after first close: %v", err)
	}
}

type closeTrackingSession struct {
	session.Service
	closes atomic.Int32
}

func (s *closeTrackingSession) Close() error {
	s.closes.Add(1)
	return nil
}

type closeTrackingMemory struct {
	memory.Service
	closes atomic.Int32
}

func (m *closeTrackingMemory) Close() error {
	m.closes.Add(1)
	return nil
}

func TestRedisBackendClosesSessionAfterPartialConstructionFailure(t *testing.T) {
	server := miniredis.RunT(t)
	backend, err := NewRedisBackend("redis://"+server.Addr()+"/0", "partial-failure")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	created := &closeTrackingSession{Service: sessioninmemory.NewSessionService()}
	backend.newSession = func() (session.Service, error) { return created, nil }
	backend.newMemory = func() (memory.Service, error) { return nil, errors.New("injected memory failure") }

	if err := backend.Ready(context.Background()); err == nil {
		t.Fatal("Ready unexpectedly succeeded")
	}
	if created.closes.Load() != 1 {
		t.Fatalf("partially created session Close calls = %d, want 1", created.closes.Load())
	}
	if backend.Session() != nil || backend.Memory() != nil {
		t.Fatal("partially created services were retained")
	}
}

func TestRedisBackendClosesNonNilServicesReturnedWithFactoryErrors(t *testing.T) {
	server := miniredis.RunT(t)
	tests := []struct {
		name        string
		configure   func(*RedisBackend, *closeTrackingSession, *closeTrackingMemory)
		wantSession int32
		wantMemory  int32
	}{
		{
			name: "session factory",
			configure: func(backend *RedisBackend, sessions *closeTrackingSession, _ *closeTrackingMemory) {
				backend.newSession = func() (session.Service, error) {
					return sessions, errors.New("session factory failed")
				}
			},
			wantSession: 1,
		},
		{
			name: "memory factory",
			configure: func(backend *RedisBackend, sessions *closeTrackingSession, memories *closeTrackingMemory) {
				backend.newSession = func() (session.Service, error) { return sessions, nil }
				backend.newMemory = func() (memory.Service, error) {
					return memories, errors.New("memory factory failed")
				}
			},
			wantSession: 1,
			wantMemory:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend, err := NewRedisBackend("redis://"+server.Addr()+"/0", "partial-error")
			if err != nil {
				t.Fatal(err)
			}
			sessions := &closeTrackingSession{Service: sessioninmemory.NewSessionService()}
			memories := &closeTrackingMemory{Service: memoryinmemory.NewMemoryService()}
			tt.configure(backend, sessions, memories)
			if err := backend.Ready(context.Background()); err == nil {
				t.Fatal("Ready unexpectedly succeeded")
			}
			if got := sessions.closes.Load(); got != tt.wantSession {
				t.Fatalf("session Close calls = %d, want %d", got, tt.wantSession)
			}
			if got := memories.closes.Load(); got != tt.wantMemory {
				t.Fatalf("memory Close calls = %d, want %d", got, tt.wantMemory)
			}
			_ = backend.Close()
		})
	}
}

func TestRedisBackendRejectsCanceledReadyBeforeConstruction(t *testing.T) {
	server := miniredis.RunT(t)
	backend, err := NewRedisBackend("redis://"+server.Addr()+"/0", "canceled-ready")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	backend.newSession = func() (session.Service, error) {
		called = true
		return sessioninmemory.NewSessionService(), nil
	}
	if err := backend.Ready(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ready() error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("session constructor called with canceled context")
	}
}

func TestRedisBackendClosesServicesWhenReadyContextIsCanceled(t *testing.T) {
	server := miniredis.RunT(t)
	backend, err := NewRedisBackend("redis://"+server.Addr()+"/0", "cancel-during-ready")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	sessions := &closeTrackingSession{Service: sessioninmemory.NewSessionService()}
	memories := &closeTrackingMemory{Service: memoryinmemory.NewMemoryService()}
	ctx, cancel := context.WithCancel(context.Background())
	backend.newSession = func() (session.Service, error) {
		cancel()
		return sessions, nil
	}
	backend.newMemory = func() (memory.Service, error) {
		return memories, nil
	}
	if err := backend.Ready(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ready() error = %v, want context.Canceled", err)
	}
	if sessions.closes.Load() != 1 || memories.closes.Load() != 0 {
		t.Fatalf("partial services closed = (%d, %d), want (1, 0)", sessions.closes.Load(), memories.closes.Load())
	}
	if backend.Session() != nil || backend.Memory() != nil {
		t.Fatal("canceled Ready retained services")
	}
}

func TestRedisBackendConcurrentClose(t *testing.T) {
	server := miniredis.RunT(t)
	backend, err := NewRedisBackend("redis://"+server.Addr()+"/0", "concurrent-close")
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- backend.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
}

func TestStrongTopologyRejectsRunIDMismatch(t *testing.T) {
	first := miniredis.RunT(t)
	second := miniredis.RunT(t)
	mockStorageTopology(first, "run-a")
	mockStorageTopology(second, "run-b")
	client := redis.NewClient(&redis.Options{Addr: first.Addr()})
	defer client.Close()
	err := verifyStrongRedisTopology(context.Background(), client, "redis://"+first.Addr()+"/0", "redis://"+second.Addr()+"/0")
	if err == nil || !strings.Contains(err.Error(), "share run_id") {
		t.Fatalf("run_id mismatch error=%v", err)
	}
}

func TestStrongRedisBackendReadinessRecoversWithoutRecreation(t *testing.T) {
	server := miniredis.RunT(t)
	redisURL := "redis://" + server.Addr() + "/0"
	backend, err := NewFencedRedisBackendWithConfig(redisURL, "phase4-recovery", sessionfence.Limits{MaxTurnEvents: 8, MaxTurnBytes: 4096}, "phase4-messaging", redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	server.Close()
	if err := backend.Ready(context.Background()); err == nil {
		t.Fatal("Strong Ready unexpectedly succeeded while Redis was stopped")
	}
	if backend.Session() != nil || backend.Memory() != nil {
		t.Fatal("failed Strong Ready retained partially initialized services")
	}
	if err := server.Restart(); err != nil {
		t.Fatal(err)
	}
	mockStorageTopology(server, "recovered-run")
	if err := backend.Ready(context.Background()); err != nil {
		t.Fatalf("Strong Ready after Redis recovery: %v", err)
	}
	if backend.Session() == nil || backend.Memory() == nil {
		t.Fatal("recovered Strong backend did not initialize services")
	}
}

func mockStorageTopology(server *miniredis.Miniredis, runID string) {
	server.Server().SetPreHook(func(peer *miniserver.Peer, command string, _ ...string) bool {
		switch command {
		case "ROLE":
			peer.WriteLen(3)
			peer.WriteBulk("master")
			peer.WriteInt(0)
			peer.WriteLen(0)
			return true
		case "INFO":
			peer.WriteBulk("# Server\r\nrun_id:" + runID + "\r\n")
			return true
		case "CLUSTER":
			peer.WriteError("ERR This instance has cluster support disabled")
			return true
		default:
			return false
		}
	})
}
