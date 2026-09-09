package storage

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type typedNilSessionService struct{ session.Service }
type typedNilMemoryService struct{ memory.Service }
type typedNilFenceAuthorizer struct{ fence.Authorizer }

func strictFenceContext(overrides func(*fence.Token)) context.Context {
	token := fence.Token{
		TenantID:      "tenant-a",
		AgentAppID:    "app-record-a",
		AgentAppName:  "support",
		ScopedAppName: "tsa1:8:tenant-a:support",
		UserID:        "user-a",
		SessionID:     "session-a",
		ExecutionID:   1,
		Generation:    1,
		Value:         "execution-token",
	}
	if overrides != nil {
		overrides(&token)
	}
	return fence.WithToken(context.Background(), token)
}

func TestStrictFencedSessionServiceRejectsCrossScopeKeysBeforeAcquire(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	service, err := NewStrictFencedSessionService(sessioninmem.NewSessionService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	cases := []session.Key{
		{AppName: "tsa1:8:tenant-b:support", UserID: "user-a", SessionID: "session-a"},
		{AppName: "tsa1:8:tenant-a:other", UserID: "user-a", SessionID: "session-a"},
		{AppName: "tsa1:8:tenant-a:support", UserID: "user-b", SessionID: "session-a"},
		{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-b"},
	}
	for _, key := range cases {
		if _, err := service.GetSession(strictFenceContext(nil), key); !errors.Is(err, fence.ErrScopeMismatch) {
			t.Fatalf("key=%#v error=%v, want ErrScopeMismatch", key, err)
		}
	}
	if got := authorizer.acquired.Load(); got != 0 {
		t.Fatalf("authorizer acquired %d times for rejected operations", got)
	}
}

func TestStrictFencedServicesRejectTypedNilBackends(t *testing.T) {
	var typedNilSession *typedNilSessionService
	if service, err := NewStrictFencedSessionService(typedNilSession, &countingFenceAuthorizer{}, "tenant-a"); service != nil || err == nil {
		t.Fatalf("typed-nil session service result=%v err=%v, want constructor failure", service, err)
	}
	var typedNilMemory *typedNilMemoryService
	if service, err := NewStrictFencedMemoryService(typedNilMemory, &countingFenceAuthorizer{}, "tenant-a"); service != nil || err == nil {
		t.Fatalf("typed-nil memory service result=%v err=%v, want constructor failure", service, err)
	}
	var typedNilAuthorizer *typedNilFenceAuthorizer
	if service, err := NewStrictFencedSessionService(sessioninmem.NewSessionService(), typedNilAuthorizer, "tenant-a"); service != nil || err == nil {
		t.Fatalf("typed-nil authorizer result=%v err=%v, want constructor failure", service, err)
	}
}

func TestStrictFencedSessionServiceUsesGroupOwnerWhileMemoryUsesActor(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	sessionService, err := NewStrictFencedSessionService(sessioninmem.NewSessionService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer sessionService.Close()

	ctx := strictFenceContext(func(token *fence.Token) {
		token.UserID = "alice"
		token.SessionOwnerID = "group-owner"
		token.SessionID = "group-session"
	})
	if _, err := sessionService.CreateSession(ctx, session.Key{
		AppName: "tsa1:8:tenant-a:support", UserID: "group-owner", SessionID: "group-session",
	}, nil); err != nil {
		t.Fatalf("group session rejected: %v", err)
	}
	if err := sessionService.UpdateSessionState(ctx, session.Key{
		AppName: "tsa1:8:tenant-a:support", UserID: "group-owner", SessionID: "group-session",
	}, session.StateMap{}); err != nil {
		t.Fatalf("group session state rejected: %v", err)
	}

	memoryService, err := NewStrictFencedMemoryService(inmemory.NewMemoryService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer memoryService.Close()
	if err := memoryService.AddMemory(ctx, memory.UserKey{
		AppName: "tsa1:8:tenant-a:support", UserID: "alice",
	}, "actor memory", nil); err != nil {
		t.Fatalf("actor memory rejected: %v", err)
	}
	if err := memoryService.AddMemory(ctx, memory.UserKey{
		AppName: "tsa1:8:tenant-a:support", UserID: "group-owner",
	}, "shared session identity must not become actor memory", nil); !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("group owner was accepted as actor memory: %v", err)
	}
	if got := authorizer.acquired.Load(); got != 3 {
		t.Fatalf("authorizer acquired %d times, want 3", got)
	}
}

func TestStrictFencedSessionServiceLegacyTokenFallsBackToUserID(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	service, err := NewStrictFencedSessionService(sessioninmem.NewSessionService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if _, err := service.CreateSession(strictFenceContext(nil), session.Key{
		AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a",
	}, nil); err != nil {
		t.Fatalf("legacy direct session rejected: %v", err)
	}
}

func TestStrictFencedSessionUserStateUsesGroupOwner(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	service, err := NewStrictFencedSessionService(sessioninmem.NewSessionService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	ctx := strictFenceContext(func(token *fence.Token) {
		token.UserID = "alice"
		token.SessionOwnerID = "group-owner"
		token.SessionID = "group-session"
	})
	key := session.UserKey{AppName: "tsa1:8:tenant-a:support", UserID: "group-owner"}
	if err := service.UpdateUserState(ctx, key, session.StateMap{"locale": []byte("en")}); err != nil {
		t.Fatalf("group user state update rejected: %v", err)
	}
	if _, err := service.ListUserStates(ctx, key); err != nil {
		t.Fatalf("group user state read rejected: %v", err)
	}
	if _, err := service.ListSessions(ctx, key); err != nil {
		t.Fatalf("group session list rejected: %v", err)
	}
	if got := authorizer.acquired.Load(); got != 3 {
		t.Fatalf("authorizer acquired %d times, want 3", got)
	}
}

func TestStrictFencedMemoryServiceRejectsCrossScopeAndIncompleteTokens(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	service, err := NewStrictFencedMemoryService(inmemory.NewMemoryService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	if err := service.AddMemory(strictFenceContext(nil), memory.UserKey{
		AppName: "tsa1:8:tenant-a:support", UserID: "user-b",
	}, "text", nil); !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("cross-user memory error=%v, want ErrScopeMismatch", err)
	}
	missingScope := strictFenceContext(func(token *fence.Token) { token.ScopedAppName = "" })
	if _, err := service.ReadMemories(missingScope, memory.UserKey{
		AppName: "tsa1:8:tenant-a:support", UserID: "user-a",
	}, 10); !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("missing canonical scope error=%v, want ErrScopeMismatch", err)
	}
	if got := authorizer.acquired.Load(); got != 0 {
		t.Fatalf("authorizer acquired %d times for rejected operations", got)
	}
}

func TestStrictFencedServiceRejectsForgedCanonicalAppScope(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	service, err := NewStrictFencedMemoryService(inmemory.NewMemoryService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	ctx := strictFenceContext(func(token *fence.Token) {
		token.ScopedAppName = "tsa1:8:tenant-b:support"
	})
	_, err = service.ReadMemories(ctx, memory.UserKey{
		AppName: "tsa1:8:tenant-b:support", UserID: "user-a",
	}, 10)
	if !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("forged canonical app scope error=%v, want ErrScopeMismatch", err)
	}
	if got := authorizer.acquired.Load(); got != 0 {
		t.Fatalf("authorizer acquired %d times for forged scope", got)
	}
}

type unhealthySessionService struct {
	session.Service
	err error
}

func (s unhealthySessionService) PingContext(context.Context) error { return s.err }

func TestFencedServicePreservesBackendHealthFailure(t *testing.T) {
	want := errors.New("backend unavailable")
	service, err := NewStrictFencedSessionService(
		unhealthySessionService{Service: sessioninmem.NewSessionService(), err: want},
		&countingFenceAuthorizer{},
		"tenant-a",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if err := checkBackendHealth(context.Background(), service); !errors.Is(err, want) {
		t.Fatalf("health error=%v, want %v", err, want)
	}
}

type wrongScopeSessionService struct{ session.Service }

func (wrongScopeSessionService) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	return &session.Session{ID: "session-b", AppName: "tsa1:8:tenant-b:support", UserID: "user-b"}, nil
}

type wrongScopeMemoryService struct{ memory.Service }

func (wrongScopeMemoryService) ReadMemories(context.Context, memory.UserKey, int) ([]*memory.Entry, error) {
	return []*memory.Entry{{ID: "memory-b", AppName: "tsa1:8:tenant-b:support", UserID: "user-b", Memory: &memory.Memory{Memory: "secret"}}}, nil
}

func TestStrictFencedServicesRejectBackendReturnedScopeMismatch(t *testing.T) {
	authorizer := &countingFenceAuthorizer{}
	sessionService, err := NewStrictFencedSessionService(wrongScopeSessionService{sessioninmem.NewSessionService()}, authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer sessionService.Close()
	if _, err := sessionService.GetSession(strictFenceContext(nil), session.Key{
		AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a",
	}); !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("wrong returned session error=%v, want ErrScopeMismatch", err)
	}

	memoryService, err := NewStrictFencedMemoryService(wrongScopeMemoryService{inmemory.NewMemoryService()}, authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer memoryService.Close()
	if _, err := memoryService.ReadMemories(strictFenceContext(nil), memory.UserKey{
		AppName: "tsa1:8:tenant-a:support", UserID: "user-a",
	}, 10); !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("wrong returned memory error=%v, want ErrScopeMismatch", err)
	}
}
