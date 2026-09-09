package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestActorScopedMemoryServiceSeparatesGroupActors(t *testing.T) {
	base := inmemory.NewMemoryService()
	defer base.Close()
	scoped, err := newActorScopedMemoryService(base, "tenant-app")
	if err != nil {
		t.Fatal(err)
	}

	const owner = "group_0123456789abcdef"
	aliceCtx := ContextWithActorIdentity(context.Background(), ActorIdentity{
		UserID: "alice", SessionOwnerID: owner, IsGroupChat: true,
	})
	bobCtx := ContextWithActorIdentity(context.Background(), ActorIdentity{
		UserID: "bob", SessionOwnerID: owner, IsGroupChat: true,
	})
	ownerKey := memory.UserKey{AppName: "tenant-app", UserID: owner}
	if err := scoped.AddMemory(aliceCtx, ownerKey, "Alice prefers tea", []string{"preference"}); err != nil {
		t.Fatalf("add Alice memory: %v", err)
	}
	if err := scoped.AddMemory(bobCtx, ownerKey, "Bob prefers coffee", []string{"preference"}); err != nil {
		t.Fatalf("add Bob memory: %v", err)
	}

	alice, err := scoped.ReadMemories(aliceCtx, ownerKey, 10)
	if err != nil {
		t.Fatalf("read Alice memories: %v", err)
	}
	if len(alice) != 1 || alice[0].UserID != "alice" || alice[0].Memory.Memory != "Alice prefers tea" {
		t.Fatalf("unexpected Alice memories: %#v", alice)
	}
	bob, err := scoped.ReadMemories(bobCtx, ownerKey, 10)
	if err != nil {
		t.Fatalf("read Bob memories: %v", err)
	}
	if len(bob) != 1 || bob[0].UserID != "bob" || bob[0].Memory.Memory != "Bob prefers coffee" {
		t.Fatalf("unexpected Bob memories: %#v", bob)
	}
}

func TestActorScopedMemoryServiceRejectsEscapedKeysAndMissingIdentity(t *testing.T) {
	base := inmemory.NewMemoryService()
	defer base.Close()
	scoped, err := newActorScopedMemoryService(base, "tenant-app")
	if err != nil {
		t.Fatal(err)
	}
	key := memory.UserKey{AppName: "tenant-app", UserID: "group_owner"}
	if _, err := scoped.ReadMemories(context.Background(), key, 1); !errors.Is(err, ErrActorIdentityRequired) {
		t.Fatalf("missing actor error=%v, want ErrActorIdentityRequired", err)
	}
	ctx := ContextWithActorIdentity(context.Background(), ActorIdentity{
		UserID: "alice", SessionOwnerID: "group_owner", IsGroupChat: true,
	})
	if _, err := scoped.ReadMemories(ctx, memory.UserKey{AppName: "other-app", UserID: "group_owner"}, 1); !errors.Is(err, ErrMemoryScopeViolation) {
		t.Fatalf("cross-app error=%v, want ErrMemoryScopeViolation", err)
	}
	if _, err := scoped.ReadMemories(ctx, memory.UserKey{AppName: "tenant-app", UserID: "bob"}, 1); !errors.Is(err, ErrMemoryScopeViolation) {
		t.Fatalf("cross-owner error=%v, want ErrMemoryScopeViolation", err)
	}
}

type recordingMemoryService struct {
	memory.Service
	enqueued []*session.Session
}

func (s *recordingMemoryService) EnqueueAutoMemoryJob(_ context.Context, sess *session.Session) error {
	s.enqueued = append(s.enqueued, sess)
	return nil
}

func TestActorScopedMemoryServiceSkipsGroupAutoExtractionAndMapsDirectSession(t *testing.T) {
	base := &recordingMemoryService{Service: inmemory.NewMemoryService()}
	defer base.Close()
	scoped, err := newActorScopedMemoryService(base, "tenant-app")
	if err != nil {
		t.Fatal(err)
	}
	groupCtx := ContextWithActorIdentity(context.Background(), ActorIdentity{
		UserID: "alice", SessionOwnerID: "group_owner", IsGroupChat: true,
	})
	groupSession := session.NewSession("tenant-app", "group_owner", "group-session")
	if err := scoped.EnqueueAutoMemoryJob(groupCtx, groupSession); err != nil {
		t.Fatalf("group enqueue returned error: %v", err)
	}
	if len(base.enqueued) != 0 {
		t.Fatal("group transcript was sent to personal auto-memory extraction")
	}

	directCtx := ContextWithActorIdentity(context.Background(), ActorIdentity{
		UserID: "alice", SessionOwnerID: "alice", IsGroupChat: false,
	})
	directSession := session.NewSession("tenant-app", "alice", "direct-session")
	if err := scoped.EnqueueAutoMemoryJob(directCtx, directSession); err != nil {
		t.Fatalf("direct enqueue returned error: %v", err)
	}
	if len(base.enqueued) != 1 || base.enqueued[0] == directSession || base.enqueued[0].UserID != "alice" {
		t.Fatalf("direct session was not safely copied: %#v", base.enqueued)
	}
}

type capturedMemoryTool struct {
	calls atomic.Int32
}

func (*capturedMemoryTool) Declaration() *tool.Declaration {
	return &tool.Declaration{Name: memory.SearchToolName}
}

func (t *capturedMemoryTool) Call(context.Context, []byte) (any, error) {
	t.calls.Add(1)
	return nil, nil
}

type capturedToolsMemoryService struct {
	memory.Service
	tools []tool.Tool
}

func (s *capturedToolsMemoryService) Tools() []tool.Tool {
	return append([]tool.Tool(nil), s.tools...)
}

func TestActorScopedMemoryServiceDoesNotExposeCapturedBaseTool(t *testing.T) {
	captured := &capturedMemoryTool{}
	base := &capturedToolsMemoryService{
		Service: inmemory.NewMemoryService(),
		tools:   []tool.Tool{captured},
	}
	defer base.Close()
	scoped, err := newActorScopedMemoryService(base, "tenant-app")
	if err != nil {
		t.Fatal(err)
	}
	tools := scoped.Tools()
	if len(tools) != 1 || tools[0] == captured {
		t.Fatalf("scoped tools=%#v, want a fresh actor-aware standard tool", tools)
	}
	callable, ok := tools[0].(tool.CallableTool)
	if !ok {
		t.Fatalf("scoped standard tool %T is not callable", tools[0])
	}
	owner := "group_0123456789abcdef"
	ctx := ContextWithActorIdentity(context.Background(), ActorIdentity{
		UserID: "alice", SessionOwnerID: owner, IsGroupChat: true,
	})
	invocation := agent.NewInvocation(
		agent.WithInvocationSession(session.NewSession("tenant-app", owner, "group-session")),
		agent.WithInvocationMemoryService(scoped),
	)
	ctx = agent.NewInvocationContext(ctx, invocation)
	if _, err := callable.Call(ctx, []byte(`{"query":"tea"}`)); err != nil {
		t.Fatalf("actor-aware standard memory tool failed: %v", err)
	}
	if got := captured.calls.Load(); got != 0 {
		t.Fatalf("captured base tool was called %d times", got)
	}
	entries, err := scoped.ReadMemories(ctx, memory.UserKey{AppName: "tenant-app", UserID: owner}, 10)
	if err != nil {
		t.Fatalf("read actor memories: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("unexpected memories from empty actor scope: %#v", entries)
	}
}

func TestActorScopedMemoryServiceOmitsUnknownNativeTools(t *testing.T) {
	base := &capturedToolsMemoryService{
		Service: inmemory.NewMemoryService(),
		tools:   []tool.Tool{&namedTestTool{name: "conversation_search"}},
	}
	defer base.Close()
	scoped, err := newActorScopedMemoryService(base, "tenant-app")
	if err != nil {
		t.Fatal(err)
	}
	if got := scoped.Tools(); len(got) != 0 {
		t.Fatalf("unknown native tools leaked through actor scope: %#v", got)
	}
}

type namedTestTool struct{ name string }

func (t *namedTestTool) Declaration() *tool.Declaration { return &tool.Declaration{Name: t.name} }
