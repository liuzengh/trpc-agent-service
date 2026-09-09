package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
)

func TestRedisPendingStoreTTLAndDelete(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisPendingStore(client, 10*time.Minute)
	if err != nil {
		t.Fatalf("NewRedisPendingStore() error = %v", err)
	}
	call := testPendingCall()
	if err := store.Save(context.Background(), call); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, err := store.Get(context.Background(), call.SessionID)
	if err != nil || got.ToolName != call.ToolName {
		t.Fatalf("Get() = %#v, %v", got, err)
	}
	if err := store.Delete(context.Background(), call.SessionID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Get(context.Background(), call.SessionID); !errors.Is(err, ErrNoPendingConfirmation) {
		t.Fatalf("Get() after Delete error = %v", err)
	}
	if err := store.Save(context.Background(), call); err != nil {
		t.Fatalf("second Save() error = %v", err)
	}
	server.FastForward(11 * time.Minute)
	if _, err := store.Get(context.Background(), call.SessionID); !errors.Is(err, ErrNoPendingConfirmation) {
		t.Fatalf("Get() after TTL error = %v", err)
	}
}

func TestConfirmationCallbackInterceptsDangerousTool(t *testing.T) {
	store := &memoryPendingStore{}
	registry := &confirmationRegistry{
		dangerous: map[string]bool{"delete_all": true},
		tools:     map[string]agenttool.Tool{"delete_all": &callableTestTool{name: "delete_all"}},
	}
	gate, err := NewRedisConfirmationGate(store, registry)
	if err != nil {
		t.Fatalf("NewRedisConfirmationGate() error = %v", err)
	}
	snapshot := workerSnapshot()
	invocation := agent.NewInvocation(agent.WithInvocationSession(&session.Session{
		ID: "tenant-a:webui:user-1",
	}))
	ctx := agent.NewInvocationContext(context.Background(), invocation)
	callback := gate.Callbacks(snapshot).BeforeTool[0]
	result, err := callback(ctx, &agenttool.BeforeToolArgs{
		ToolCallID: "call-1",
		ToolName:   "delete_all",
		Arguments:  []byte(`{"scope":"all"}`),
	})
	if err != nil {
		t.Fatalf("BeforeTool callback error = %v", err)
	}
	if result.CustomResult == nil {
		t.Fatal("dangerous tool was not intercepted")
	}
	if store.call.ToolName != "delete_all" || store.call.SessionID != "tenant-a:webui:user-1" {
		t.Fatalf("stored pending call = %#v", store.call)
	}
}

func TestConfirmationResolveBranches(t *testing.T) {
	tests := []struct {
		name         string
		decision     string
		wantDecision string
		wantCalls    int
	}{
		{name: "confirm", decision: "确认", wantDecision: "tool_confirmed", wantCalls: 1},
		{name: "cancel", decision: "取消", wantDecision: "tool_denied"},
		{name: "waiting", decision: "later", wantDecision: "tool_confirm_pending"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			toolInstance := &callableTestTool{name: "delete_all"}
			store := &memoryPendingStore{call: testPendingCall(), present: true}
			registry := &confirmationRegistry{
				dangerous: map[string]bool{"delete_all": true},
				tools:     map[string]agenttool.Tool{"delete_all": toolInstance},
			}
			gate, err := NewRedisConfirmationGate(store, registry)
			if err != nil {
				t.Fatalf("NewRedisConfirmationGate() error = %v", err)
			}
			stream, handled, err := gate.Resolve(
				context.Background(),
				workerSnapshot(),
				"tenant-a:webui:user-1",
				[]storage.UserEvent{{Text: test.decision}},
				[]agenttool.Tool{toolInstance},
			)
			if err != nil || !handled {
				t.Fatalf("Resolve() handled=%v error=%v", handled, err)
			}
			events := collectEvents(stream)
			if !hasDecision(events, test.wantDecision) {
				t.Fatalf("events = %#v, want decision %q", events, test.wantDecision)
			}
			if toolInstance.calls != test.wantCalls {
				t.Fatalf("tool calls = %d, want %d", toolInstance.calls, test.wantCalls)
			}
			if test.decision != "later" && store.present {
				t.Fatal("terminal decision did not clear pending call")
			}
		})
	}
}

func TestConfirmationResolveNoPending(t *testing.T) {
	gate, err := NewRedisConfirmationGate(&memoryPendingStore{}, &confirmationRegistry{})
	if err != nil {
		t.Fatalf("NewRedisConfirmationGate() error = %v", err)
	}
	stream, handled, err := gate.Resolve(
		context.Background(), workerSnapshot(), "session",
		[]storage.UserEvent{{Text: "确认"}}, nil,
	)
	if err != nil || handled || stream != nil {
		t.Fatalf("Resolve(no pending) = %v, %v, %v", stream, handled, err)
	}
}

func testPendingCall() PendingToolCall {
	return PendingToolCall{
		TenantID:   "tenant-a",
		AppID:      "app-a",
		SessionID:  "tenant-a:webui:user-1",
		ToolCallID: "call-1",
		ToolName:   "delete_all",
		Arguments:  json.RawMessage(`{"scope":"all"}`),
		CreatedAt:  time.Now(),
	}
}

type memoryPendingStore struct {
	call    PendingToolCall
	present bool
	err     error
}

func (m *memoryPendingStore) Save(_ context.Context, call PendingToolCall) error {
	if m.err != nil {
		return m.err
	}
	m.call = call
	m.present = true
	return nil
}
func (m *memoryPendingStore) Get(context.Context, string) (PendingToolCall, error) {
	if m.err != nil {
		return PendingToolCall{}, m.err
	}
	if !m.present {
		return PendingToolCall{}, ErrNoPendingConfirmation
	}
	return m.call, nil
}
func (m *memoryPendingStore) Delete(context.Context, string) error {
	if m.err != nil {
		return m.err
	}
	m.present = false
	return nil
}

type confirmationRegistry struct {
	dangerous map[string]bool
	tools     map[string]agenttool.Tool
}

func (r *confirmationRegistry) IsDangerous(name string) bool {
	return r.dangerous[name]
}
func (r *confirmationRegistry) Lookup(name string) (agenttool.Tool, bool) {
	value, ok := r.tools[name]
	return value, ok
}

type callableTestTool struct {
	name  string
	calls int
}

func (t *callableTestTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{
		Name: t.name, InputSchema: &agenttool.Schema{Type: "object"},
	}
}
func (t *callableTestTool) Call(context.Context, []byte) (any, error) {
	t.calls++
	return map[string]any{"deleted": true}, nil
}

func hasDecision(events []Event, decision string) bool {
	for _, event := range events {
		if event.Decision == decision {
			return true
		}
	}
	return false
}
