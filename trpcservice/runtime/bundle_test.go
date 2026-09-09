package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func runtimeSnapshot(t *testing.T, tenantID string, version int) config.RuntimeSnapshot {
	return runtimeSnapshotWithTools(t, tenantID, version, "[echo, calculator]")
}

func TestBuildAgentSupportsPublishedWorkflowTypes(t *testing.T) {
	tests := []struct {
		name     string
		workflow tenant.WorkflowConfig
	}{
		{name: "llm", workflow: tenant.WorkflowConfig{Type: tenant.WorkflowLLM}},
		{name: "chain", workflow: tenant.WorkflowConfig{Type: tenant.WorkflowChain, Nodes: []tenant.WorkflowNode{{ID: "first", Instruction: "First."}, {ID: "second", Instruction: "Second."}}}},
		{name: "parallel", workflow: tenant.WorkflowConfig{Type: tenant.WorkflowParallel, Nodes: []tenant.WorkflowNode{{ID: "left", Instruction: "Left."}, {ID: "right", Instruction: "Right."}}, Aggregator: &tenant.WorkflowNode{ID: "merge", Instruction: "Merge."}}},
		{name: "cycle", workflow: tenant.WorkflowConfig{Type: tenant.WorkflowCycle, Nodes: []tenant.WorkflowNode{{ID: "review", Instruction: "Review."}}, MaxIterations: 2}},
		{name: "graph", workflow: tenant.WorkflowConfig{Type: tenant.WorkflowGraph, Nodes: []tenant.WorkflowNode{{ID: "start", Instruction: "Start."}, {ID: "finish", Instruction: "Finish."}}, Edges: []tenant.WorkflowEdge{{From: "start", To: "finish"}}, Entry: "start", Finish: "finish", MaxConcurrency: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := tenant.AgentApp{Name: "root", Config: tenant.AppConfig{Instruction: "Answer."}, Workflow: test.workflow}
			root, err := buildAgent(app, serviceagent.MockModel{}, nil, model.GenerationConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if root == nil {
				t.Fatal("workflow root agent is nil")
			}
		})
	}
}

func TestUserMessageBuildsMultimodalImageAndExtractedDocument(t *testing.T) {
	message, err := userMessage(RunInput{Text: "inspect attachments", Attachments: []gateway.Attachment{
		{Kind: "image", Name: "image.png", MIME: "image/png", Data: []byte{1, 2, 3}},
		{Kind: "file", Name: "notes.txt", MIME: "text/plain", ExtractedText: "document content"},
	}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(message.ContentParts) != 1 || message.ContentParts[0].Image == nil || message.ContentParts[0].Image.Format != "png" {
		t.Fatalf("content parts=%+v", message.ContentParts)
	}
	if !strings.Contains(message.Content, "[Document notes.txt]") || !strings.Contains(message.Content, "document content") {
		t.Fatalf("content=%q", message.Content)
	}
}

func TestUserMessageRejectsImageForTextOnlyModel(t *testing.T) {
	_, err := userMessage(RunInput{Text: "inspect", Attachments: []gateway.Attachment{{Kind: "image", MIME: "image/png", Data: []byte{1}}}}, false)
	if err == nil {
		t.Fatal("text-only model accepted image input")
	}
}

func runtimeSnapshotWithTools(t *testing.T, tenantID string, version int, allowed string) config.RuntimeSnapshot {
	t.Helper()
	payload := fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: %s
  name: Tenant
  enabled: true
  config_version: %d
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
    config: {instruction: Echo the user.}
    model: {provider: mock, name: offline-mock}
    tools: {allow: %s, deny: []}
    channels: [{binding_id: http, type: http, provider_account_id: local, enabled: true}]
    storage:
      session: {type: inmemory}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: inmemory}
      audit: {type: inmemory}
`, tenantID, version, allowed)
	file, err := config.Load(strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := file.Snapshot(tenantID, "assistant")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type externalCallable struct{ name string }

func (tool externalCallable) Declaration() *trpctool.Declaration {
	return &trpctool.Declaration{Name: tool.name, Description: "external fixture", InputSchema: &trpctool.Schema{Type: "object"}}
}
func (externalCallable) Call(context.Context, []byte) (any, error) {
	return map[string]any{"ok": true}, nil
}

func TestBundleOwnsVersionPinnedExternalToolLifecycle(t *testing.T) {
	snapshot := runtimeSnapshotWithTools(t, "tenant-a", 1, "[business_lookup]")
	services, err := storage.NewTestServices(snapshot.App().Storage)
	if err != nil {
		t.Fatal(err)
	}
	var closes atomic.Int32
	bundle, err := NewBundleWithServicesAndTools(snapshot, services, map[string]trpctool.Tool{"business_lookup": externalCallable{name: "business_lookup"}}, func() error {
		closes.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if names := bundle.ToolNames(); len(names) != 1 || names[0] != "business_lookup" {
		t.Fatalf("external tools = %v", names)
	}
	if err := bundle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bundle.Close(); err != nil {
		t.Fatal(err)
	}
	if closes.Load() != 1 {
		t.Fatalf("external close count = %d", closes.Load())
	}
}

func TestBundleRunsPublicTRPCAgentChain(t *testing.T) {
	bundle, err := NewTestBundle(runtimeSnapshot(t, "tenant-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	result, err := bundle.Run(context.Background(), RunInput{RequestID: "inbox-1", UserID: "user/a", SessionID: "dm/http/a", Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	var completion bool
	for _, item := range result.Events {
		if item.RequestID != "inbox-1" {
			t.Fatalf("request ID=%q", item.RequestID)
		}
		completion = completion || item.IsRunnerCompletion()
	}
	if !completion {
		t.Fatal("runner completion event missing")
	}
	saved, err := bundle.services.Session.GetSession(context.Background(), session.Key{AppName: bundle.AppName(), UserID: "user/a", SessionID: "dm/http/a"})
	if err != nil || saved == nil {
		t.Fatalf("saved session=%v err=%v", saved, err)
	}
	if got := bundle.ToolNames(); len(got) != 2 || got[0] != "echo" || got[1] != "calculator" {
		t.Fatalf("tools=%v", got)
	}
}

type observingRunner struct {
	release chan struct{}
}

func (runner *observingRunner) Run(context.Context, string, string, model.Message, ...trpcagent.RunOption) (<-chan *event.Event, error) {
	stream := make(chan *event.Event)
	go func() {
		stream <- &event.Event{RequestID: "observe"}
		<-runner.release
		close(stream)
	}()
	return stream, nil
}
func (*observingRunner) Close() error { return nil }

func TestBundleObserverReceivesEventBeforeRunCompletes(t *testing.T) {
	release := make(chan struct{})
	bundle := &Bundle{runner: &observingRunner{release: release}, drainTimeout: time.Second}
	observed := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		_, err := bundle.Run(context.Background(), RunInput{RequestID: "observe", UserID: "u", SessionID: "s", Text: "hello", Observer: func(*event.Event) { observed <- struct{}{} }})
		done <- err
	}()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("observer did not receive the live event")
	}
	select {
	case err := <-done:
		t.Fatalf("Run completed before stream release: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBundleExecutesCalculatorThroughRunner(t *testing.T) {
	bundle, err := NewTestBundle(runtimeSnapshot(t, "tenant-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	request := policy.Request{TenantID: "tenant-a", AppID: "assistant", UserID: "user", RequestID: "tool-request", Policy: tenant.ToolPolicy{Allow: []string{"calculator"}}}
	ctx := policy.WithRequest(context.Background(), &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}}, request)
	result, err := bundle.Run(ctx, RunInput{RequestID: "tool-request", UserID: "user", SessionID: "tool-session", Text: "calculate 6*7"})
	if err != nil {
		t.Fatal(err)
	}
	var toolResult, final bool
	for _, item := range result.Events {
		if item.Response == nil {
			continue
		}
		toolResult = toolResult || item.Response.IsToolResultResponse()
		for _, choice := range item.Choices {
			final = final || strings.Contains(choice.Message.Content, "42")
		}
	}
	if !toolResult || !final {
		t.Fatalf("toolResult=%v final=%v events=%d", toolResult, final, len(result.Events))
	}
}

func TestBundleResumesDangerousToolAfterApproval(t *testing.T) {
	bundle, err := NewTestBundle(runtimeSnapshot(t, "tenant-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	approvals := policy.NewMemoryApprovals()
	engine := &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}, Approvals: approvals}
	request := policy.Request{TenantID: "tenant-a", AppID: "assistant", UserID: "user", RequestID: "approval-request", Policy: tenant.ToolPolicy{Allow: []string{"calculator"}, RequireApproval: []string{"calculator"}}}
	controls, err := engine.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx := policy.WithRequest(context.Background(), engine, request)
	input := RunInput{RequestID: request.RequestID, UserID: request.UserID, SessionID: "approval-session", Text: "calculate 6*7", ToolFilter: controls.Visibility, ToolExecutionFilter: controls.Execution, ToolPermissionPolicy: controls.Permission}
	first, err := bundle.Run(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	var name, callID string
	var arguments []byte
	for _, item := range first.Events {
		if item == nil || item.Response == nil || !item.Response.IsToolCallResponse() {
			continue
		}
		for _, choice := range item.Choices {
			if len(choice.Message.ToolCalls) > 0 {
				call := choice.Message.ToolCalls[0]
				name, callID, arguments = call.Function.Name, call.ID, call.Function.Arguments
			}
		}
	}
	if name != "calculator" || callID == "" {
		t.Fatalf("pending tool name=%q callID=%q", name, callID)
	}
	if _, err := bundle.ResumeTool(ctx, ToolResume{Input: input, ToolName: name, ToolCallID: callID, Arguments: arguments}); !errors.Is(err, policy.ErrApprovalRequired) {
		t.Fatalf("unapproved ResumeTool error=%v", err)
	}
	approvals.Grant(request.TenantID, request.RequestID, name)
	resumed, err := bundle.ResumeTool(ctx, ToolResume{Input: input, ToolName: name, ToolCallID: callID, Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	var final bool
	for _, item := range resumed.Events {
		if item == nil || item.Response == nil {
			continue
		}
		for _, choice := range item.Choices {
			final = final || strings.Contains(choice.Message.Content, "42")
		}
	}
	if !final {
		t.Fatal("approved tool result did not resume the model")
	}
}

func TestBundlesIsolateSameUserAndSessionAcrossTenants(t *testing.T) {
	a, _ := NewTestBundle(runtimeSnapshot(t, "tenant-a", 1))
	defer a.Close()
	b, _ := NewTestBundle(runtimeSnapshot(t, "tenant-b", 1))
	defer b.Close()
	input := RunInput{RequestID: "request-a", UserID: "same-user", SessionID: "same-session", Text: "hello"}
	if _, err := a.Run(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if found, err := b.services.Session.GetSession(context.Background(), session.Key{AppName: b.AppName(), UserID: input.UserID, SessionID: input.SessionID}); err != nil || found != nil {
		t.Fatalf("tenant-b observed tenant-a session: %v %v", found, err)
	}
	if err := a.services.Memory.AddMemory(context.Background(), memory.UserKey{AppName: a.AppName(), UserID: input.UserID}, "tenant-a-memory", nil); err != nil {
		t.Fatal(err)
	}
	memories, err := b.services.Memory.ReadMemories(context.Background(), memory.UserKey{AppName: b.AppName(), UserID: input.UserID}, 10)
	if err != nil || len(memories) != 0 {
		t.Fatalf("tenant-b observed tenant-a memory: %v %v", memories, err)
	}
}

func TestManagerKeepsOldBundleWhileNewVersionRuns(t *testing.T) {
	manager, _ := NewManager(func(snapshot config.RuntimeSnapshot) (Runtime, error) { return NewTestBundle(snapshot) })
	oldLease, err := manager.Acquire(runtimeSnapshot(t, "tenant-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	newLease, err := manager.Acquire(runtimeSnapshot(t, "tenant-a", 2))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		lease   *Lease
		request string
	}{{oldLease, "old-request"}, {newLease, "new-request"}} {
		if _, err := test.lease.Runtime.Run(context.Background(), RunInput{RequestID: test.request, UserID: "user", SessionID: test.request, Text: "hello"}); err != nil {
			t.Fatalf("%s: %v", test.request, err)
		}
	}
	oldLease.Release()
	newLease.Release()
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type fakeRuntime struct{ closed atomic.Int32 }

func (*fakeRuntime) Run(context.Context, RunInput) (RunResult, error) { return RunResult{}, nil }
func (runtime *fakeRuntime) Close() error                             { runtime.closed.Add(1); return nil }

func TestManagerConcurrentBuildAndVersionDrain(t *testing.T) {
	var builds atomic.Int32
	var mu sync.Mutex
	var built []*fakeRuntime
	manager, _ := NewManager(func(config.RuntimeSnapshot) (Runtime, error) {
		builds.Add(1)
		runtime := &fakeRuntime{}
		mu.Lock()
		built = append(built, runtime)
		mu.Unlock()
		return runtime, nil
	})
	v1 := runtimeSnapshot(t, "tenant-a", 1)
	leases := make([]*Lease, 20)
	var wg sync.WaitGroup
	for i := range leases {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			lease, err := manager.Acquire(v1)
			if err != nil {
				t.Error(err)
				return
			}
			leases[index] = lease
		}(i)
	}
	wg.Wait()
	if builds.Load() != 1 {
		t.Fatalf("builds=%d", builds.Load())
	}
	v2lease, err := manager.Acquire(runtimeSnapshot(t, "tenant-a", 2))
	if err != nil {
		t.Fatal(err)
	}
	if built[0].closed.Load() != 0 {
		t.Fatal("old runtime closed with active leases")
	}
	if _, err := manager.Acquire(v1); !errors.Is(err, ErrStaleSnapshot) {
		t.Fatalf("stale Acquire() error=%v", err)
	}
	for _, lease := range leases {
		lease.Release()
		lease.Release()
	}
	if built[0].closed.Load() != 1 {
		t.Fatalf("old closes=%d", built[0].closed.Load())
	}
	v2lease.Release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if built[1].closed.Load() != 1 {
		t.Fatalf("new closes=%d", built[1].closed.Load())
	}
}

func TestManagerBuildsDifferentTenantsInParallel(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	manager, _ := NewManager(func(config.RuntimeSnapshot) (Runtime, error) {
		started <- struct{}{}
		<-release
		return &fakeRuntime{}, nil
	})
	results := make(chan error, 2)
	for _, snapshot := range []config.RuntimeSnapshot{runtimeSnapshot(t, "tenant-a", 1), runtimeSnapshot(t, "tenant-b", 1)} {
		go func(snapshot config.RuntimeSnapshot) {
			lease, err := manager.Acquire(snapshot)
			if err == nil {
				lease.Release()
			}
			results <- err
		}(snapshot)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("tenant builds were serialized")
		}
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerCloseWaitsForLeaseOrContext(t *testing.T) {
	manager, _ := NewManager(func(config.RuntimeSnapshot) (Runtime, error) { return &fakeRuntime{}, nil })
	lease, _ := manager.Acquire(runtimeSnapshot(t, "tenant-a", 1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := manager.Close(ctx); err == nil {
		t.Fatal("Close() error=nil with active lease")
	}
	lease.Release()
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type cancelRunner struct {
	stream        chan *event.Event
	closeOnCancel bool
	canceled      atomic.Bool
	once          sync.Once
}

func (run *cancelRunner) Run(context.Context, string, string, model.Message, ...trpcagent.RunOption) (<-chan *event.Event, error) {
	return run.stream, nil
}
func (*cancelRunner) Close() error { return nil }
func (run *cancelRunner) Cancel(string) bool {
	run.canceled.Store(true)
	if run.closeOnCancel {
		run.once.Do(func() { close(run.stream) })
	}
	return true
}
func (*cancelRunner) RunStatus(string) (runner.RunStatus, bool) { return runner.RunStatus{}, false }

func TestBundleCancelUsesManagedRunnerAndDrains(t *testing.T) {
	controlled := &cancelRunner{stream: make(chan *event.Event), closeOnCancel: true}
	bundle := &Bundle{appName: "tenant/a/app/b", runner: controlled, drainTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := bundle.Run(ctx, RunInput{RequestID: "request", UserID: "user", SessionID: "session", Text: "hello"})
	if err == nil || !controlled.canceled.Load() {
		t.Fatalf("Run() error=%v canceled=%v", err, controlled.canceled.Load())
	}
}

func TestBundleCancelHasBoundedDrain(t *testing.T) {
	controlled := &cancelRunner{stream: make(chan *event.Event)}
	bundle := &Bundle{appName: "tenant/a/app/b", runner: controlled, drainTimeout: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := bundle.Run(ctx, RunInput{RequestID: "request", UserID: "user", SessionID: "session", Text: "hello"})
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Run() error=%v, want drain timeout", err)
	}
}

func TestBundleRejectsEmptyUserText(t *testing.T) {
	bundle, err := NewTestBundle(runtimeSnapshot(t, "tenant-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	if _, err := bundle.Run(context.Background(), RunInput{RequestID: "request", UserID: "user", SessionID: "session"}); err == nil {
		t.Fatal("empty text accepted")
	}
}

type namedPlugin string

func (value namedPlugin) Name() string        { return string(value) }
func (namedPlugin) Register(*plugin.Registry) {}

func TestNewBundleRejectsDuplicatePluginsWithoutPanic(t *testing.T) {
	if _, err := NewTestBundle(runtimeSnapshot(t, "tenant-a", 1), namedPlugin("duplicate"), namedPlugin("duplicate")); err == nil {
		t.Fatal("duplicate plugins accepted")
	}
}
