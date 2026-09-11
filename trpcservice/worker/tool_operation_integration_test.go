package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type afterToolMessagesManager interface {
	AfterToolMessages(context.Context, *plugin.AfterToolMessagesArgs) (*plugin.AfterToolMessagesResult, error)
}

type sideEffectIntegrationRunner struct {
	mu           sync.Mutex
	executions   int
	operationKey string
}

func (r *sideEffectIntegrationRunner) Run(
	ctx context.Context,
	_, _ string,
	_ model.Message,
	options ...agent.RunOption,
) (<-chan *event.Event, error) {
	runOptions := agent.RunOptions{}
	for _, option := range options {
		option(&runOptions)
	}
	if runOptions.ToolPermissionPolicy == nil || len(runOptions.Plugins) == 0 {
		return nil, errors.New("worker omitted tool guard run options")
	}
	manager := runOptions.Plugins[len(runOptions.Plugins)-1]
	defer manager.Close(context.Background())
	callbacks := manager.ToolCallbacks()
	if callbacks == nil {
		return nil, errors.New("tool guard callbacks are unavailable")
	}
	const callID = "worker-side-effect-call"
	arguments := []byte(`{"resource":"opaque-7"}`)
	before, err := callbacks.RunBeforeTool(ctx, &tool.BeforeToolArgs{
		ToolCallID: callID, ToolName: "tenant_admin_action", Arguments: arguments,
	})
	if err != nil {
		return nil, err
	}
	toolCtx := ctx
	if before != nil && before.Context != nil {
		toolCtx = before.Context
	}
	if before != nil && before.CustomResult != nil {
		return nil, errors.New("fresh operation was unexpectedly replayed")
	}
	decision, err := runOptions.ToolPermissionPolicy.CheckToolPermission(toolCtx, &tool.PermissionRequest{
		ToolCallID: callID, ToolName: "tenant_admin_action", Arguments: arguments,
	})
	if err != nil {
		return nil, err
	}
	if decision.Action != tool.PermissionActionAllow {
		return nil, errors.New("side-effect permission was not allowed")
	}
	operationKey, ok := tooloperation.OperationKeyFromContext(toolCtx)
	if !ok {
		return nil, errors.New("tool did not receive provider idempotency key")
	}
	r.mu.Lock()
	r.executions++
	r.operationKey = operationKey
	r.mu.Unlock()
	after, err := callbacks.RunAfterTool(toolCtx, &tool.AfterToolArgs{
		ToolCallID: callID, ToolName: "tenant_admin_action", Arguments: arguments,
		Result: map[string]any{"status": "done"},
	})
	if err != nil {
		return nil, err
	}
	if after != nil && after.Context != nil {
		toolCtx = after.Context
	}
	hooks, ok := manager.(afterToolMessagesManager)
	if !ok {
		return nil, errors.New("tool message finalizer is unavailable")
	}
	message := model.NewToolMessage(callID, "tenant_admin_action", `{"status":"done"}`)
	if _, err := hooks.AfterToolMessages(toolCtx, &plugin.AfterToolMessagesArgs{
		ToolCalls: []model.ToolCall{{ID: callID}}, ToolResultMessages: []model.Message{message},
	}); err != nil {
		return nil, err
	}
	return strictCompletionEvents("completed", 1, 1), nil
}

func (*sideEffectIntegrationRunner) Close() error { return nil }

func (r *sideEffectIntegrationRunner) snapshot() (int, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.executions, r.operationKey
}

func TestProcessWiresSideEffectGuardThroughRunnerLifecycle(t *testing.T) {
	ledger := tooloperation.NewMemory()
	runner := &sideEffectIntegrationRunner{}
	manager := agentruntime.NewManager()
	coordinator := coordination.NewInMemory()
	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() {
		_ = sessionService.Close()
		_ = manager.Close()
		_ = coordinator.Close()
	})
	service := NewService(
		manager, coordinator, channels.NewRegistry(), governance.NewFilter(), nil, metrics.NewMetrics(),
		Options{
			LockTTL: time.Minute, DedupTTL: time.Hour, RunTimeout: 5 * time.Second,
			ToolOperations: ledger,
		},
	)
	service.acquireRuntime = func(context.Context, config.TenantConfig) (*agentruntime.Runtime, func(), error) {
		return &agentruntime.Runtime{
			Runner: runner, AppNamespace: "ta_worker_guard", Session: sessionService,
		}, func() {}, nil
	}
	tenantConfig := testTenant("tenant-tool-guard")
	tenantConfig.Tools = config.ToolPolicy{
		Allow: []string{"tenant_admin_action"}, SideEffects: []string{"tenant_admin_action"},
	}
	result, err := service.Process(context.Background(), taskFor(
		tenantConfig, "side-effect-message", "user-1", "dm", domain.ScopeDirect,
	))
	if err != nil {
		t.Fatal(err)
	}
	executions, operationKey := runner.snapshot()
	if executions != 1 || operationKey == "" {
		t.Fatalf("executions=%d operation_key=%q", executions, operationKey)
	}
	record, err := ledger.Get(context.Background(), tenantConfig.TenantID, operationKey)
	if err != nil || record.State != tooloperation.StateConfirmed {
		t.Fatalf("operation record=%+v err=%v", record, err)
	}
	if len(result.Tools) < 2 {
		t.Fatalf("result omitted operation transitions: %+v", result.Tools)
	}
}
