package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/redis/go-redis/v9"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// This runner exercises the exact permission policy and request context
// assembled by Worker, without using a paid model to propose a tool call.
type approvalIntegrationRunner struct{}

func (*approvalIntegrationRunner) Run(ctx context.Context, _, _ string, _ model.Message, options ...agent.RunOption) (<-chan *event.Event, error) {
	var opts agent.RunOptions
	for _, option := range options {
		option(&opts)
	}
	for _, manager := range opts.Plugins {
		defer manager.Close(context.Background())
	}
	if opts.ToolPermissionPolicy == nil {
		return nil, errors.New("worker omitted permission policy")
	}
	decision, err := opts.ToolPermissionPolicy.CheckToolPermission(ctx, &tool.PermissionRequest{
		ToolName: "calculator", Arguments: []byte(`{"expression":"2+2"}`),
	})
	if err != nil {
		return nil, err
	}
	out := make(chan *event.Event, 1)
	out <- event.NewResponseEvent("approval-run", "test-agent", &model.Response{
		Object: model.ObjectTypeRunnerCompletion, Done: true, Choices: []model.Choice{{Message: model.Message{
			Role: model.RoleAssistant, Content: string(decision.Action) + " " + decision.Reason,
		}}},
	})
	close(out)
	return out, nil
}

func (*approvalIntegrationRunner) Close() error { return nil }

func TestWorkerConsumesApprovalIssuedByAnotherNode(t *testing.T) {
	server := miniredis.RunT(t)
	newNode := func() *Service {
		client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
		t.Cleanup(func() { _ = client.Close() })
		manager := agentruntime.NewManager()
		sessions := sessioninmemory.NewSessionService()
		t.Cleanup(func() { _ = sessions.Close(); _ = manager.Close() })
		service := NewService(manager, nil, nil, nil, nil, nil, Options{
			RunTimeout: time.Second,
			Approvals:  governance.NewRedisApprovalStore(client, "worker-test"),
		})
		t.Cleanup(func() { _ = service.coordinator.Close() })
		service.acquireRuntime = func(context.Context, config.TenantConfig) (*agentruntime.Runtime, func(), error) {
			return &agentruntime.Runtime{Runner: &approvalIntegrationRunner{}, AppNamespace: "shared-app", Session: sessions}, func() {}, nil
		}
		return service
	}
	first, second := newNode(), newNode()
	tenant := testTenant("approval-tenant")
	tenant.Tools.RequireConfirm = []string{"calculator"}
	request := taskFor(tenant, "request", "alice", "group", domain.ScopeGroup)
	result, err := first.Process(context.Background(), request)
	if err != nil || !strings.HasPrefix(result.Text, string(tool.PermissionActionAsk)) {
		t.Fatalf("initial confirmation=%+v err=%v", result, err)
	}
	_, nonce := governance.ExtractApproval(result.Text)
	if nonce == "" {
		t.Fatal("worker did not return an approval token")
	}
	// A different member of the same group shares the session but cannot
	// consume Alice's authorization. The failed attempt must not burn it.
	other := taskFor(tenant, "other-user", "bob", "group", domain.ScopeGroup)
	other.Message.Text += " #approve:" + nonce
	result, err = second.Process(context.Background(), other)
	if err != nil || !strings.HasPrefix(result.Text, string(tool.PermissionActionAsk)) {
		t.Fatalf("other group user confirmation=%+v err=%v", result, err)
	}
	confirm := taskFor(tenant, "confirm", "alice", "group", domain.ScopeGroup)
	confirm.Message.Text += " #approve:" + nonce
	result, err = second.Process(context.Background(), confirm)
	if err != nil || !strings.HasPrefix(result.Text, string(tool.PermissionActionAllow)) {
		t.Fatalf("confirmation on second node=%+v err=%v", result, err)
	}
	confirm.Message.ExternalMessageID = "replay-confirmation"
	result, err = first.Process(context.Background(), confirm)
	if err != nil || !strings.HasPrefix(result.Text, string(tool.PermissionActionAsk)) {
		t.Fatalf("consumed approval replay=%+v err=%v", result, err)
	}
}
