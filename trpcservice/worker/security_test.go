package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestToolPermissionPolicyIsTenantScoped(t *testing.T) {
	cases := []struct {
		name   string
		tenant string
		tools  []string
		want   frameworktool.PermissionAction
	}{
		{name: "tenant A allows configured tool", tenant: "tenant-a", tools: []string{"todo_write"}, want: frameworktool.PermissionActionAllow},
		{name: "tenant B denies absent tool", tenant: "tenant-b", want: frameworktool.PermissionActionDeny},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			w := Worker{}
			exec := securityTestExecution()
			exec.Tenant.TenantID = tt.tenant
			exec.Config.TenantID = tt.tenant
			exec.Config.Tools = tenant.ToolPolicy{ExecutableTools: tt.tools}

			decision, err := w.toolPermissionPolicy(exec).CheckToolPermission(context.Background(), &frameworktool.PermissionRequest{
				ToolName: "todo_write",
			})
			if err != nil {
				t.Fatalf("check tool permission: %v", err)
			}
			if decision.Action != tt.want {
				t.Fatalf("decision = %q, want %q", decision.Action, tt.want)
			}
		})
	}
}

func TestToolPermissionPolicyReturnsAskForReviewRequiredTool(t *testing.T) {
	w := Worker{}
	exec := securityTestExecution()
	exec.Config.Tools = tenant.ToolPolicy{
		ExecutableTools:     []string{"delete"},
		ReviewRequiredTools: []string{"delete"},
	}
	decision, err := w.toolPermissionPolicy(exec).CheckToolPermission(context.Background(), &frameworktool.PermissionRequest{
		ToolName: "delete",
	})
	if err != nil {
		t.Fatalf("check review-required tool: %v", err)
	}
	if decision.Action != frameworktool.PermissionActionAsk {
		t.Fatalf("decision = %q, want %q", decision.Action, frameworktool.PermissionActionAsk)
	}
}

func TestToolPermissionPolicyRejectsLostExecutionLease(t *testing.T) {
	w := Worker{
		LeaseValidator: executionLeaseValidatorFunc(func(context.Context, Execution, queue.Lease) error {
			return queue.ErrLeaseLost
		}),
	}
	exec := securityTestExecution()
	exec.Config.Tools = tenant.ToolPolicy{ExecutableTools: []string{"todo_write"}}
	ctx, err := ContextWithJobLease(context.Background(), queue.Lease{
		Owner: "worker-a", Token: "run-a", Until: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("attach execution lease: %v", err)
	}
	_, err = w.toolPermissionPolicy(exec).CheckToolPermission(ctx, &frameworktool.PermissionRequest{
		ToolName: "todo_write",
	})
	if !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("permission error = %v, want lease lost", err)
	}
}

type executionLeaseValidatorFunc func(context.Context, Execution, queue.Lease) error

func (f executionLeaseValidatorFunc) ValidateExecutionLease(ctx context.Context, exec Execution, lease queue.Lease) error {
	return f(ctx, exec, lease)
}

func securityTestExecution() Execution {
	return Execution{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID: "tenant-a", AppID: "app-a", ConfigVersion: "v1",
			SessionID: "session-1", SessionPrincipalID: "principal-1", UserID: "user-1", TraceID: "trace-1",
		},
		Config: tenant.AppConfig{
			TenantID: "tenant-a", AppID: "app-a", Version: "v1",
			Model: tenant.ModelConfig{Provider: "openai", Model: "gpt-test", APIKeyRef: tenant.SecretRef{Name: "model-key"}},
		},
	}
}
