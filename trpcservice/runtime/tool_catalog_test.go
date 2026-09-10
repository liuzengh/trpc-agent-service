package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworktodo "trpc.group/trpc-go/trpc-agent-go/tool/todo"
)

func TestToolCatalogAcceptsRegisteredTool(t *testing.T) {
	catalog := NewToolCatalog()
	policy := tenant.ToolPolicy{VisibleTools: []string{"todo_write"}}
	exec := worker.Execution{}
	exec.Config.Tools = policy

	tools, err := catalog.ResolveTools(context.Background(), exec)
	if err != nil || len(tools) != 1 || tools[0].Declaration().Name != "todo_write" {
		t.Fatalf("ResolveTools() = %#v, error = %v, want todo_write", tools, err)
	}
	if err := catalog.ValidateToolPolicy(context.Background(), policy); err != nil {
		t.Fatalf("ValidateToolPolicy() error = %v", err)
	}
}

func TestToolCatalogRejectsUnknownTool(t *testing.T) {
	catalog := NewToolCatalog()
	policy := tenant.ToolPolicy{VisibleTools: []string{"unknown"}}
	if err := catalog.ValidateToolPolicy(context.Background(), policy); err == nil ||
		!strings.Contains(err.Error(), "unsupported runtime tool") {
		t.Fatalf("ValidateToolPolicy() error = %v, want unsupported-tool rejection", err)
	}
}

func TestToolCatalogExposesSafetyMetadata(t *testing.T) {
	safety, ok := NewToolCatalog().Safety(frameworktodo.DefaultToolName)
	if !ok || safety != platformtool.SafetyIdempotent {
		t.Fatalf("todo safety = %q, %t; want idempotent", safety, ok)
	}
	if _, ok := NewToolCatalog().Safety("unknown"); ok {
		t.Fatal("unknown tool received safety metadata")
	}
}

func TestClassifyToolExecutionFailureMapsToWorkerPolicy(t *testing.T) {
	cause := errors.New("remote write may have happened")
	classified := classifyToolExecutionFailure(
		platformtool.FailureSideEffectResultUncertain,
		cause,
	)
	if !worker.IsSideEffectUncertainError(classified) || !errors.Is(classified, cause) {
		t.Fatalf("classified tool error = %v, want uncertain cause", classified)
	}
	if retryable := classifyToolExecutionFailure(
		platformtool.FailureInfrastructureRetryable,
		cause,
	); !worker.IsRetryableExecutionError(retryable) {
		t.Fatalf("retryable tool error = %v, want retryable", retryable)
	}
	known := classifyToolExecutionFailure(platformtool.FailureSideEffectResultKnown, cause)
	if !worker.IsPermanentExecutionError(known) {
		t.Fatalf("known-result tool error = %v, want permanent", known)
	}
}
