package runtimeadapter

import (
	"context"
	"errors"
	"slices"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/workspaceadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func validateWorkspacePlan(p domain.Plan) error {
	used := map[string]bool{}
	for _, n := range p.Nodes {
		if n.Workspace == nil {
			continue
		}
		w := n.Workspace
		if n.Kind != "llm" || (protocol.ManifestWorkspace{ExecutorResource: w.ExecutorResource, Tools: w.Tools}).Validate() != nil {
			return application.ErrManifestInvalid
		}
		r, ok := p.Executors[w.ExecutorResource]
		if !ok || (protocol.ManifestExecutorResource{Kind: r.Kind, AdapterVersion: r.AdapterVersion}).Validate() != nil {
			return application.ErrManifestInvalid
		}
		if slices.Contains(w.Tools, "workspace_save_artifact") && (!n.Artifact || p.Artifact == nil) {
			return application.ErrManifestInvalid
		}
		used[w.ExecutorResource] = true
	}
	if len(used) != len(p.Executors) {
		return application.ErrManifestInvalid
	}
	return nil
}

// Only explicitly selected workspace Runs enter the SDK workspace permit. The
// original Run context covers both waiting and execution. Cleanup finishes before
// any candidate can be staged, including cancellation/error returns.
func (a *attempt) executeWorkspace(ctx context.Context, request trpcagent.Request) (result trpcagent.Result, err error) {
	if len(a.plan.Executors) == 0 {
		return a.executor.Execute(ctx, request)
	}
	workspace, err := workspaceadapter.Open(ctx, "/workspace")
	if err != nil {
		return result, err
	}
	defer func() {
		if closeErr := workspace.Close(); closeErr != nil {
			result = trpcagent.Result{}
			err = errors.Join(err, closeErr)
		}
	}()
	if err = a.check(ctx); err != nil {
		return result, err
	}
	request.Workspace = &trpcagent.WorkspaceConfig{ExecTool: workspace.ExecTool()}
	for _, n := range a.plan.Nodes {
		if n.Workspace != nil && slices.Contains(n.Workspace.Tools, "workspace_save_artifact") {
			request.Workspace.SaveArtifactTool = workspace.SaveArtifactTool()
		}
	}
	return a.executor.Execute(ctx, request)
}
