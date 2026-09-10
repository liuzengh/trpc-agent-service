package agent

import (
	agenttrace "trpc.group/trpc-go/trpc-agent-go/agent/trace"
	"trpc.group/trpc-go/trpc-agent-go/model"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func projectExecutionTrace(source *agenttrace.Trace) *storage.AgentExecutionTrace {
	if source == nil {
		return nil
	}
	projected := &storage.AgentExecutionTrace{
		Status:           string(source.Status),
		RootAgentName:    source.RootAgentName,
		RootInvocationID: source.RootInvocationID,
		SessionID:        source.SessionID,
		StartedAt:        source.StartedAt,
		EndedAt:          source.EndedAt,
		Usage:            projectTraceUsage(source.Usage),
		Steps:            make([]storage.ExecutionTraceStep, 0, len(source.Steps)),
	}
	for _, step := range source.Steps {
		projected.Steps = append(projected.Steps, storage.ExecutionTraceStep{
			StepID:             step.StepID,
			InvocationID:       step.InvocationID,
			ParentInvocationID: step.ParentInvocationID,
			AgentName:          step.AgentName,
			Branch:             step.Branch,
			NodeID:             step.NodeID,
			NodeType:           step.NodeType,
			StartedAt:          step.StartedAt,
			EndedAt:            step.EndedAt,
			PredecessorStepIDs: append([]string(nil), step.PredecessorStepIDs...),
			AppliedSurfaceIDs:  append([]string(nil), step.AppliedSurfaceIDs...),
			Usage:              projectTraceUsage(step.Usage),
			Failed:             step.Error != "",
		})
	}
	return projected
}

func projectTraceUsage(usage *model.Usage) *storage.ExecutionTraceUsage {
	if usage == nil {
		return nil
	}
	return &storage.ExecutionTraceUsage{
		PromptTokens:        usage.PromptTokens,
		CompletionTokens:    usage.CompletionTokens,
		TotalTokens:         usage.TotalTokens,
		CachedTokens:        usage.PromptTokensDetails.CachedTokens,
		CacheCreationTokens: usage.PromptTokensDetails.CacheCreationTokens,
		CacheReadTokens:     usage.PromptTokensDetails.CacheReadTokens,
		ReasoningTokens:     usage.CompletionTokensDetails.ReasoningTokens,
	}
}
