package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func invokeTool(ctx context.Context, invoker ToolInvoker, tc tenant.TenantContext, spec AgentSpec, name string, arguments map[string]any) (ToolResult, []RunnerEvent, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return ToolResult{}, nil, fmt.Errorf("%w: tool name is required", ErrInvalidInput)
	}
	started := RunnerEvent{Type: "tool.started", Role: "tool", ToolName: name}
	if invoker == nil {
		err := fmt.Errorf("%w: tool invoker is not configured", ErrToolFailure)
		failed := RunnerEvent{Type: "tool.failed", Role: "tool", ToolName: name, ErrorType: "not_configured"}
		return ToolResult{}, []RunnerEvent{started, failed}, err
	}
	result, err := invoker.Invoke(ctx, ToolRequest{TenantContext: cloneTenantContext(tc), Agent: cloneSpec(spec), ToolName: name, Arguments: cloneArguments(arguments)})
	if err != nil {
		errorType := "tool"
		if errors.Is(err, context.DeadlineExceeded) {
			errorType = "deadline_exceeded"
		} else if errors.Is(err, context.Canceled) {
			errorType = "canceled"
		}
		failed := RunnerEvent{Type: "tool.failed", Role: "tool", ToolName: name, ErrorType: errorType}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ToolResult{}, []RunnerEvent{started, failed}, err
		}
		return ToolResult{}, []RunnerEvent{started, failed}, fmt.Errorf("%w: %w", ErrToolFailure, err)
	}
	if result.IsError {
		return result, []RunnerEvent{started, {Type: "tool.failed", Role: "tool", ToolName: name, ErrorType: "tool_result", Content: result.Content}}, fmt.Errorf("%w: tool returned an error result", ErrToolFailure)
	}
	return result, []RunnerEvent{started, {Type: "tool.completed", Role: "tool", ToolName: name, Content: result.Content}}, nil
}

func cloneArguments(arguments map[string]any) map[string]any {
	if arguments == nil {
		return nil
	}
	out := make(map[string]any, len(arguments))
	for key, value := range arguments {
		out[key] = cloneArgumentValue(value)
	}
	return out
}

func cloneArgumentValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneArguments(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneArgumentValue(item)
		}
		return out
	default:
		return value
	}
}
