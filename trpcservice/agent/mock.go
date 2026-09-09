// Package agent builds service-owned tRPC-Agent-Go agents and offline models.
package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// MockModel is a deterministic offline model for tests and quickstarts.
type MockModel struct{}

// GenerateContent echoes the latest user or tool content and closes its channel.
func (MockModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if request == nil {
		return nil, errors.New("mock model: nil request")
	}
	content := "ok"
	var latest model.Message
	for i := len(request.Messages) - 1; i >= 0; i-- {
		if request.Messages[i].Content != "" {
			latest = request.Messages[i]
			content = request.Messages[i].Content
			break
		}
	}
	responseMessage := model.NewAssistantMessage("echo: " + content)
	if latest.Role == model.RoleUser && latest.Content == "calculate 6*7" {
		responseMessage = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{Type: "function", ID: "mock-calculation", Function: model.FunctionDefinitionParam{Name: "calculator", Arguments: []byte(`{"a":6,"b":7,"operation":"multiply"}`)}}}}
	} else if latest.Role == model.RoleTool {
		responseMessage = model.NewAssistantMessage("calculated: " + latest.Content)
	}
	responses := make(chan *model.Response, 1)
	select {
	case <-ctx.Done():
		close(responses)
		return responses, nil
	case responses <- &model.Response{ID: "mock-response", Object: model.ObjectTypeChatCompletion, Model: "offline-mock", Done: true, Choices: []model.Choice{{Index: 0, Message: responseMessage}}}:
		close(responses)
		return responses, nil
	}
}

// Info describes the offline model.
func (MockModel) Info() model.Info { return model.Info{Name: "offline-mock", ContextWindow: 4096} }

type echoInput struct {
	Text string `json:"text"`
}
type echoOutput struct {
	Text string `json:"text"`
}
type calculatorInput struct {
	A         float64 `json:"a"`
	B         float64 `json:"b"`
	Operation string  `json:"operation"`
}
type calculatorOutput struct {
	Result float64 `json:"result"`
}

// Tools resolves the configured allow/deny policy to known offline tools.
func Tools(allow, deny []string) ([]tool.Tool, []string, error) {
	return ToolsWithExtras(allow, deny, nil)
}

// ToolsWithExtras applies the same allow/deny policy to Bundle-owned dynamic
// tools such as tenant-scoped knowledge search.
func ToolsWithExtras(allow, deny []string, extras map[string]tool.Tool) ([]tool.Tool, []string, error) {
	denied := make(map[string]struct{}, len(deny))
	for _, name := range deny {
		denied[name] = struct{}{}
	}
	registry := map[string]tool.Tool{
		"echo":       function.NewFunctionTool(func(_ context.Context, in echoInput) (echoOutput, error) { return echoOutput{Text: in.Text}, nil }, function.WithName("echo"), function.WithDescription("Echo text without external access.")),
		"calculator": function.NewFunctionTool(calculate, function.WithName("calculator"), function.WithDescription("Calculate add, subtract, multiply, or divide.")),
	}
	for name, candidate := range extras {
		if name == "" || candidate == nil {
			return nil, nil, errors.New("agent: dynamic tool name and implementation are required")
		}
		if _, exists := registry[name]; exists {
			return nil, nil, fmt.Errorf("agent: dynamic tool %q conflicts with a built-in tool", name)
		}
		registry[name] = candidate
	}
	var selected []tool.Tool
	var names []string
	for _, name := range allow {
		if _, blocked := denied[name]; blocked {
			continue
		}
		candidate, ok := registry[name]
		if !ok {
			return nil, nil, fmt.Errorf("agent: configured tool %q is unavailable", name)
		}
		selected = append(selected, candidate)
		names = append(names, name)
	}
	return selected, names, nil
}

func calculate(_ context.Context, in calculatorInput) (calculatorOutput, error) {
	switch strings.ToLower(in.Operation) {
	case "add", "+":
		return calculatorOutput{Result: in.A + in.B}, nil
	case "subtract", "-":
		return calculatorOutput{Result: in.A - in.B}, nil
	case "multiply", "*":
		return calculatorOutput{Result: in.A * in.B}, nil
	case "divide", "/":
		if in.B == 0 {
			return calculatorOutput{}, errors.New("calculator: division by zero")
		}
		return calculatorOutput{Result: in.A / in.B}, nil
	default:
		return calculatorOutput{}, fmt.Errorf("calculator: unsupported operation %q", in.Operation)
	}
}
