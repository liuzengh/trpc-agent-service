package agent

import (
	"context"
	"strings"
	"time"

	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

type DeterministicAgent struct {
	name  string
	delay time.Duration
}

func NewDeterministicAgent(name string) *DeterministicAgent {
	return NewDeterministicAgentWithDelay(name, 0)
}

func NewDeterministicAgentWithDelay(name string, delay time.Duration) *DeterministicAgent {
	if name == "" {
		name = "deterministic-agent"
	}
	return &DeterministicAgent{name: name, delay: delay}
}

func (a *DeterministicAgent) Run(ctx context.Context, invocation *frameworkagent.Invocation) (<-chan *event.Event, error) {
	results := make(chan *event.Event, 3)
	go func() {
		defer close(results)
		if a.delay > 0 {
			timer := time.NewTimer(a.delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
		input := invocation.Message.Content
		output := "framework:" + input
		partial := event.NewResponseEvent(invocation.InvocationID, a.name, &model.Response{
			Object:    model.ObjectTypeChatCompletionChunk,
			IsPartial: true,
			Choices:   []model.Choice{{Delta: model.Message{Role: model.RoleAssistant, Content: output}}},
		})
		select {
		case <-ctx.Done():
			return
		case results <- partial:
		}
		final := event.NewResponseEvent(invocation.InvocationID, a.name, &model.Response{
			Object:  model.ObjectTypeChatCompletion,
			Done:    true,
			Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: output}}},
		})
		final.Timestamp = time.Now().UTC()
		select {
		case <-ctx.Done():
		case results <- final:
		}
	}()
	return results, nil
}

func (a *DeterministicAgent) Tools() []tool.Tool { return nil }

func (a *DeterministicAgent) Info() frameworkagent.Info {
	return frameworkagent.Info{Name: a.name, Description: "deterministic framework integration agent"}
}

func (a *DeterministicAgent) SubAgents() []frameworkagent.Agent { return nil }

func (a *DeterministicAgent) FindSubAgent(string) frameworkagent.Agent { return nil }

type deterministicToolInput struct {
	Target string `json:"target"`
}

type deterministicToolOutput struct {
	Status string `json:"status"`
}

type deterministicToolModel struct {
	name     string
	toolName string
}

// NewDeterministicToolAgent exercises the upstream model-to-Tool execution
// path without relying on a hosted model or an external side effect.
func NewDeterministicToolAgent(name, toolName string) frameworkagent.Agent {
	return NewDeterministicToolAgentWithDelay(name, toolName, 0)
}

func NewDeterministicToolAgentWithDelay(name, toolName string, delay time.Duration) frameworkagent.Agent {
	modelStub := &deterministicToolModel{name: name + "-model", toolName: toolName}
	toolStub := function.NewFunctionTool(
		func(ctx context.Context, _ deterministicToolInput) (deterministicToolOutput, error) {
			if delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return deterministicToolOutput{}, ctx.Err()
				case <-timer.C:
				}
			}
			return deterministicToolOutput{Status: "completed"}, nil
		},
		function.WithName(toolName),
		function.WithDescription("Deterministic local Tool used by the framework integration runtime"),
	)
	return llmagent.New(name, llmagent.WithModel(modelStub), llmagent.WithTools([]tool.Tool{toolStub}))
}

func (m *deterministicToolModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	message := model.Message{
		Role: model.RoleAssistant,
		ToolCalls: []model.ToolCall{{
			ID:   "deterministic-tool-call",
			Type: "function",
			Function: model.FunctionDefinitionParam{
				Name:      m.toolName,
				Arguments: []byte(`{"target":"stage5"}`),
			},
		}},
	}
	for _, candidate := range request.Messages {
		if candidate.Role == model.RoleTool && candidate.ToolName == m.toolName && !strings.Contains(candidate.Content, "tool callback error:") {
			message = model.NewAssistantMessage("framework:tool completed")
			break
		}
	}
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{
		Object:  model.ObjectTypeChatCompletion,
		Done:    true,
		Choices: []model.Choice{{Message: message}},
	}
	close(responses)
	return responses, nil
}

func (m *deterministicToolModel) Info() model.Info { return model.Info{Name: m.name} }
