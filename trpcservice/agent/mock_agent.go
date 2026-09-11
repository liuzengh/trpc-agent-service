package agent

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// mockAgent makes the complete gateway/runner/session path runnable without an
// external model key. It is deterministic and intentionally does not execute
// tools; switch a tenant to provider=openai for the full LLM tool loop.
type mockAgent struct {
	name        string
	description string
	tools       []tool.Tool
}

func (a *mockAgent) Run(ctx context.Context, invocation *agent.Invocation) (<-chan *event.Event, error) {
	out := make(chan *event.Event, 1)
	go func() {
		defer close(out)
		turn := 1
		if invocation != nil && invocation.Session != nil {
			// Runner has already appended the current user event.
			turn = (invocation.Session.GetEventCount() + 1) / 2
		}
		content := ""
		if invocation != nil {
			content = invocation.Message.Content
		}
		response := &model.Response{
			Done: true,
			Choices: []model.Choice{{Index: 0, Message: model.Message{
				Role: model.RoleAssistant, Content: fmt.Sprintf("[mock turn %d] %s", turn, content),
			}}},
		}
		evt := event.NewResponseEvent(invocation.InvocationID, a.name, response)
		_ = agent.EmitEvent(ctx, invocation, out, evt)
	}()
	return out, nil
}

func (a *mockAgent) Tools() []tool.Tool { return a.tools }

func (a *mockAgent) Info() agent.Info {
	return agent.Info{Name: a.name, Description: a.description}
}

func (*mockAgent) SubAgents() []agent.Agent { return nil }

func (*mockAgent) FindSubAgent(string) agent.Agent { return nil }
