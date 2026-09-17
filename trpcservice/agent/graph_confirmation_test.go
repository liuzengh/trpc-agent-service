package agent

import (
	"context"
	"encoding/json"
	"testing"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type graphConfirmationEventAgent struct{ events []*event.Event }

func (a graphConfirmationEventAgent) Run(context.Context, *agentcore.Invocation) (<-chan *event.Event, error) {
	values := make(chan *event.Event, len(a.events))
	for _, value := range a.events {
		values <- value
	}
	close(values)
	return values, nil
}
func (graphConfirmationEventAgent) Tools() []agenttool.Tool { return nil }
func (graphConfirmationEventAgent) Info() agentcore.Info    { return agentcore.Info{Name: "child"} }
func (graphConfirmationEventAgent) SubAgents() []agentcore.Agent {
	return nil
}
func (graphConfirmationEventAgent) FindSubAgent(string) agentcore.Agent { return nil }

func TestGraphConfirmationResumeMessageRequiresExactTargetedPayload(t *testing.T) {
	ask := map[string]struct{}{"danger": {}}
	resume := map[string]any{"schema_version": 1, "kind": "tool_result", "tool_call_id": "call-1",
		"tool_name": "danger", "result": `{"done":true}`}
	state := map[string]any{graph.StateKeyCommand: graph.NewResumeCommand().AddResumeValue("call-1", resume)}
	message, present, valid := graphConfirmationResumeMessage(state, ask)
	if !present || !valid || message.Role != model.RoleTool || message.ToolID != "call-1" {
		t.Fatalf("message=%#v present=%v valid=%v", message, present, valid)
	}

	resume["unexpected"] = true
	if _, present, valid := graphConfirmationResumeMessage(state, ask); !present || valid {
		t.Fatalf("unknown resume field accepted: present=%v valid=%v", present, valid)
	}
	delete(resume, "unexpected")
	resume["tool_name"] = "other"
	if _, present, valid := graphConfirmationResumeMessage(state, ask); !present || valid {
		t.Fatalf("wrong resume tool accepted: present=%v valid=%v", present, valid)
	}
}

func TestGraphConfirmationResumeMessageDistinguishesInitialRunFromMalformedResume(t *testing.T) {
	ask := map[string]struct{}{"danger": {}}
	if _, present, valid := graphConfirmationResumeMessage(nil, ask); present || valid {
		t.Fatalf("initial run treated as resume: present=%v valid=%v", present, valid)
	}
	if _, present, valid := graphConfirmationResumeMessage(map[string]any{graph.StateKeyCommand: "invalid"}, ask); !present || valid {
		t.Fatalf("malformed command did not fail closed: present=%v valid=%v", present, valid)
	}
}

func TestGraphConfirmationAgentMergesStreamingFragmentsOfOneToolCall(t *testing.T) {
	toolCall := func(arguments string) *event.Event {
		return event.NewResponseEvent("invocation", "child", &model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "call-1", Type: "function",
				Function: model.FunctionDefinitionParam{Name: "danger", Arguments: []byte(arguments)}}}},
		}}})
	}
	wrapped, err := newGraphConfirmationAgent(graphConfirmationEventAgent{events: []*event.Event{
		toolCall(`{"value":`),
		event.NewResponseEvent("invocation", "child", &model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{Type: "function",
				Function: model.FunctionDefinitionParam{Arguments: []byte(`1}`)}}}},
		}}}),
		toolCall(`{"value":1}`),
	}}, map[string]struct{}{"danger": {}})
	if err != nil {
		t.Fatal(err)
	}
	events, err := wrapped.Run(context.Background(), agentcore.NewInvocation(
		agentcore.WithInvocationID("invocation"), agentcore.WithInvocationAgent(wrapped),
	))
	if err != nil {
		t.Fatal(err)
	}
	var interrupt *event.Event
	for value := range events {
		if value.IsTerminalError() {
			t.Fatalf("same streaming tool call was rejected: %#v", value)
		}
		if value.Object == graph.ObjectTypeGraphPregelStep {
			interrupt = value
		}
	}
	if interrupt == nil {
		t.Fatal("confirmation interrupt missing")
	}
	var metadata graph.PregelStepMetadata
	if err := json.Unmarshal(interrupt.StateDelta[graph.MetadataKeyPregel], &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.InterruptKey != "call-1" {
		t.Fatalf("interrupt=%#v", metadata)
	}
}
