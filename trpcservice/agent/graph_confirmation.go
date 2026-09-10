package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const graphConfirmationSchemaVersion = 1

type graphConfirmationAgent struct {
	inner    agentcore.Agent
	askTools map[string]struct{}
}

type graphConfirmationInterrupt struct {
	SchemaVersion int    `json:"schema_version"`
	Kind          string `json:"kind"`
	ToolCallID    string `json:"tool_call_id"`
	ToolName      string `json:"tool_name"`
}

type graphConfirmationResume struct {
	SchemaVersion int    `json:"schema_version"`
	Kind          string `json:"kind"`
	ToolCallID    string `json:"tool_call_id"`
	ToolName      string `json:"tool_name"`
	Result        string `json:"result"`
}

func newGraphConfirmationAgent(inner agentcore.Agent, askTools map[string]struct{}) (agentcore.Agent, error) {
	if inner == nil || len(askTools) == 0 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	values := make(map[string]struct{}, len(askTools))
	for name := range askTools {
		if name == "" {
			return nil, runtime.ErrInvariantViolation
		}
		values[name] = struct{}{}
	}
	return &graphConfirmationAgent{inner: inner, askTools: values}, nil
}

func (a *graphConfirmationAgent) Run(ctx context.Context, invocation *agentcore.Invocation) (<-chan *event.Event, error) {
	if a == nil || a.inner == nil || invocation == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	value := invocation.View()
	resumed, present, valid := graphConfirmationResumeMessage(value.RunOptions.RuntimeState, a.askTools)
	if present && !valid {
		return nil, runtime.ErrInvalidEnvelope
	}
	if valid {
		value.Message = resumed
	}
	values, err := a.inner.Run(ctx, value)
	if err != nil {
		return nil, err
	}
	out := make(chan *event.Event, 8)
	go func() {
		defer close(out)
		var pending *model.ToolCall
		for item := range values {
			if item != nil && item.Response != nil {
				for _, choice := range item.Choices {
					for index := range choice.Message.ToolCalls {
						call := choice.Message.ToolCalls[index]
						// In an OpenAI-compatible streaming response, only the
						// first delta for a tool call is required to carry its ID
						// and name.  Later argument deltas may omit either field.
						// They cannot describe an independently confirmable call,
						// so wait for a stable ID rather than rejecting them.
						if call.ID == "" {
							continue
						}
						if _, ask := a.askTools[call.Function.Name]; !ask {
							continue
						}
						if pending != nil {
							// Providers may emit one logical tool call in several
							// streaming events.  It is safe to retain the final
							// fragment only when it names the same call and tool;
							// distinct calls remain unsupported because one durable
							// confirmation binds exactly one call and argument digest.
							if pending.ID == call.ID && (call.Function.Name == "" || pending.Function.Name == call.Function.Name) {
								pending.Function.Arguments = call.Function.Arguments
								continue
							}
							out <- event.NewErrorEvent(value.InvocationID, value.AgentName, model.ErrorTypeFlowError,
								"graph confirmation received more than one stable tool call: "+runtime.ErrCapabilityUnsupported.Error())
							return
						}
						copyCall := call
						pending = &copyCall
					}
				}
			}
			out <- item
		}
		if pending == nil {
			return
		}
		coordinate := graphConfirmationInterrupt{SchemaVersion: graphConfirmationSchemaVersion, Kind: "tool_confirmation",
			ToolCallID: pending.ID, ToolName: pending.Function.Name}
		sum := sha256.Sum256([]byte(value.InvocationID + "\x00" + pending.ID + "\x00" + pending.Function.Name))
		out <- graph.NewPregelInterruptEvent(
			graph.WithPregelEventInvocationID(value.InvocationID),
			graph.WithPregelEventNodeID(value.AgentName),
			graph.WithPregelEventInterruptKey(pending.ID),
			graph.WithPregelEventInterruptValue(coordinate),
			graph.WithPregelEventLineageID(value.InvocationID),
			graph.WithPregelEventCheckpointID("continuation_"+hex.EncodeToString(sum[:16])),
			graph.WithPregelEventCheckpointNS(value.AgentName),
		)
	}()
	return out, nil
}

func graphConfirmationResumeMessage(state map[string]any, askTools map[string]struct{}) (model.Message, bool, bool) {
	if len(state) == 0 {
		return model.Message{}, false, false
	}
	var resumeMap map[string]any
	raw, present := state[graph.StateKeyCommand]
	if !present {
		return model.Message{}, false, false
	}
	switch command := raw.(type) {
	case *graph.Command:
		resumeMap = command.ResumeMap
	case *graph.ResumeCommand:
		resumeMap = command.ResumeMap
	default:
		return model.Message{}, true, false
	}
	if len(resumeMap) != 1 {
		return model.Message{}, true, false
	}
	for taskID, raw := range resumeMap {
		encoded, err := json.Marshal(raw)
		if err != nil {
			return model.Message{}, true, false
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 5 {
			return model.Message{}, true, false
		}
		var value graphConfirmationResume
		if err := json.Unmarshal(encoded, &value); err != nil || value.SchemaVersion != graphConfirmationSchemaVersion ||
			value.Kind != "tool_result" || value.ToolCallID == "" || value.ToolCallID != taskID || value.ToolName == "" || value.Result == "" {
			return model.Message{}, true, false
		}
		if _, ok := askTools[value.ToolName]; !ok {
			return model.Message{}, true, false
		}
		return model.NewToolMessage(value.ToolCallID, value.ToolName, value.Result), true, true
	}
	return model.Message{}, true, false
}

func (a *graphConfirmationAgent) Tools() []agenttool.Tool      { return a.inner.Tools() }
func (a *graphConfirmationAgent) Info() agentcore.Info         { return a.inner.Info() }
func (a *graphConfirmationAgent) SubAgents() []agentcore.Agent { return a.inner.SubAgents() }
func (a *graphConfirmationAgent) FindSubAgent(name string) agentcore.Agent {
	return a.inner.FindSubAgent(name)
}

var _ agentcore.Agent = (*graphConfirmationAgent)(nil)
