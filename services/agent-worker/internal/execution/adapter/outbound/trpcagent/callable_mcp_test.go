package trpcagent

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const mcpSearchProvider = "fn_0743b5beea9f08b8654a3b2ca47ac15dc41ca5a27f64de8e5350b326c465"
const mcpAProvider = "fn_da443d268e0685ca1f76ad4d2ab4b7d2f071a373f075d92fbfe9133512d8"

func TestCallableMCPMultipleBindingsHistoryAndFragments(t *testing.T) {
	i0, i1, i2 := 0, 1, 2
	calls := []model.ToolCall{
		{Index: &i0, Function: model.FunctionDefinitionParam{Name: mcpSearchProvider[:3]}},
		{Index: &i1, Function: model.FunctionDefinitionParam{Name: mcpAProvider[:3]}},
		{Index: &i2, Function: model.FunctionDefinitionParam{Name: docsCallable[:3]}},
	}
	tail := []model.ToolCall{
		{Index: &i0, Function: model.FunctionDefinitionParam{Name: mcpSearchProvider[3:]}},
		{Index: &i1, Function: model.FunctionDefinitionParam{Name: mcpAProvider[3:]}},
		{Index: &i2, Function: model.FunctionDefinitionParam{Name: docsCallable[3:]}},
	}
	stub := &callableModelStub{responses: []*model.Response{
		{Choices: []model.Choice{{Delta: model.Message{ToolCalls: calls}}}},
		{Choices: []model.Choice{{Delta: model.Message{ToolCalls: tail}}}},
		{Done: true, Choices: []model.Choice{{Message: model.Message{ToolCalls: []model.ToolCall{
			{Function: model.FunctionDefinitionParam{Name: mcpSearchProvider}},
			{Function: model.FunctionDefinitionParam{Name: mcpAProvider}},
			{Function: model.FunctionDefinitionParam{Name: docsCallable}},
		}}}}},
	}}
	bindings := map[string]string{"mcp_search": "tools/search", "mcp_a": "tools/a", sdkKnowledgeName: "knowledge/docs"}
	wrapped, err := WrapCallableModel(stub, bindings)
	if err != nil {
		t.Fatal(err)
	}
	req := &model.Request{Tools: map[string]tool.Tool{}, Messages: []model.Message{{ToolName: "mcp_search", ToolCalls: []model.ToolCall{{Function: model.FunctionDefinitionParam{Name: "mcp_a"}}}}}}
	for alias := range bindings {
		req.Tools[alias] = callableNamedTool(alias)
	}
	req.Tools["memory_add"] = callableNamedTool("memory_add")
	req.Tools["artifact_load"] = callableNamedTool("artifact_load")
	ch, err := wrapped.GenerateContent(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	frames := 0
	want := []string{"mcp_search", "mcp_a", sdkKnowledgeName}
	for response := range ch {
		if response.Error != nil {
			t.Fatal(response.Error)
		}
		if frames == 1 {
			for i, c := range response.Choices[0].Delta.ToolCalls {
				if c.Function.Name != want[i] {
					t.Fatal("stream mapping", c.Function.Name)
				}
			}
		}
		if response.Done {
			for i, c := range response.Choices[0].Message.ToolCalls {
				if c.Function.Name != want[i] {
					t.Fatal("final mapping", c.Function.Name)
				}
			}
		}
		frames++
	}
	if frames != 3 {
		t.Fatal(frames)
	}
	for _, provider := range []string{mcpSearchProvider, mcpAProvider, docsCallable, "memory_add", "artifact_load"} {
		if stub.request.Tools[provider] == nil || stub.request.Tools[provider].Declaration().Name != provider {
			t.Fatal("declaration mapping", provider)
		}
	}
	if stub.request.Messages[0].ToolName != mcpSearchProvider || stub.request.Messages[0].ToolCalls[0].Function.Name != mcpAProvider {
		t.Fatal("history mapping")
	}
	if req.Messages[0].ToolName != "mcp_search" || req.Messages[0].ToolCalls[0].Function.Name != "mcp_a" {
		t.Fatal("caller mutation")
	}
}
func TestCallableMCPRejectsBindingAndSelectionErrors(t *testing.T) {
	for _, binding := range []map[string]string{
		{"a": "tools/search", "b": "tools/search"},
		{"a": "tools/Bad"},
		{"": "tools/search"},
	} {
		if _, err := WrapCallableModel(&callableModelStub{}, binding); !errors.Is(err, ErrCallable) {
			t.Fatal(binding, err)
		}
	}
	for _, name := range []string{"remote_search", "mcp_search", mcpAProvider, "knowledge_search"} {
		stub := &callableModelStub{responses: []*model.Response{{Done: true, Choices: []model.Choice{{Message: model.Message{ToolCalls: []model.ToolCall{{Function: model.FunctionDefinitionParam{Name: name}}}}}}}}}
		wrapper, _ := WrapCallableModel(stub, map[string]string{"mcp_search": "tools/search"})
		ch, err := wrapper.GenerateContent(context.Background(), &model.Request{Tools: map[string]tool.Tool{"mcp_search": callableNamedTool("mcp_search")}})
		if err != nil {
			t.Fatal(err)
		}
		rejected := false
		for r := range ch {
			rejected = rejected || r.Error != nil
		}
		if !rejected {
			t.Fatal("unselected provider tool accepted", name)
		}
	}
}
