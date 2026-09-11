package testutil

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// ExtractingModel replies to ordinary turns and emits memory_add tool calls
// when the request is a framework memory extraction pass.
type ExtractingModel struct {
	Name   string
	Reply  string
	Memory string
	Kind   memory.Kind
	mu     sync.Mutex
	tools  [][]string
	texts  []string
	calls  int
}

func NewExtractingModel(name, reply, memoryText string) *ExtractingModel {
	return &ExtractingModel{Name: name, Reply: reply, Memory: memoryText, Kind: memory.KindFact}
}

func (m *ExtractingModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	toolNames := requestToolNames(request)
	m.mu.Lock()
	m.calls++
	m.tools = append(m.tools, toolNames)
	m.texts = append(m.texts, requestText(request))
	m.mu.Unlock()

	if requestHasTool(request, memory.AddToolName) {
		return singleModelResponse(extractionToolCall(m.Memory, m.Kind)), nil
	}
	return singleModelResponse(model.NewAssistantMessage(m.Reply)), nil
}

func (m *ExtractingModel) Info() model.Info {
	name := m.Name
	if name == "" {
		name = "extracting-model"
	}
	return model.Info{Name: name, ContextWindow: 4096}
}

func (m *ExtractingModel) ToolNames() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]string, len(m.tools))
	for i, names := range m.tools {
		out[i] = append([]string(nil), names...)
	}
	return out
}

func (m *ExtractingModel) Texts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.texts...)
}

func requestHasTool(request *model.Request, name string) bool {
	return request != nil && request.Tools[name] != nil
}

func requestToolNames(request *model.Request) []string {
	if request == nil || len(request.Tools) == 0 {
		return nil
	}
	names := make([]string, 0, len(request.Tools))
	for name := range request.Tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func requestText(request *model.Request) string {
	if request == nil {
		return ""
	}
	var text []byte
	for _, message := range request.Messages {
		text = append(text, message.Content...)
		text = append(text, '\n')
		for _, part := range message.ContentParts {
			if part.Text != nil {
				text = append(text, *part.Text...)
			}
			text = append(text, '\n')
		}
	}
	return string(text)
}

func extractionToolCall(memoryText string, kind memory.Kind) model.Message {
	if kind == "" {
		kind = memory.KindFact
	}
	args, _ := json.Marshal(map[string]any{
		"memory":      memoryText,
		"topics":      []string{"extracted"},
		"memory_kind": string(kind),
	})
	return model.Message{
		Role: model.RoleAssistant,
		ToolCalls: []model.ToolCall{{
			Type: "function",
			Function: model.FunctionDefinitionParam{
				Name:      memory.AddToolName,
				Arguments: args,
			},
		}},
	}
}

func singleModelResponse(message model.Message) <-chan *model.Response {
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{
		ID:     "extracting-response",
		Object: "chat.completion",
		Choices: []model.Choice{{
			Index:        0,
			Message:      message,
			FinishReason: model.StringPtr("stop"),
		}},
		Done: true,
	}
	close(responses)
	return responses
}
