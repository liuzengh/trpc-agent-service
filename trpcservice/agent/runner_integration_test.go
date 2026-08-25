package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func localOpenAIProviderFactory(endpoint string) ProviderFactory {
	return OpenAIProviderFactory{
		Configs: ModelConfigResolverFunc(func(_ context.Context, tc tenant.TenantContext, spec AgentSpec) (ModelConfig, error) {
			return openAIConfig(tc, spec, endpoint), nil
		}),
		Secrets: SecretResolverFunc(func(context.Context, tenant.TenantContext, string) (string, error) {
			return "local-secret", nil
		}),
	}
}

func TestRuntimeActualOpenAIRunnerSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"local","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"runner answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}`)
	}))
	defer server.Close()

	tc := validContext()
	spec := validSpec()
	spec.ModelProvider = "openai"
	spec.ModelConfigRef = "ref-runner"
	factory, err := NewFactory(RuntimeDependencies{ProviderFactory: localOpenAIProviderFactory(server.URL), DrainTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runtimeValue, err := factory.Build(context.Background(), tc, spec)
	if err != nil {
		t.Fatal(err)
	}
	input := AgentInput{TenantContext: tc, Agent: spec, Input: Message{ID: tc.MessageID, Role: "user", Content: "hello"}}
	result, err := runtimeValue.Run(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "runner answer" || len(result.Events) == 0 {
		t.Fatalf("unexpected runner result: %+v", result)
	}
}

func TestRuntimeFrameworkToolCallsInvoker(t *testing.T) {
	var requestsMu sync.Mutex
	var requestBodies []map[string]any
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("invalid request: %v", err)
		}
		requestsMu.Lock()
		requestBodies = append(requestBodies, payload)
		callCount++
		current := callCount
		requestsMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if current == 1 {
			_, _ = io.WriteString(w, `{"id":"local","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"key\":\"value\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"local-2","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"tool answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`)
	}))
	defer server.Close()

	tc := validContext()
	spec := validSpec()
	spec.ModelProvider = "openai"
	spec.ModelConfigRef = "ref-tool"
	spec.Tools = []ToolSpec{{Name: "lookup", Description: "look up a value", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"key": map[string]any{"type": "string"}}, "required": []any{"key"}}}}
	var invoked ToolRequest
	var invokedMu sync.Mutex
	factory, err := NewFactory(RuntimeDependencies{
		ProviderFactory: localOpenAIProviderFactory(server.URL),
		ToolInvoker: ToolInvokerFunc(func(_ context.Context, request ToolRequest) (ToolResult, error) {
			invokedMu.Lock()
			invoked = request
			invokedMu.Unlock()
			return ToolResult{Content: "value"}, nil
		}),
		DrainTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeValue, err := factory.Build(context.Background(), tc, spec)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtimeValue.Run(context.Background(), AgentInput{TenantContext: tc, Agent: spec, Input: Message{ID: tc.MessageID, Role: "user", Content: "find it"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "tool answer" {
		t.Fatalf("unexpected tool result: %+v", result)
	}
	invokedMu.Lock()
	got := invoked
	invokedMu.Unlock()
	if got.ToolName != "lookup" || got.TenantContext.TenantID != tc.TenantID || got.Agent.ModelConfigRef != spec.ModelConfigRef || got.Arguments["key"] != "value" {
		t.Fatalf("unexpected tool request: %+v", got)
	}
	var started, completed bool
	var previousSequence int64
	for _, eventValue := range result.Events {
		if eventValue.Sequence <= previousSequence {
			t.Fatalf("event sequence is not strictly increasing: %+v", result.Events)
		}
		previousSequence = eventValue.Sequence
		if eventValue.Type == "tool.started" {
			if eventValue.ToolName != "lookup" {
				t.Fatalf("unexpected started tool event: %+v", eventValue)
			}
			started = true
		}
		if eventValue.Type == "tool.completed" {
			if !started {
				t.Fatal("tool completed before tool started")
			}
			if eventValue.ToolName != "lookup" || eventValue.Content != "value" {
				t.Fatalf("unexpected completed tool event: %+v", eventValue)
			}
			completed = true
		}
	}
	if !started || !completed {
		t.Fatalf("tool lifecycle events missing: %+v", result.Events)
	}
	requestsMu.Lock()
	defer requestsMu.Unlock()
	if len(requestBodies) != 2 {
		t.Fatalf("expected two model requests, got %d", len(requestBodies))
	}
	messages, ok := requestBodies[1]["messages"].([]any)
	if !ok {
		t.Fatalf("second request messages have unexpected type: %T", requestBodies[1]["messages"])
	}
	var toolCallID string
	var toolResultFound bool
	for _, rawMessage := range messages {
		message, ok := rawMessage.(map[string]any)
		if !ok {
			continue
		}
		if message["role"] == "assistant" {
			calls, _ := message["tool_calls"].([]any)
			for _, rawCall := range calls {
				call, _ := rawCall.(map[string]any)
				if id, ok := call["id"].(string); ok {
					toolCallID = id
				}
			}
		}
		if message["role"] == "tool" {
			content, _ := message["content"].(string)
			var decodedContent string
			if err := json.Unmarshal([]byte(content), &decodedContent); err == nil && decodedContent == "value" {
				toolResultFound = true
			}
			if message["tool_call_id"] != "call-1" {
				t.Fatalf("tool result used wrong call id: %+v", message)
			}
		}
	}
	if toolCallID != "call-1" || !toolResultFound {
		t.Fatalf("second request did not contain the expected tool result: %+v", messages)
	}
}
