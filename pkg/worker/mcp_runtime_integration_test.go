package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/platformtool"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type integrationMCPSecretResolver map[tenant.SecretRef][]byte

func (r integrationMCPSecretResolver) Resolve(_ context.Context, ref tenant.SecretRef) ([]byte, error) {
	value, ok := r[ref]
	if !ok {
		return nil, errors.New("test MCP secret missing")
	}
	return append([]byte(nil), value...), nil
}

type integrationMCPServer struct {
	token string
	calls atomic.Int32
}

func (s *integrationMCPServer) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		http.Error(w, "notifications not enabled", http.StatusMethodNotAllowed)
		return
	}
	if request.Method != http.MethodPost || request.Header.Get("Authorization") != s.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var envelope struct {
		ID     any             `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20)).Decode(&envelope); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if envelope.ID == nil {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	var result any
	switch envelope.Method {
	case "initialize":
		result = map[string]any{
			"protocolVersion": "2025-03-26",
			"serverInfo":      map[string]string{"name": "orders-integration", "version": "1.0.0"},
			"capabilities":    map[string]any{"tools": map[string]any{}},
		}
	case "tools/list":
		result = map[string]any{"tools": []map[string]any{{
			"name": "lookup_order", "description": "Look up an order",
			"inputSchema": map[string]any{
				"type": "object", "properties": map[string]any{"order_id": map[string]string{"type": "string"}},
				"required": []string{"order_id"},
			},
			"annotations": map[string]any{"readOnlyHint": true, "destructiveHint": false, "openWorldHint": true},
		}}}
	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(envelope.Params, &params); err != nil || params.Name != "lookup_order" || params.Arguments["order_id"] != "42" {
			http.Error(w, "invalid MCP call", http.StatusBadRequest)
			return
		}
		s.calls.Add(1)
		result = map[string]any{"content": []map[string]string{{"type": "text", "text": "order 42 is ready"}}}
	default:
		http.Error(w, "unsupported MCP method", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": envelope.ID, "result": result})
}

type mcpRunnerModel struct {
	mu       sync.Mutex
	requests [][]model.Message
	calls    atomic.Int32
}

func (m *mcpRunnerModel) GenerateContent(_ context.Context, request *model.Request) (<-chan *model.Response, error) {
	m.mu.Lock()
	m.requests = append(m.requests, append([]model.Message(nil), request.Messages...))
	m.mu.Unlock()
	call := m.calls.Add(1)
	response := &model.Response{
		ID: "mcp-model-response", Object: model.ObjectTypeChatCompletion, Done: true,
		Choices: []model.Choice{{Index: 0, Message: model.Message{Role: model.RoleAssistant}}},
	}
	if call == 1 {
		response.Choices[0].Message.ToolCalls = []model.ToolCall{{
			ID: "mcp-call-1", Type: "function",
			Function: model.FunctionDefinitionParam{Name: "mcp_orders_lookup_order", Arguments: []byte(`{"order_id":"42"}`)},
		}}
	} else {
		if !modelRequestContains(request.Messages, "order 42 is ready") {
			return nil, errors.New("MCP result was not returned to the model")
		}
		response.Choices[0].Message.Content = "订单 42 已准备好"
	}
	output := make(chan *model.Response, 1)
	output <- response
	close(output)
	return output, nil
}

func (*mcpRunnerModel) Info() model.Info { return model.Info{Name: "mcp-runner-model"} }

func modelRequestContains(messages []model.Message, value string) bool {
	encoded, _ := json.Marshal(messages)
	return bytes.Contains(encoded, []byte(value))
}

func TestMCPProfileWorkerRunnerGovernanceVerticalSlice(t *testing.T) {
	const (
		mcpCredential = "Bearer integration-only-mcp-token"
		toolName      = "mcp_orders_lookup_order"
	)
	mcpService := &integrationMCPServer{token: mcpCredential}
	mcpHTTP := httptest.NewServer(mcpService)
	t.Cleanup(mcpHTTP.Close)
	profiles, err := json.Marshal([]platformtool.MCPProfile{{
		ID: "orders", Transport: "streamable", ServerURL: mcpHTTP.URL,
		Timeout: "5s", Tools: []string{"lookup_order"}, AllowInsecure: true,
		HeaderRefs: map[string]string{"Authorization": "env://TRPC_SECRET_MCP_ORDERS_AUTH"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := platformtool.NewMCPRuntimeResolver(string(profiles), integrationMCPSecretResolver{
		"env://TRPC_SECRET_MCP_ORDERS_AUTH": []byte(mcpCredential),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resolver.Close() })
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	resolved, err := resolver.ResolveContext(context.Background(), []string{toolName})
	if err != nil {
		t.Fatalf("resolve MCP profile: %v", err)
	}
	if len(resolved) != 1 {
		t.Fatalf("resolved MCP tool count=%d", len(resolved))
	}
	if metadata := tool.MetadataOf(resolved[0]); !metadata.ReadOnly || !metadata.OpenWorld {
		t.Fatalf("MCP metadata was not preserved: %#v", metadata)
	}

	tenantValue := &tenant.Tenant{
		ID:         "mcp-integration-tenant",
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{toolName}},
	}
	modelValue := &mcpRunnerModel{}
	agentValue := llmagent.New(
		"support", llmagent.WithModel(modelValue), llmagent.WithTools(resolved),
		llmagent.WithMaxLLMCalls(3), llmagent.WithMaxToolIterations(2),
	)
	sessionService := sessioninmem.NewSessionService()
	var audit bytes.Buffer
	collector := telemetry.NewCollectorWithAuditSinkAndIdentityKey(&audit, []byte("integration-audit-key"))
	runnerValue := runner.NewRunner(
		"mcp-integration-app", agentValue,
		runner.WithSessionService(sessionService),
		runner.WithPlugins(governance.NewPlugin(
			governance.NewGovernanceFilter(tenantValue), "mcp-integration-governance", collector,
		)),
	)
	value := &Worker{
		tenant: tenantValue, runner: runnerValue, sessionService: sessionService,
		sessionLocks: storage.NewSessionLockManager(redisClient),
		collector:    collector, appName: "mcp-integration-app", agentName: "support",
	}
	t.Cleanup(func() { _ = value.Close() })
	response, err := value.Process(context.Background(), &Request{
		TenantID: tenantValue.ID, ChannelType: "wework", UserID: "alice",
		SessionID: "wecom-alice", IdempotencyKey: "mcp-inbox-1", Content: "查询 42 号订单",
	})
	if err != nil {
		t.Fatalf("run MCP Worker path: %v", err)
	}
	if response == nil || response.Content != "订单 42 已准备好" {
		t.Fatalf("Worker response = %#v", response)
	}
	if modelValue.calls.Load() != 2 || mcpService.calls.Load() != 1 {
		t.Fatalf("model calls=%d MCP calls=%d", modelValue.calls.Load(), mcpService.calls.Load())
	}
	auditText := audit.String()
	for _, required := range []string{`"tool_name":"` + toolName + `"`, `"decision":"tool_allowed"`, `"decision":"tool_succeeded"`} {
		if !strings.Contains(auditText, required) {
			t.Fatalf("audit is missing %s: %s", required, auditText)
		}
	}
	for _, forbidden := range []string{mcpCredential, "integration-only-mcp-token", "查询 42 号订单"} {
		if strings.Contains(auditText, forbidden) {
			t.Fatalf("audit leaked %q: %s", forbidden, auditText)
		}
	}
}
