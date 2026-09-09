package platformtool

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type mcpTestSecretResolver map[tenant.SecretRef][]byte

func (r mcpTestSecretResolver) Resolve(_ context.Context, ref tenant.SecretRef) ([]byte, error) {
	value, ok := r[ref]
	if !ok {
		return nil, errors.New("test credential missing")
	}
	return append([]byte(nil), value...), nil
}

type mcpProtocolRecorder struct {
	mu               sync.Mutex
	methods          []string
	calls            int
	token            string
	callErrorMessage string
}

func (r *mcpProtocolRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		http.Error(w, "SSE notifications disabled by test server", http.StatusMethodNotAllowed)
		return
	}
	if req.Method != http.MethodPost || req.Header.Get("Authorization") != r.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var envelope struct {
		ID     any             `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 1<<20)).Decode(&envelope); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.methods = append(r.methods, envelope.Method)
	r.mu.Unlock()
	if envelope.ID == nil {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	var result any
	switch envelope.Method {
	case "initialize":
		result = map[string]any{
			"protocolVersion": "2025-03-26",
			"serverInfo":      map[string]string{"name": "orders-test", "version": "1.0.0"},
			"capabilities":    map[string]any{"tools": map[string]any{}},
		}
	case "tools/list":
		readOnly, destructive, openWorld := true, false, true
		result = map[string]any{"tools": []map[string]any{{
			"name": "lookup_order", "description": "Look up an order",
			"inputSchema": map[string]any{
				"type": "object", "properties": map[string]any{"order_id": map[string]string{"type": "string"}},
				"required": []string{"order_id"},
			},
			"annotations": map[string]any{
				"readOnlyHint": readOnly, "destructiveHint": destructive, "openWorldHint": openWorld,
			},
		}}}
	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(envelope.Params, &params); err != nil || params.Name != "lookup_order" || params.Arguments["order_id"] != "42" {
			http.Error(w, "invalid call", http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.calls++
		callErrorMessage := r.callErrorMessage
		r.mu.Unlock()
		if callErrorMessage != "" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0", "id": envelope.ID,
				"error": map[string]any{"code": -32000, "message": callErrorMessage},
			})
			return
		}
		result = map[string]any{"content": []map[string]string{{"type": "text", "text": "order 42 is ready"}}}
	default:
		http.Error(w, "unknown method", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": envelope.ID, "result": result})
}

func (r *mcpProtocolRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestMCPAdmissionResolverExposesOnlyConfiguredNames(t *testing.T) {
	raw := `[{
		"id":"orders","transport":"streamable","serverUrl":"https://mcp.example.test/rpc",
		"timeout":"5s","tools":["lookup_order"],
		"headerRefs":{"Authorization":"env://TRPC_SECRET_MCP_ORDERS_AUTH"}
	}]`
	resolver, err := NewMCPAdmissionResolver(raw)
	if err != nil {
		t.Fatal(err)
	}
	values, err := resolver.Resolve([]string{"current_time", "mcp_orders_lookup_order"})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[1].Declaration().Name != "mcp_orders_lookup_order" {
		t.Fatalf("resolved tools = %#v", values)
	}
	if _, err := resolver.Resolve([]string{"mcp_orders_delete_order"}); err == nil {
		t.Fatal("unconfigured MCP tool was admitted")
	}
}

func TestMCPRuntimeResolverUsesOfficialToolSetAndPreservesMetadata(t *testing.T) {
	recorder := &mcpProtocolRecorder{token: "Bearer test-only-mcp-token"}
	server := httptest.NewServer(recorder)
	t.Cleanup(server.Close)
	profile, err := json.Marshal([]MCPProfile{{
		ID: "orders", Transport: "streamable", ServerURL: server.URL,
		Timeout: "5s", Tools: []string{"lookup_order"}, AllowInsecure: true,
		HeaderRefs: map[string]string{"Authorization": "env://TRPC_SECRET_MCP_ORDERS_AUTH"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewMCPRuntimeResolver(string(profile), mcpTestSecretResolver{
		"env://TRPC_SECRET_MCP_ORDERS_AUTH": []byte(recorder.token),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	// Loading a built-in tool must not contact an unrelated remote profile.
	if _, err := resolver.ResolveContext(context.Background(), []string{"current_time"}); err != nil {
		t.Fatal(err)
	}
	if recorder.callCount() != 0 {
		t.Fatal("MCP tool was called while resolving a built-in tool")
	}
	values, err := resolver.ResolveContext(context.Background(), []string{"mcp_orders_lookup_order"})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Declaration().Name != "mcp_orders_lookup_order" {
		t.Fatalf("runtime tools = %#v", values)
	}
	metadata := tool.MetadataOf(values[0])
	if !metadata.ReadOnly || metadata.Destructive || !metadata.OpenWorld {
		t.Fatalf("MCP metadata was not preserved/hardened: %#v", metadata)
	}
	callable, ok := values[0].(tool.CallableTool)
	if !ok {
		t.Fatal("resolved MCP tool is not callable")
	}
	result, err := callable.Call(context.Background(), []byte(`{"order_id":"42"}`))
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(result)
	if !strings.Contains(string(encoded), "order 42 is ready") || recorder.callCount() != 1 {
		t.Fatalf("MCP call result=%s calls=%d", encoded, recorder.callCount())
	}
}

func TestMCPProfileValidationFailsClosed(t *testing.T) {
	cases := []string{
		`null`,
		`[{"id":"orders","transport":"stdio","serverUrl":"https://mcp.example.test","tools":["lookup"]}]`,
		`[{"id":"orders","transport":"streamable","serverUrl":"http://mcp.example.test","tools":["lookup"]}]`,
		`[{"id":"orders","transport":"streamable","serverUrl":"https://user:secret@mcp.example.test","tools":["lookup"]}]`,
		`[{"id":"orders","transport":"streamable","serverUrl":"https://mcp.example.test","tools":["lookup","lookup"]}]`,
		`[{"id":"orders","transport":"streamable","serverUrl":"https://mcp.example.test","tools":["lookup"],"headerRefs":{"Host":"env://TRPC_SECRET_MCP_HOST"}}]`,
		`[{"id":"orders","transport":"streamable","serverUrl":"https://mcp.example.test","tools":["lookup"],"headerRefs":{"authorization":"env://TRPC_SECRET_MCP_ONE","Authorization":"env://TRPC_SECRET_MCP_TWO"}}]`,
		`[{"id":"orders","transport":"streamable","serverUrl":"https://mcp.example.test","tools":["lookup"],"unknown":true}]`,
		`[] {}`,
	}
	for _, raw := range cases {
		if _, err := NewMCPAdmissionResolver(raw); !errors.Is(err, ErrMCPProfileConfig) {
			t.Errorf("config %s error=%v, want ErrMCPProfileConfig", raw, err)
		}
	}
}

func TestMCPResolverFailsClosedAfterClose(t *testing.T) {
	recorder := &mcpProtocolRecorder{token: "Bearer close-test"}
	server := httptest.NewServer(recorder)
	t.Cleanup(server.Close)
	profile, _ := json.Marshal([]MCPProfile{{
		ID: "orders", Transport: "streamable", ServerURL: server.URL,
		Tools: []string{"lookup_order"}, AllowInsecure: true,
		HeaderRefs: map[string]string{"Authorization": "env://TRPC_SECRET_MCP_CLOSE"},
	}})
	resolver, err := NewMCPRuntimeResolver(string(profile), mcpTestSecretResolver{
		"env://TRPC_SECRET_MCP_CLOSE": []byte(recorder.token),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveContext(context.Background(), []string{"mcp_orders_lookup_order"}); err != nil {
		t.Fatal(err)
	}
	if err := resolver.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveContext(context.Background(), []string{"mcp_orders_lookup_order"}); !errors.Is(err, ErrMCPProfileUnavailable) {
		t.Fatalf("resolve after close error=%v, want ErrMCPProfileUnavailable", err)
	}
}

func TestMCPToolCallRedactsRemoteError(t *testing.T) {
	recorder := &mcpProtocolRecorder{token: "Bearer error-test", callErrorMessage: "provider-secret=never-return"}
	server := httptest.NewServer(recorder)
	t.Cleanup(server.Close)
	profile, _ := json.Marshal([]MCPProfile{{
		ID: "orders", Transport: "streamable", ServerURL: server.URL,
		Tools: []string{"lookup_order"}, AllowInsecure: true,
		HeaderRefs: map[string]string{"Authorization": "env://TRPC_SECRET_MCP_ERROR"},
	}})
	resolver, err := NewMCPRuntimeResolver(string(profile), mcpTestSecretResolver{
		"env://TRPC_SECRET_MCP_ERROR": []byte(recorder.token),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resolver.Close() })
	values, err := resolver.ResolveContext(context.Background(), []string{"mcp_orders_lookup_order"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = values[0].(tool.CallableTool).Call(context.Background(), []byte(`{"order_id":"42"}`))
	if !errors.Is(err, ErrMCPToolCall) || strings.Contains(err.Error(), recorder.callErrorMessage) {
		t.Fatalf("tool error=%v, want stable redacted ErrMCPToolCall", err)
	}
}

func TestMCPRuntimeResolverRedactsCredentialAndTransportErrors(t *testing.T) {
	profile := `[{"id":"orders","transport":"streamable","serverUrl":"https://private-host.invalid/rpc","timeout":"100ms","tools":["lookup"],"headerRefs":{"Authorization":"env://TRPC_SECRET_MCP_PRIVATE"}}]`
	resolver, err := NewMCPRuntimeResolver(profile, mcpTestSecretResolver{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolver.ResolveContext(context.Background(), []string{"mcp_orders_lookup"})
	if !errors.Is(err, ErrMCPProfileUnavailable) {
		t.Fatalf("error=%v, want ErrMCPProfileUnavailable", err)
	}
	for _, forbidden := range []string{"private-host", "TRPC_SECRET_MCP_PRIVATE", "test credential missing"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("runtime error leaked %q: %v", forbidden, err)
		}
	}
}
