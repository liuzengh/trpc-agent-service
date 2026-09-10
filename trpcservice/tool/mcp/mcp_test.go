package mcp

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	tmcp "trpc.group/trpc-go/trpc-mcp-go"
)

func TestRegistrationUsesUpstreamMCPToolSetWithScopedSecret(t *testing.T) {
	handler := &recordingHandler{}
	expected, err := DeclarationDigest(&agenttool.Declaration{Name: "weather_lookup", Description: "tenant weather", InputSchema: &agenttool.Schema{Type: "object"}})
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Transport: "streamable", ServerURL: "https://mcp.test/tools", RemoteToolName: "weather_lookup", Timeout: time.Second,
		ExpectedDeclarationDigest: expected,
		SecretHeader:              "Authorization", SecretPrefix: "Bearer ", clientOptions: []tmcp.ClientOption{tmcp.WithHTTPReqHandler(handler)},
	}
	secretRef := secrets.SecretRef{Ref: "secret://mcp-weather", Version: 4}
	binding, err := BindingDigest(config, secretRef)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := NewRegistration("tenant-a", "weather_lookup", 2, config, secretRef)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := servicetool.NewCatalog(registration)
	if err != nil {
		t.Fatal(err)
	}
	provider := &recordingSecrets{value: []byte("tenant-token")}
	resolver := servicetool.Resolver{Catalog: catalog, Secrets: provider}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "req-1", SubjectID: "worker-model"})
	values, err := resolver.ResolveTools(ctx, "tenant-a", []profile.VersionedRef{{ID: "weather_lookup", Version: 2, ContentDigest: binding}})
	if err != nil || len(values) != 1 || values[0].Declaration().Name != "weather_lookup" {
		t.Fatalf("values=%#v err=%v", values, err)
	}
	callable, ok := values[0].(agenttool.CallableTool)
	if !ok {
		t.Fatalf("resolved tool is not callable: %T", values[0])
	}
	result, err := callable.Call(ctx, []byte(`{"city":"Shanghai"}`))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil || !strings.Contains(string(encoded), "weather:Shanghai") {
		t.Fatalf("result=%s err=%v", encoded, err)
	}
	if provider.calls == 0 || provider.scope.TenantID != "tenant-a" || provider.scope.Subject != "worker-model" ||
		provider.scope.Purpose != secrets.PurposeToolCall || provider.scope.ResourceID != "weather_lookup" || provider.scope.ResourceVersion != 2 {
		t.Fatalf("secret calls=%d scope=%#v", provider.calls, provider.scope)
	}
	for _, header := range handler.headers() {
		if header.Get("Authorization") != "Bearer tenant-token" {
			t.Fatalf("headers=%v", header)
		}
	}
	if !handler.seen("initialize") || !handler.seen("tools/list") || !handler.seen("tools/call") {
		t.Fatalf("methods=%v", handler.methods())
	}
	if _, err := resolver.ResolveTools(ctx, "tenant-a", []profile.VersionedRef{{ID: "weather_lookup", Version: 2, ContentDigest: strings.Repeat("0", 64)}}); err != runtime.ErrVersionMismatch {
		t.Fatalf("binding digest mismatch err=%v", err)
	}
}

func TestRegistrationRejectsUnboundedOrMutableMCPConfiguration(t *testing.T) {
	valid := Config{Transport: "streamable", ServerURL: "https://mcp.example.com/v1", RemoteToolName: "lookup", ExpectedDeclarationDigest: strings.Repeat("a", 64), Timeout: time.Second}
	cases := []Config{
		{Transport: "stdio", ServerURL: "https://mcp.example.com/v1", RemoteToolName: "lookup", ExpectedDeclarationDigest: valid.ExpectedDeclarationDigest, Timeout: time.Second},
		{Transport: "streamable", ServerURL: "http://mcp.example.com/v1", RemoteToolName: "lookup", ExpectedDeclarationDigest: valid.ExpectedDeclarationDigest, Timeout: time.Second},
		{Transport: "streamable", ServerURL: "https://127.0.0.1/v1", RemoteToolName: "lookup", ExpectedDeclarationDigest: valid.ExpectedDeclarationDigest, Timeout: time.Second},
		{Transport: "streamable", ServerURL: "https://mcp.example.com/v1?token=leak", RemoteToolName: "lookup", ExpectedDeclarationDigest: valid.ExpectedDeclarationDigest, Timeout: time.Second},
		{Transport: "streamable", ServerURL: "https://mcp.example.com/v1", RemoteToolName: "other", ExpectedDeclarationDigest: valid.ExpectedDeclarationDigest, Timeout: time.Second},
		{Transport: "streamable", ServerURL: "https://mcp.example.com/v1", RemoteToolName: "lookup", ExpectedDeclarationDigest: valid.ExpectedDeclarationDigest, Timeout: 0},
	}
	for _, config := range cases {
		if _, err := NewRegistration("tenant-a", "lookup", 1, config, secrets.SecretRef{}); err == nil {
			t.Fatalf("config=%#v accepted", config)
		}
	}
	if _, err := NewRegistration("tenant-a", "lookup", 1, valid, secrets.SecretRef{Ref: "secret://mcp", Version: 1}); err == nil {
		t.Fatal("secret without controlled header accepted")
	}
	if _, err := NewRegistration("tenant-a", "lookup", 1, Config{Transport: valid.Transport, ServerURL: valid.ServerURL,
		RemoteToolName: valid.RemoteToolName, Timeout: valid.Timeout, SecretHeader: "Cookie"}, secrets.SecretRef{Ref: "secret://mcp", Version: 1}); err == nil {
		t.Fatal("unsafe secret header accepted")
	}
}

func TestSafeHTTPHandlerRejectsHostnameResolvedToPrivateAddress(t *testing.T) {
	original := lookupIP
	lookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.0.0.8")}}, nil
	}
	t.Cleanup(func() { lookupIP = original })
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://reviewed.example.test/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (safeHTTPHandler{}).Handle(context.Background(), http.DefaultClient, request); err != runtime.ErrCapabilityUnsupported {
		t.Fatalf("private resolved address err=%v", err)
	}
}

type recordingSecrets struct {
	calls int
	scope secrets.Scope
	value []byte
}

func (s *recordingSecrets) Resolve(_ context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	s.calls++
	s.scope = scope
	return secrets.SecretValue{Bytes: append([]byte(nil), s.value...), Version: ref.Version}, nil
}

type recordingHandler struct {
	mu      sync.Mutex
	records []mcpRequest
}

type mcpRequest struct {
	method  string
	headers http.Header
}

func (h *recordingHandler) Handle(_ context.Context, _ *http.Client, request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		ID     any    `json:"id"`
		Method string `json:"method"`
		Params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		} `json:"params"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.records = append(h.records, mcpRequest{method: envelope.Method, headers: request.Header.Clone()})
	h.mu.Unlock()
	if envelope.ID == nil {
		return mcpResponse(http.StatusAccepted, nil, nil), nil
	}
	switch envelope.Method {
	case "initialize":
		return mcpResponse(http.StatusOK, envelope.ID, map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]any{"name": "test", "version": "1"}, "capabilities": map[string]any{}}), nil
	case "tools/list":
		return mcpResponse(http.StatusOK, envelope.ID, map[string]any{"tools": []map[string]any{{"name": "weather_lookup", "description": "tenant weather", "inputSchema": map[string]any{"type": "object"}}}}), nil
	case "tools/call":
		if envelope.Params.Name != "weather_lookup" {
			return nil, io.ErrUnexpectedEOF
		}
		return mcpResponse(http.StatusOK, envelope.ID, map[string]any{"content": []map[string]any{{"type": "text", "text": "weather:" + envelope.Params.Arguments["city"].(string)}}}), nil
	default:
		return mcpResponse(http.StatusOK, envelope.ID, map[string]any{}), nil
	}
}

func (h *recordingHandler) headers() []http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]http.Header, len(h.records))
	for index, record := range h.records {
		result[index] = record.headers
	}
	return result
}

func (h *recordingHandler) seen(method string) bool {
	for _, value := range h.methods() {
		if value == method {
			return true
		}
	}
	return false
}

func (h *recordingHandler) methods() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]string, len(h.records))
	for index, record := range h.records {
		result[index] = record.method
	}
	return result
}

func mcpResponse(status int, id any, result any) *http.Response {
	if id == nil {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(""))}
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body)))}
}
