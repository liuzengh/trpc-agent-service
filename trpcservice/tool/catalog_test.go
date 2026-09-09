package tool

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

const catalogCanary = "catalog-secret-canary"

type fakeCallable struct {
	name   string
	result any
	err    error
}

type ledgerFixture struct {
	mu       sync.Mutex
	status   ExecutionStatus
	result   any
	begin    int
	complete int
}

func (fixture *ledgerFixture) Begin(_ context.Context, _ ExecutionRecord) (ExecutionDecision, error) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.begin++
	if fixture.status == "" {
		fixture.status = ExecutionRunning
		return ExecutionDecision{Created: true, Status: ExecutionRunning}, nil
	}
	if fixture.status == ExecutionCompleted {
		return ExecutionDecision{Status: ExecutionCompleted, Result: fixture.result}, nil
	}
	return ExecutionDecision{Status: fixture.status}, nil
}

func (fixture *ledgerFixture) Complete(_ context.Context, _, _, _ string, result any) error {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.complete++
	fixture.status = ExecutionCompleted
	fixture.result = result
	return nil
}

func (fixture *ledgerFixture) Fail(_ context.Context, _, _, _, _ string, status ExecutionStatus) error {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.status = status
	return nil
}

func ledgerToolContext() context.Context {
	ctx := catalogPolicyContext("ledger_tool")
	return context.WithValue(ctx, trpctool.ContextKeyToolCallID{}, "call-1")
}

func TestExecutionLedgerFirstCallCompletesAndReplays(t *testing.T) {
	fixture := &ledgerFixture{}
	var calls atomic.Int32
	wrapped := &executionLedgerTool{
		name: "ledger_tool", store: fixture,
		delegate: fakeCallable{name: "ledger_tool", result: "fresh"},
	}
	wrapped.delegate = callableFunc{declaration: (&fakeCallable{name: "ledger_tool"}).Declaration(), call: func(context.Context, []byte) (any, error) {
		calls.Add(1)
		return "fresh", nil
	}}

	value, err := wrapped.Call(ledgerToolContext(), []byte(`{"value":1}`))
	if err != nil || value != "fresh" {
		t.Fatalf("first call = %#v, %v", value, err)
	}
	value, err = wrapped.Call(ledgerToolContext(), []byte(`{"value":1}`))
	if err != nil || value != "fresh" {
		t.Fatalf("replay = %#v, %v", value, err)
	}
	if calls.Load() != 1 || fixture.begin != 2 || fixture.complete != 1 {
		t.Fatalf("ledger counts calls=%d begin=%d complete=%d", calls.Load(), fixture.begin, fixture.complete)
	}
}

func TestExecutionLedgerRejectsExistingRunningDuplicate(t *testing.T) {
	fixture := &ledgerFixture{status: ExecutionRunning}
	var calls atomic.Int32
	wrapped := &executionLedgerTool{
		name: "ledger_tool", store: fixture,
		delegate: callableFunc{declaration: (&fakeCallable{name: "ledger_tool"}).Declaration(), call: func(context.Context, []byte) (any, error) {
			calls.Add(1)
			return "unexpected", nil
		}},
	}
	if _, err := wrapped.Call(ledgerToolContext(), []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "reconciliation") {
		t.Fatalf("duplicate error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("duplicate executed delegate %d times", calls.Load())
	}
}

type callableFunc struct {
	declaration *trpctool.Declaration
	call        func(context.Context, []byte) (any, error)
}

func (tool callableFunc) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool callableFunc) Call(ctx context.Context, args []byte) (any, error) {
	return tool.call(ctx, args)
}

func (tool fakeCallable) Declaration() *trpctool.Declaration {
	return &trpctool.Declaration{Name: tool.name, Description: "fixture", InputSchema: &trpctool.Schema{Type: "object"}}
}
func (tool fakeCallable) Call(context.Context, []byte) (any, error) { return tool.result, tool.err }
func (tool fakeCallable) ToolMetadata() trpctool.ToolMetadata {
	return trpctool.ToolMetadata{Destructive: true}
}

type compatibleRemoteResult struct{}

func (compatibleRemoteResult) MarshalJSON() ([]byte, error) {
	return []byte(`{"token":"` + catalogCanary + `"}`), nil
}
func (compatibleRemoteResult) GetCallbackResult() any {
	return map[string]any{"secret": catalogCanary}
}
func (compatibleRemoteResult) GetMeta() map[string]any {
	return map[string]any{"authorization": catalogCanary}
}
func (compatibleRemoteResult) RetryResultError() bool { return true }

type fakeMCPSet struct {
	tools  []trpctool.Tool
	init   error
	closed atomic.Int32
}

func (set *fakeMCPSet) Init(context.Context) error            { return set.init }
func (set *fakeMCPSet) Tools(context.Context) []trpctool.Tool { return set.tools }
func (set *fakeMCPSet) Close() error                          { set.closed.Add(1); return nil }

func catalogPolicyContext(toolName string) context.Context {
	engine := &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}}
	request := policy.Request{TenantID: "tenant-a", AppID: "app-a", UserID: "user-a", RequestID: "request-1", Policy: tenant.ToolPolicy{Allow: []string{toolName}}}
	return policy.WithRequest(context.Background(), engine, request)
}

func TestCatalogNamespacesMCPToolsAndRedactsResults(t *testing.T) {
	registry, err := NewCatalogRegistry(func(tenant.SecretRef) (string, error) { return catalogCanary, nil })
	if err != nil {
		t.Fatal(err)
	}
	sets := map[string]*fakeMCPSet{}
	registry.mcpFactory = func(server tenant.MCPServer, headers map[string]string) mcpToolSet {
		if headers["Authorization"] != "Bearer "+catalogCanary {
			t.Fatalf("authorization header = %q", headers["Authorization"])
		}
		set := &fakeMCPSet{tools: []trpctool.Tool{fakeCallable{name: "lookup", result: map[string]any{"token": catalogCanary, "value": "safe"}}}}
		sets[server.ID] = set
		return set
	}
	app := tenant.AgentApp{MCPServers: []tenant.MCPServer{
		{ID: "crm", Endpoint: "https://crm.example.com", Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "CRM_TOKEN"}, AllowedTools: []string{"lookup"}, Enabled: true},
		{ID: "erp", Endpoint: "https://erp.example.com", Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "ERP_TOKEN"}, AllowedTools: []string{"lookup"}, Enabled: true},
	}}
	catalog, err := registry.Build(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mcp__crm__lookup", "mcp__erp__lookup"} {
		candidate, ok := catalog.Tools()[name].(trpctool.CallableTool)
		if !ok {
			t.Fatalf("missing callable %q", name)
		}
		result, err := candidate.Call(catalogPolicyContext(name), []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		text := fmt.Sprint(result)
		if strings.Contains(text, catalogCanary) || !strings.Contains(text, "[REDACTED]") {
			t.Fatalf("result was not redacted: %v", result)
		}
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	for name, set := range sets {
		if set.closed.Load() != 1 {
			t.Fatalf("server %q close count = %d", name, set.closed.Load())
		}
	}
}

func TestCatalogFailureIsGenericAndClosesInitializedSessions(t *testing.T) {
	registry, _ := NewCatalogRegistry(func(tenant.SecretRef) (string, error) { return catalogCanary, nil })
	first := &fakeMCPSet{tools: []trpctool.Tool{fakeCallable{name: "lookup"}}}
	second := &fakeMCPSet{init: errors.New("upstream leaked " + catalogCanary)}
	registry.mcpFactory = func(server tenant.MCPServer, _ map[string]string) mcpToolSet {
		if server.ID == "first" {
			return first
		}
		return second
	}
	_, err := registry.Build(context.Background(), tenant.AgentApp{MCPServers: []tenant.MCPServer{
		{ID: "first", Enabled: true, AllowedTools: []string{"lookup"}},
		{ID: "second", Enabled: true, AllowedTools: []string{"lookup"}},
	}})
	if err == nil || strings.Contains(err.Error(), catalogCanary) {
		t.Fatalf("unsafe error = %v", err)
	}
	if first.closed.Load() != 1 {
		t.Fatalf("initialized session close count = %d", first.closed.Load())
	}
	if second.closed.Load() != 1 {
		t.Fatalf("failed session close count = %d", second.closed.Load())
	}
}

func TestSafeRemoteToolPreservesResultAndMetadataContracts(t *testing.T) {
	wrapped := &safeRemoteTool{
		delegate:    fakeCallable{name: "danger", result: compatibleRemoteResult{}},
		declaration: &trpctool.Declaration{Name: "mcp__remote__danger"},
		redactor:    servicelog.NewRedactor(nil, []string{catalogCanary}),
		callGate:    make(chan struct{}, 1),
	}
	metadata := trpctool.MetadataOf(wrapped)
	if !metadata.Destructive || !metadata.OpenWorld {
		t.Fatalf("metadata = %+v", metadata)
	}
	value, err := wrapped.Call(catalogPolicyContext("mcp__remote__danger"), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	result, ok := value.(*safeRemoteResult)
	if !ok || !result.RetryResultError() {
		t.Fatalf("compatible result = %#v", value)
	}
	for _, candidate := range []any{result.value, result.GetCallbackResult(), result.GetMeta()} {
		if text := fmt.Sprint(candidate); strings.Contains(text, catalogCanary) || !strings.Contains(text, "[REDACTED]") {
			t.Fatalf("compatible result leaked: %v", candidate)
		}
	}
}

func TestHTTPBusinessToolPolicyHeadersLimitsAndRedaction(t *testing.T) {
	var reject atomic.Bool
	var calls atomic.Uint64
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("Authorization") != "Bearer "+catalogCanary {
			t.Errorf("unexpected authenticated request")
		}
		expectedKey := fmt.Sprintf("request-1:ticket_lookup:%d", calls.Add(1))
		if request.Header.Get("X-Idempotency-Key") != expectedKey {
			t.Errorf("idempotency key = %q", request.Header.Get("X-Idempotency-Key"))
		}
		writer.Header().Set("Content-Type", "application/json")
		if reject.Load() {
			writer.WriteHeader(http.StatusUnauthorized)
			_, _ = writer.Write([]byte(`{"error":"` + catalogCanary + `"}`))
			return
		}
		_, _ = writer.Write([]byte(`{"token":"` + catalogCanary + `","status":"ok"}`))
	}))
	defer server.Close()
	tool := &HTTPJSONTool{
		config:     tenant.HTTPBusinessTool{Name: "ticket_lookup", Description: "Lookup ticket", Endpoint: server.URL},
		credential: catalogCanary, client: server.Client(), redactor: servicelog.NewRedactor(nil, []string{catalogCanary}),
	}
	if _, err := tool.Call(context.Background(), []byte(`{}`)); !errors.Is(err, policy.ErrToolDenied) {
		t.Fatalf("call without policy error = %v", err)
	}
	ctx := catalogPolicyContext("ticket_lookup")
	result, err := tool.Call(ctx, []byte(`{"ticket_id":"42"}`))
	if err != nil {
		t.Fatal(err)
	}
	text := fmt.Sprint(result)
	if strings.Contains(text, catalogCanary) || !strings.Contains(text, "[REDACTED]") {
		t.Fatalf("business result leaked: %v", result)
	}
	reject.Store(true)
	if _, err := tool.Call(ctx, []byte(`{"ticket_id":"42"}`)); err == nil || strings.Contains(err.Error(), catalogCanary) {
		t.Fatalf("unsafe rejected response error = %v", err)
	}
	reject.Store(false)
	if _, err := tool.Call(ctx, []byte(`[]`)); err == nil {
		t.Fatal("array request should be rejected")
	}
}

func TestCatalogRealStreamableMCPRoundTrip(t *testing.T) {
	server := mcp.NewServer("catalog-test", "1.0.0", mcp.WithServerPath("/mcp"))
	server.RegisterTool(mcp.NewTool("lookup", mcp.WithDescription("Lookup a record"), mcp.WithString("id")), func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewTextResult("record:" + fmt.Sprint(request.Params.Arguments["id"])), nil
	})
	httpServer := httptest.NewServer(server.HTTPHandler())
	defer httpServer.Close()
	registry, _ := NewCatalogRegistry(func(tenant.SecretRef) (string, error) { return "", nil })
	catalog, err := registry.Build(context.Background(), tenant.AgentApp{MCPServers: []tenant.MCPServer{{ID: "records", Endpoint: httpServer.URL + "/mcp", AllowedTools: []string{"lookup"}, Enabled: true}}})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	callable := catalog.Tools()["mcp__records__lookup"].(trpctool.CallableTool)
	result, err := callable.Call(catalogPolicyContext("mcp__records__lookup"), []byte(`{"id":"42"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(result), "record:42") {
		t.Fatalf("unexpected MCP result: %v", result)
	}
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 12)
	for index := 0; index < 12; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			value, err := callable.Call(catalogPolicyContext("mcp__records__lookup"), []byte(fmt.Sprintf(`{"id":"%d"}`, index)))
			if err != nil {
				errorsSeen <- err
				return
			}
			if !strings.Contains(fmt.Sprint(value), fmt.Sprintf("record:%d", index)) {
				errorsSeen <- fmt.Errorf("unexpected concurrent result: %v", value)
			}
		}(index)
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
}
