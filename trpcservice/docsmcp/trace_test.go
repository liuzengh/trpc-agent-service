package docsmcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session"
	coretool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestMCPHTTPPreservesTraceWithoutBaggage(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) }()
	_, index := fixture(t)
	handler, err := NewHandler(index, testToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Traceparent") == "" {
			t.Error("MCP traceparent missing")
		}
		if r.Header.Get("Baggage") != "" {
			t.Error("baggage crossed MCP boundary")
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	member, _ := baggage.NewMember("private", "baggage-canary")
	bag, _ := baggage.New(member)
	ctx, parent := provider.Tracer("test").Start(baggage.ContextWithBaggage(context.Background(), bag), "tool-parent")
	defer parent.End()
	scope := runtimecontext.TutorialScope()
	raw, _ := json.Marshal(platformtool.MCPCredential{URL: server.URL + "/mcp", BearerToken: testToken, AllowedTools: []string{ToolName}, ReadOnlyTools: []string{ToolName}})
	tools, err := platformtool.BuildMCPTools(ctx, secret.StaticStore{"docs": string(raw)}, scope, []platformtool.MCPServerSpec{{Name: "docs", CredentialRef: "docs", Tools: []string{ToolName}}}, []string{"mcp_docs_search_project_docs"})
	if err != nil {
		t.Fatal(err)
	}
	inv := &agentcore.Invocation{Session: session.NewSession(scope.StorageScope, "user", "session"), RunOptions: agentcore.RunOptions{AppName: scope.StorageScope}}
	if _, err := tools[0].(coretool.CallableTool).Call(agentcore.NewInvocationContext(ctx, inv), []byte(`{"query":"Redis Session"}`)); err != nil {
		t.Fatal(err)
	}
	requestFound, searchFound := false, false
	for _, span := range recorder.Ended() {
		if span.Name() == "mcp.docs.request" {
			requestFound = true
			if span.Parent().SpanID() != parent.SpanContext().SpanID() {
				t.Error("MCP HTTP span disconnected from Tool parent")
			}
		}
		if span.Name() == "mcp.docs.search" {
			searchFound = true
			if span.SpanContext().TraceID() != parent.SpanContext().TraceID() {
				t.Error("search trace disconnected")
			}
		}
	}
	if !requestFound || !searchFound {
		t.Fatal("missing MCP request/search spans")
	}
}
