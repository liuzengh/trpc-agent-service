package docsmcp

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session"
	coretool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const testToken = "only-a-test-token-0123456789abcdef0123456789"

func TestHTTPGuards(t *testing.T) {
	_, index := fixture(t)
	handler, err := NewHandler(index, testToken)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		method, path, token, origin, body string
		status                            int
	}{
		{"POST", "/mcp", "", "", `{}`, 401},
		{"POST", "/mcp", testToken, "https://untrusted.example", `{}`, 403},
		{"POST", "/mcp?token=anything", testToken, "", `{}`, 403},
		{"GET", "/mcp", testToken, "", "", 405},
		{"POST", "/mcp", testToken, "", strings.Repeat("x", (16<<10)+1), 413},
		{"POST", "/mcp", testToken, "", `{"method":"resources/read"}`, 400},
		{"GET", "/.env", testToken, "", "", 404},
	} {
		t.Run(test.method+test.path+test.origin, func(t *testing.T) {
			r := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			r.Header.Set("Content-Type", "application/json")
			if test.token != "" {
				r.Header.Set("Authorization", "Bearer "+test.token)
			}
			r.Header.Set("Origin", test.origin)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != test.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestRealMCPProtocolWithPlatformClient(t *testing.T) {
	_, index := fixture(t)
	handler, err := NewHandler(index, testToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	scope := runtimecontext.TutorialScope()
	raw, _ := json.Marshal(platformtool.MCPCredential{URL: server.URL + "/mcp", BearerToken: testToken, AllowedTools: []string{ToolName}, ReadOnlyTools: []string{ToolName}, TimeoutSeconds: 5})
	store := secret.StaticStore{"docs": string(raw)}
	spec := []platformtool.MCPServerSpec{{Name: "docs", CredentialRef: "docs", Tools: []string{ToolName}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			tools, err := platformtool.BuildMCPTools(ctx, store, scope, spec, []string{platformtool.MCPToolName("docs", ToolName)})
			if err != nil || len(tools) != 1 {
				t.Errorf("discover=%v count=%d", err, len(tools))
				return
			}
			inv := &agentcore.Invocation{Session: session.NewSession(scope.StorageScope, "user", "session"), RunOptions: agentcore.RunOptions{AppName: scope.StorageScope}}
			result, err := tools[0].(coretool.CallableTool).Call(agentcore.NewInvocationContext(ctx, inv), []byte(`{"query":"Redis Session","limit":2}`))
			if err != nil {
				t.Error(err)
				return
			}
			encoded, _ := json.Marshal(result)
			if !strings.Contains(string(encoded), "Redis Session") || strings.Contains(string(encoded), "secret-canary") {
				t.Error("invalid or unsafe MCP result")
			}
		}()
	}
	group.Wait()
}
