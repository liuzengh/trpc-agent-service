package main

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

func TestRealMCPServerSelectedAndCorrectableBusinessError(t *testing.T) {
	f := newFixture("private-fixture-token")
	s := httptest.NewServer(f)
	defer s.Close()
	c, e := mcp.NewClient(s.URL+"/mcp", mcp.Implementation{Name: "actual-fixture-client", Version: "1"}, mcp.WithHTTPHeaders(http.Header{"Authorization": []string{"Bearer private-fixture-token"}}), mcp.WithClientGetSSEEnabled(false), mcp.WithClientLogger(quietLogger{}))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, e = c.Initialize(ctx, &mcp.InitializeRequest{}); e != nil {
		t.Fatal(e)
	}
	list, e := c.ListTools(ctx, &mcp.ListToolsRequest{})
	if e != nil || len(list.Tools) != 2 {
		t.Fatalf("real tools/list: %#v %v", list, e)
	}
	// The SDK enumerates tools from a map; tools/list has no stable order.
	slices.SortFunc(list.Tools, func(a, b mcp.Tool) int { return cmp.Compare(a.Name, b.Name) })
	if list.Tools[1].Name != "unselected_secret" {
		t.Fatal("unexpected registered tool")
	}
	if list.Tools[0].Name != "selected_search" || len(list.Tools[0].InputSchema.Required) != 1 || list.Tools[0].InputSchema.Required[0] != "query" {
		t.Fatal("selected input schema changed")
	}
	for _, query := range []string{"correct-me", "orchid"} {
		got, e := c.CallTool(ctx, &mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "selected_search", Arguments: map[string]interface{}{"query": query}}})
		if e != nil {
			t.Fatal(e)
		}
		want := answer
		if query != "orchid" {
			want = correction
		}
		if got.IsError != (query != "orchid") || len(got.Content) != 1 || got.Content[0].(mcp.TextContent).Text != want {
			t.Fatalf("actual MCP result differs: %#v", got)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := 0
	for _, e := range f.events {
		if !e.Authenticated {
			t.Fatal("authenticated MCP context lost")
		}
		if e.Method == "tool_execution" {
			calls++
			if e.Name != "selected_search" {
				t.Fatal("unselected called")
			}
		}
	}
	if calls != 2 {
		t.Fatalf("calls %d", calls)
	}
	raw, _ := json.Marshal(f.events)
	if strings.Contains(string(raw), "private-fixture-token") {
		t.Fatal("credential leaked")
	}
}
func TestRealMCPServerRejectsAuthenticationBeforeProtocol(t *testing.T) {
	f := newFixture("private-fixture-token")
	s := httptest.NewServer(f)
	defer s.Close()
	r, e := http.Post(s.URL+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","method":"initialize","id":1}`))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Body.Close()
	if r.StatusCode != 401 {
		t.Fatal(r.StatusCode)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) != 1 || f.events[0].Method != "http_denied" {
		t.Fatal(f.events)
	}
}
