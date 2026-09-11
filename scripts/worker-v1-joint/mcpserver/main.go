// Command mcpserver is an isolated verification dependency, not a Worker service.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

const answer = "MCP_ORCHID_627: the orchid is violet.\nThe checked service window is 09:17 UTC."
const correction = "QUERY_REQUIRES_ORCHID: retry the selected tool with query=orchid."

type contextKey struct{}
type event struct {
	Method        string `json:"method"`
	Name          string `json:"name,omitempty"`
	Query         string `json:"query,omitempty"`
	Authenticated bool   `json:"authenticated"`
	IsError       bool   `json:"is_error,omitempty"`
}
type fixture struct {
	token   string
	mu      sync.Mutex
	events  []event
	handler http.Handler
}

func (f *fixture) add(e event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) < 1024 {
		f.events = append(f.events, e)
	}
}
func newFixture(token string) *fixture {
	f := &fixture{token: token, events: []event{}}
	middleware := func(next mcp.HandlerFunc) mcp.HandlerFunc {
		return func(ctx context.Context, r *mcp.JSONRPCRequest) (mcp.JSONRPCMessage, error) {
			f.add(event{Method: r.Method, Authenticated: ctx.Value(contextKey{}) == true})
			return next(ctx, r)
		}
	}
	server := mcp.NewServer("joint-mcp-real-server", "1.0.0", mcp.WithServerPath("/mcp"), mcp.WithStatelessMode(true), mcp.WithPostSSEEnabled(false), mcp.WithGetSSEEnabled(false), mcp.WithServerLogger(quietLogger{}), mcp.WithMiddleware(middleware))
	selected := mcp.NewTool("selected_search", mcp.WithDescription("Search the checked orchid service reference. Use query orchid to retrieve the verified canary."), mcp.WithString("query", mcp.Required(), mcp.Description("Search query, use orchid for the checked reference.")))
	server.RegisterTool(selected, func(ctx context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query, ok := r.Params.Arguments["query"].(string)
		bad := !ok || len(r.Params.Arguments) != 1 || query != "orchid"
		f.add(event{Method: "tool_execution", Name: r.Params.Name, Query: query, Authenticated: ctx.Value(contextKey{}) == true, IsError: bad})
		if bad {
			return mcp.NewErrorResult(correction), nil
		}
		return mcp.NewTextResult(answer), nil
	})
	server.RegisterTool(mcp.NewTool("unselected_secret", mcp.WithDescription("Unselected server tool; this tool must never be exposed to the model.")), func(ctx context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		f.add(event{Method: "tool_execution", Name: r.Params.Name, Authenticated: ctx.Value(contextKey{}) == true})
		return mcp.NewTextResult("UNSELECTED_TOOL_EXECUTED"), nil
	})
	f.handler = server.HTTPHandler()
	return f
}
func (f *fixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/fixture/state" {
		f.mu.Lock()
		snapshot := append([]event{}, f.events...)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"registered_tools": []string{"selected_search", "unselected_secret"}, "events": snapshot})
		return
	}
	if r.URL.Path != "/mcp" {
		http.NotFound(w, r)
		return
	}
	auth := subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+f.token)) == 1
	if !auth {
		f.add(event{Method: "http_denied", Authenticated: false})
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	f.handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, true)))
}
func main() {
	addr := flag.String("addr", "127.0.0.1:0", "isolated loopback listener")
	flag.Parse()
	host, _, err := net.SplitHostPort(*addr)
	if err != nil || host != "127.0.0.1" || os.Getenv("MCP_FIXTURE_TOKEN") == "" {
		fmt.Fprintln(os.Stderr, "invalid fixture configuration")
		os.Exit(2)
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fixture listen failed")
		os.Exit(1)
	}
	server := &http.Server{Handler: newFixture(os.Getenv("MCP_FIXTURE_TOKEN")), ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	fmt.Println("MCP_FIXTURE_READY=" + listener.Addr().String())
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	select {
	case <-ctx.Done():
	case err = <-stopped:
		if err != nil && err != http.ErrServerClosed {
			os.Exit(1)
		}
	}
	drain, end := context.WithTimeout(context.Background(), 5*time.Second)
	defer end()
	if server.Shutdown(drain) != nil {
		_ = server.Close()
		os.Exit(1)
	}
}

// MCP library logs are intentionally suppressed: credentials never enter logs.
type quietLogger struct{}

func (quietLogger) Debug(...interface{})          {}
func (quietLogger) Debugf(string, ...interface{}) {}
func (quietLogger) Info(...interface{})           {}
func (quietLogger) Infof(string, ...interface{})  {}
func (quietLogger) Warn(...interface{})           {}
func (quietLogger) Warnf(string, ...interface{})  {}
func (quietLogger) Error(...interface{})          {}
func (quietLogger) Errorf(string, ...interface{}) {}
func (quietLogger) Fatal(...interface{})          {}
func (quietLogger) Fatalf(string, ...interface{}) {}
