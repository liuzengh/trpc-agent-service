package mcptoolset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

type fixture struct {
	calls, lists, other                      atomic.Int32
	business, cancel, unauthorized, protocol atomic.Bool
	duplicate                                bool
	schema                                   any
}

func newFixture(t *testing.T, f *fixture, auth string) *httptest.Server {
	t.Helper()
	server := mcp.NewServer("fixture", "1", mcp.WithServerPath("/mcp"), mcp.WithStatelessMode(true))
	selected := mcp.NewTool("echo")
	if f.schema != nil {
		wire, _ := json.Marshal(f.schema)
		_ = json.Unmarshal(wire, &selected.InputSchema)
	}
	server.RegisterTool(selected, func(ctx context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		f.calls.Add(1)
		if f.protocol.Load() {
			return nil, errors.New("private-fixture-token provider diagnostic")
		}
		if f.cancel.Load() {
			select {
			case <-ctx.Done():
			case <-time.After(150 * time.Millisecond):
			}
			return nil, context.Canceled
		}
		return &mcp.CallToolResult{IsError: f.business.Load(), Content: []mcp.Content{mcp.NewTextContent("echo:" + r.Params.Arguments["q"].(string))}}, nil
	})
	server.RegisterTool(mcp.NewTool("unselected"), func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		f.other.Add(1)
		return &mcp.CallToolResult{}, nil
	})
	h := server.Handler()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != auth {
			t.Error("unexpected auth header")
		}
		if f.unauthorized.Load() {
			w.WriteHeader(401)
			_, _ = w.Write([]byte("private-fixture-token"))
			return
		}
		if r.Body != nil {
			body, _ := io.ReadAll(r.Body)
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			var envelope struct{ Method string }
			_ = json.Unmarshal(body, &envelope)
			if envelope.Method == "tools/list" {
				f.lists.Add(1)
				if f.duplicate || f.schema != nil {
					recorder := httptest.NewRecorder()
					h.ServeHTTP(recorder, r)
					for k, vs := range recorder.Header() {
						for _, v := range vs {
							w.Header().Add(k, v)
						}
					}
					w.WriteHeader(recorder.Code)
					transform := func(raw string) string {
						var envelope map[string]any
						if json.Unmarshal([]byte(raw), &envelope) != nil {
							return raw
						}
						result, _ := envelope["result"].(map[string]any)
						tools, _ := result["tools"].([]any)
						for _, value := range tools {
							entry, _ := value.(map[string]any)
							if entry["name"] == "echo" && f.schema != nil {
								entry["inputSchema"] = f.schema
							}
						}
						if f.duplicate {
							tools = append(tools, map[string]any{"name": "echo", "inputSchema": f.schema})
						}
						result["tools"] = tools
						wire, _ := json.Marshal(envelope)
						return string(wire)
					}
					wire := recorder.Body.String()
					if strings.Contains(recorder.Header().Get("Content-Type"), "text/event-stream") {
						lines := strings.Split(wire, "\n")
						for i, line := range lines {
							if strings.HasPrefix(line, "data:") {
								lines[i] = "data: " + transform(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
							}
						}
						wire = strings.Join(lines, "\n")
					} else {
						wire = transform(wire)
					}
					_, _ = w.Write([]byte(wire))
					return
				}
			}
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(httpServer.Close)
	return httpServer
}
func fixedSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}, "additionalProperties": false}
}
func TestMCPRealSDKSelectionAndCall(t *testing.T) {
	for _, auth := range []string{"none", "bearer"} {
		t.Run(auth, func(t *testing.T) {
			f := &fixture{schema: fixedSchema()}
			token, header := "", ""
			if auth == "bearer" {
				token = "private-fixture-token"
				header = "Bearer " + token
			}
			server := newFixture(t, f, header)
			s, err := Open(context.Background(), Config{ServerURL: server.URL + "/mcp", ToolsetName: "fixed", ToolName: "echo", AuthKind: auth, BearerToken: token, Timeout: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if f.lists.Load() != 1 || s.Tool().Declaration().Name != "echo" {
				t.Fatal("discovery selection")
			}
			if s.Tool().Declaration().InputSchema.AdditionalProperties != false {
				t.Fatal("additionalProperties:false lost")
			}
			gotSchema, marshalErr := json.Marshal(s.Tool().Declaration().InputSchema)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			wantSchema, _ := json.Marshal(fixedSchema())
			if string(gotSchema) != string(wantSchema) {
				var got, want any
				_ = json.Unmarshal(gotSchema, &got)
				_ = json.Unmarshal(wantSchema, &want)
				normalizedGot, _ := json.Marshal(got)
				normalizedWant, _ := json.Marshal(want)
				if string(normalizedGot) != string(normalizedWant) {
					t.Fatal("discovery schema changed", string(gotSchema))
				}
			}
			// Public declarations are detached copies.
			s.Tool().Declaration().InputSchema.Properties["q"].Type = "number"
			if s.Tool().Declaration().InputSchema.Properties["q"].Type != "string" {
				t.Fatal("schema alias")
			}
			result, err := s.Tool().Call(context.Background(), []byte(`{"q":"hello"}`))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(result)
			if !strings.Contains(string(body), "echo:hello") {
				t.Fatal(string(body))
			}
			if _, err = s.Tool().Call(context.Background(), []byte(`{"q":"hello","extra":true}`)); !errors.Is(err, ErrArguments) {
				t.Fatal(err)
			}
			f.business.Store(true)
			result, err = s.Tool().Call(context.Background(), []byte(`{"q":"correctable"}`))
			if err != nil {
				t.Fatal("business error became fatal", err)
			}
			flagged, ok := result.(interface{ RetryResultError() bool })
			if !ok || !flagged.RetryResultError() {
				t.Fatal("SDK IsError result lost")
			}
			f.business.Store(false)
			f.protocol.Store(true)
			if _, err = s.Tool().Call(context.Background(), []byte(`{"q":"protocol-failure"}`)); !errors.Is(err, ErrProtocol) || strings.Contains(err.Error(), token) && token != "" {
				t.Fatal("protocol diagnostic", err)
			}
			f.protocol.Store(false)
			f.unauthorized.Store(true)
			if _, err = s.Tool().Call(context.Background(), []byte(`{"q":"auth-failure"}`)); !errors.Is(err, ErrAuthentication) {
				t.Fatal("call authentication", err)
			}
			f.unauthorized.Store(false)
			if _, err = s.Tool().Call(context.Background(), []byte(`{"q":"after-business-error"}`)); err != nil {
				t.Fatal("business error became sticky", err)
			}
			if f.calls.Load() != 4 || f.other.Load() != 0 || f.lists.Load() != 1 {
				t.Fatal("extra discovery/call/unselected exposure")
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = s.Tool().Call(canceled, []byte(`{"q":"cancel"}`)); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			f.cancel.Store(true)
			deadline, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			if _, err = s.Tool().Call(deadline, []byte(`{"q":"wait"}`)); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			s.Close()
			if _, err = s.Tool().Call(context.Background(), []byte(`{"q":"closed"}`)); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}
func TestMCPDiscoveryFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    *fixture
		tool string
		want error
	}{
		{"missing", &fixture{schema: fixedSchema()}, "missing", ErrDiscovery},
		{"duplicate", &fixture{schema: fixedSchema(), duplicate: true}, "echo", ErrDiscovery},
		{"invalid", &fixture{schema: map[string]any{"type": "object", "required": "wrong"}}, "echo", ErrSchema},
		{"unrepresentable", &fixture{schema: map[string]any{"type": "object", "properties": map[string]any{"n": map[string]any{"type": "number", "minimum": 1}}}}, "echo", ErrSchema},
		{"external-ref", &fixture{schema: map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"$ref": "https://untrusted.invalid/schema"}}}}, "echo", ErrSchema},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newFixture(t, tc.f, "")
			s, err := Open(context.Background(), Config{ServerURL: server.URL + "/mcp", ToolsetName: "fixed", ToolName: tc.tool, AuthKind: "none", Timeout: time.Second})
			if s != nil || !errors.Is(err, tc.want) {
				t.Fatal(s, err)
			}
		})
	}
	t.Run("defaultSDKSchema", func(t *testing.T) {
		f := &fixture{}
		server := newFixture(t, f, "")
		s, err := Open(context.Background(), Config{ServerURL: server.URL + "/mcp", ToolsetName: "default", ToolName: "echo", AuthKind: "none", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	})
}
func TestMCPAuthenticationAndRedirect(t *testing.T) {
	f := &fixture{schema: fixedSchema()}
	f.unauthorized.Store(true)
	server := newFixture(t, f, "Bearer private-fixture-token")
	c := Config{ServerURL: server.URL + "/mcp", ToolsetName: "fixed", ToolName: "echo", AuthKind: "bearer", BearerToken: "private-fixture-token", Timeout: time.Second}
	if _, err := Open(context.Background(), c); !errors.Is(err, ErrAuthentication) || strings.Contains(err.Error(), c.BearerToken) {
		t.Fatal(err)
	}
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	c.ServerURL = redirect.URL
	if _, err := Open(context.Background(), c); !errors.Is(err, ErrProtocol) || leaked.Load() != 0 {
		t.Fatal(err)
	}
	broken := httptest.NewServer(http.NotFoundHandler())
	c.ServerURL = broken.URL
	broken.Close()
	if _, err := Open(context.Background(), c); !errors.Is(err, ErrNetwork) {
		t.Fatal(err)
	}
}

func TestMCPTLSAndClosedBoundaries(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("untrusted TLS reached handler") }))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	config := Config{ServerURL: server.URL, ToolsetName: "fixed", ToolName: "echo", AuthKind: "none", Timeout: time.Second}
	if _, err := Open(context.Background(), config); !errors.Is(err, ErrNetwork) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.AuthKind = "" }, func(c *Config) { c.BearerToken = "unexpected" }, func(c *Config) { c.ServerURL = "https://user:password@host/mcp" }, func(c *Config) { c.Timeout = 0 }} {
		bad := config
		mutate(&bad)
		if _, err := Open(context.Background(), bad); !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
	}
}
