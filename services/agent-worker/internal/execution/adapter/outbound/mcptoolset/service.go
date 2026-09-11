// Package mcptoolset selects exactly one discovered SDK MCP callable from one
// fixed Manifest resource. It never exposes a ToolSet to an Agent.
package mcptoolset

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	sdkmcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

var (
	ErrConfig         = errors.New("MCP configuration invalid")
	ErrDiscovery      = errors.New("MCP selected tool missing or ambiguous")
	ErrSchema         = errors.New("MCP schema invalid or not faithfully representable")
	ErrAuthentication = errors.New("MCP authentication failed")
	ErrNetwork        = errors.New("MCP network failed")
	ErrProtocol       = errors.New("MCP protocol failed")
	ErrArguments      = errors.New("MCP arguments invalid")
	ErrClosed         = errors.New("MCP service closed")
)

type Config struct {
	ServerURL, ToolsetName, ToolName, AuthKind, BearerToken string
	Timeout                                                 time.Duration
}
type Service struct {
	set         *sdkmcp.ToolSet
	selected    tool.CallableTool
	declaration []byte
	input       *jsonschema.Schema
	config      Config
	transport   *http.Transport
	life        context.Context
	cancel      context.CancelFunc
	once        sync.Once
}
type operation struct {
	err error
	raw []rawTool
}
type operationKey struct{}
type rawTool struct {
	Name   string          `json:"name"`
	Input  json.RawMessage `json:"inputSchema"`
	Output json.RawMessage `json:"outputSchema"`
}
type boundHTTP struct {
	client     *http.Client
	url, token string
}

func (h *boundHTTP) Handle(ctx context.Context, _ *http.Client, req *http.Request) (*http.Response, error) {
	state, _ := ctx.Value(operationKey{}).(*operation)
	fail := func(err error) (*http.Response, error) {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if state != nil {
			state.err = err
		}
		return nil, err
	}
	if req.URL.String() != h.url {
		return fail(ErrProtocol)
	}
	req = req.Clone(ctx)
	req.Header.Del("Authorization")
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	var method string
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return fail(ErrNetwork)
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body))
		var envelope struct{ Method string }
		if json.Unmarshal(body, &envelope) != nil {
			return fail(ErrProtocol)
		}
		method = envelope.Method
	}
	response, err := h.client.Do(req)
	if err != nil {
		return fail(ErrNetwork)
	}
	if response.StatusCode == 401 || response.StatusCode == 403 {
		response.Body.Close()
		return fail(ErrAuthentication)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		response.Body.Close()
		return fail(ErrProtocol)
	}
	// Streamable HTTP POST responses terminate with the response to this operation;
	// unsolicited GET SSE is disabled. Observe bytes, leave successful wire intact.
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return fail(ErrNetwork)
	}
	if h.token != "" && bytes.Contains(body, []byte(h.token)) {
		return fail(ErrProtocol)
	}
	if len(bytes.TrimSpace(body)) > 0 {
		envelopes := [][]byte{body}
		if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
			envelopes = nil
			for _, event := range strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n\n") {
				var data []string
				for _, line := range strings.Split(event, "\n") {
					if strings.HasPrefix(line, "data:") {
						data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
					}
				}
				if len(data) > 0 {
					envelopes = append(envelopes, []byte(strings.Join(data, "\n")))
				}
			}
		}
		for _, wire := range envelopes {
			var e struct {
				Error  json.RawMessage
				Result json.RawMessage
			}
			if json.Unmarshal(wire, &e) != nil {
				return fail(ErrProtocol)
			}
			if len(e.Error) > 0 && string(e.Error) != "null" {
				return fail(ErrProtocol)
			}
			if method == "tools/list" && len(e.Result) > 0 {
				var list struct {
					Tools      []rawTool
					NextCursor string
				}
				if json.Unmarshal(e.Result, &list) != nil || list.NextCursor != "" {
					return fail(ErrDiscovery)
				}
				if state == nil {
					return fail(ErrProtocol)
				}
				state.raw = list.Tools
			}
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}
func Open(ctx context.Context, c Config) (*Service, error) {
	u, err := url.Parse(c.ServerURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.TrimSpace(c.ToolName) == "" || strings.TrimSpace(c.ToolsetName) == "" || c.Timeout <= 0 {
		return nil, ErrConfig
	}
	if (c.AuthKind != "none" && c.AuthKind != "bearer") || (c.AuthKind == "none" && c.BearerToken != "") || (c.AuthKind == "bearer" && (strings.TrimSpace(c.BearerToken) == "" || strings.ContainsAny(c.BearerToken, "\r\n"))) {
		return nil, ErrConfig
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	life, cancel := context.WithCancel(context.Background())
	s := &Service{config: c, transport: tr, life: life, cancel: cancel}
	var selected []tool.CallableTool
	s.set = sdkmcp.NewMCPToolSet(sdkmcp.ConnectionConfig{Transport: "streamable", ServerURL: c.ServerURL, Timeout: c.Timeout}, sdkmcp.WithName(c.ToolsetName), sdkmcp.WithMCPOptions(mcp.WithClientGetSSEEnabled(false), mcp.WithHTTPReqHandler(&boundHTTP{client: client, url: c.ServerURL, token: c.BearerToken})), sdkmcp.WithToolFilterFunc(func(_ context.Context, t tool.Tool) bool {
		if t.Declaration() != nil && t.Declaration().Name == c.ToolName {
			if callable, ok := t.(tool.CallableTool); ok {
				selected = append(selected, callable)
			}
		}
		return false
	}))
	op, done, state := s.operation(ctx)
	defer done()
	if err = s.set.Init(op); err != nil {
		result := classify(op, state, ErrProtocol)
		s.Close()
		return nil, result
	}
	var candidates []rawTool
	for _, r := range state.raw {
		if r.Name == c.ToolName {
			candidates = append(candidates, r)
		}
	}
	if len(candidates) != 1 {
		s.Close()
		return nil, ErrDiscovery
	}
	input, compiled, err := faithfulSchema(candidates[0].Input, true)
	if err != nil {
		s.Close()
		return nil, err
	}
	output, _, err := faithfulSchema(candidates[0].Output, false)
	if err != nil {
		s.Close()
		return nil, err
	}
	if len(selected) != 1 {
		s.Close()
		return nil, ErrDiscovery
	}
	declaration := *selected[0].Declaration()
	declaration.InputSchema = input
	declaration.OutputSchema = output
	s.declaration, err = json.Marshal(declaration)
	if err != nil {
		s.Close()
		return nil, ErrSchema
	}
	s.selected = selected[0]
	s.input = compiled
	return s, nil
}
func (s *Service) operation(ctx context.Context) (context.Context, func(), *operation) {
	op, cancel := context.WithTimeout(ctx, s.config.Timeout)
	stop := context.AfterFunc(s.life, cancel)
	state := &operation{}
	return context.WithValue(op, operationKey{}, state), func() { stop(); cancel() }, state
}
func classify(ctx context.Context, state *operation, fallback error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if state.err != nil {
		return state.err
	}
	return fallback
}
func (s *Service) Tool() tool.CallableTool { return s }
func (s *Service) Declaration() *tool.Declaration {
	var d tool.Declaration
	_ = json.Unmarshal(s.declaration, &d)
	return &d
}
func (s *Service) ToolMetadata() tool.ToolMetadata {
	if p, ok := s.selected.(tool.MetadataProvider); ok {
		return p.ToolMetadata()
	}
	return tool.ToolMetadata{}
}
func (s *Service) Call(ctx context.Context, args []byte) (any, error) {
	if s.life.Err() != nil {
		return nil, ErrClosed
	}
	op, done, state := s.operation(ctx)
	defer done()
	var value any
	if len(args) == 0 {
		args = []byte("{}")
	}
	if json.Unmarshal(args, &value) != nil || s.input.Validate(value) != nil {
		return nil, ErrArguments
	}
	result, err := s.selected.Call(op, args)
	if err != nil {
		return nil, classify(op, state, ErrProtocol)
	}
	// Preserve the SDK result object including IsError/RetryResultError metadata.
	// Business tool errors are results for model correction, not fatal errors.
	return result, nil
}
func (s *Service) Close() error {
	s.once.Do(func() { s.cancel(); _ = s.set.Close(); s.transport.CloseIdleConnections() })
	return nil
}

type noRemoteSchemas struct{}

func (noRemoteSchemas) Load(string) (any, error) { return nil, ErrSchema }
func faithfulSchema(raw json.RawMessage, required bool) (*tool.Schema, *jsonschema.Schema, error) {
	if len(raw) == 0 || string(raw) == "null" {
		if !required {
			return nil, nil, nil
		}
		return nil, nil, ErrSchema
	}
	var original map[string]any
	if json.Unmarshal(raw, &original) != nil || original["type"] != "object" {
		return nil, nil, ErrSchema
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noRemoteSchemas{})
	if compiler.AddResource("urn:worker:mcp:schema", original) != nil {
		return nil, nil, ErrSchema
	}
	compiled, err := compiler.Compile("urn:worker:mcp:schema")
	if err != nil {
		return nil, nil, ErrSchema
	}
	var schema tool.Schema
	if json.Unmarshal(raw, &schema) != nil {
		return nil, nil, ErrSchema
	}
	roundtrip, _ := json.Marshal(schema)
	var actual map[string]any
	_ = json.Unmarshal(roundtrip, &actual)
	normalizeSchema(original)
	normalizeSchema(actual)
	if !reflect.DeepEqual(original, actual) {
		return nil, nil, ErrSchema
	}
	return &schema, compiled, nil
}
func normalizeSchema(m map[string]any) {
	for k, v := range m {
		switch x := v.(type) {
		case map[string]any:
			if (k == "properties" || k == "$defs") && len(x) == 0 {
				delete(m, k)
				continue
			}
			normalizeSchema(x)
		case []any:
			if k == "required" && len(x) == 0 {
				delete(m, k)
			}
		}
	}
}

var _ tool.CallableTool = (*Service)(nil)
