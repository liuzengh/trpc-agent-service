package tool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"go.opentelemetry.io/otel/propagation"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	coretool "trpc.group/trpc-go/trpc-agent-go/tool"
	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

type MCPServerSpec struct {
	Name          string   `json:"name"`
	CredentialRef string   `json:"credential_ref"`
	Tools         []string `json:"tools"`
}

// Credentials are deployment-owned JSON in a Secret, never tenant-editable
// URLs/headers. Read-only exceptions are also deployment grants, not MCP hints.
type MCPCredential struct {
	URL            string   `json:"url"`
	BearerToken    string   `json:"bearer_token"`
	AllowedTools   []string `json:"allowed_tools"`
	ReadOnlyTools  []string `json:"read_only_tools"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

var mcpName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,31}$`)

func MCPToolName(server, name string) string { return "mcp_" + server + "_" + name }
func ParseMCPServers(raw json.RawMessage) ([]MCPServerSpec, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, errors.New("invalid Agent MCP configuration")
	}
	var cfg struct{ Servers []MCPServerSpec }
	if value, ok := values["mcp_servers"]; ok {
		d := json.NewDecoder(bytes.NewReader(value))
		d.DisallowUnknownFields()
		if d.Decode(&cfg.Servers) != nil || d.Decode(new(any)) != io.EOF {
			return nil, errors.New("invalid Agent MCP server configuration")
		}
	}
	if len(cfg.Servers) > 8 {
		return nil, errors.New("too many MCP servers")
	}
	seen := map[string]bool{}
	for _, s := range cfg.Servers {
		if !mcpName.MatchString(s.Name) || s.CredentialRef == "" || len(s.Tools) == 0 || len(s.Tools) > 32 {
			return nil, errors.New("MCP name, credential reference and explicit tool list required")
		}
		for _, name := range s.Tools {
			alias := MCPToolName(s.Name, name)
			if !mcpName.MatchString(name) || len(alias) > 64 || seen[alias] {
				return nil, errors.New("invalid or duplicate MCP tool name")
			}
			seen[alias] = true
		}
	}
	return cfg.Servers, nil
}
func MCPLocalTools(servers []MCPServerSpec, allowed []string) []string {
	remote := map[string]bool{}
	for _, s := range servers {
		for _, name := range s.Tools {
			remote[MCPToolName(s.Name, name)] = true
		}
	}
	var local []string
	for _, name := range allowed {
		if !remote[name] {
			local = append(local, name)
		}
	}
	return local
}
func loadMCPCredential(ctx context.Context, store secret.Store, tenantID, ref string) (MCPCredential, error) {
	raw, err := store.Resolve(ctx, tenantID, secret.MCPServer, ref)
	if err != nil {
		return MCPCredential{}, secret.ErrForbidden
	}
	var cfg MCPCredential
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil || d.Decode(new(any)) != io.EOF {
		return cfg, errors.New("invalid MCP deployment credential")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" || u.Path == "" {
		return cfg, errors.New("MCP deployment endpoint must be an explicit HTTP(S) URL")
	}
	if cfg.TimeoutSeconds == 0 {
		cfg.TimeoutSeconds = 20
	}
	if cfg.TimeoutSeconds < 1 || cfg.TimeoutSeconds > 60 || len(cfg.AllowedTools) == 0 || len(cfg.AllowedTools) > 128 || strings.ContainsAny(cfg.BearerToken, "\r\n") {
		return cfg, errors.New("invalid MCP deployment limits")
	}
	for _, name := range append(slices.Clone(cfg.AllowedTools), cfg.ReadOnlyTools...) {
		if !mcpName.MatchString(name) {
			return cfg, errors.New("invalid MCP deployment tool name")
		}
	}
	return cfg, nil
}
func MCPDangerousTools(ctx context.Context, store secret.Store, tenantID string, servers []MCPServerSpec) ([]string, error) {
	var out []string
	for _, s := range servers {
		cfg, err := loadMCPCredential(ctx, store, tenantID, s.CredentialRef)
		if err != nil {
			return nil, err
		}
		for _, name := range s.Tools {
			if !slices.Contains(cfg.AllowedTools, name) {
				return nil, secret.ErrForbidden
			}
			if !slices.Contains(cfg.ReadOnlyTools, name) {
				out = append(out, MCPToolName(s.Name, name))
			}
		}
	}
	return out, nil
}

type remoteMCPTool struct {
	declaration                    *coretool.Declaration
	scope                          runtimecontext.Scope
	spec                           MCPServerSpec
	name, endpointHash, schemaHash string
	readOnly                       bool
	store                          secret.Store
}

func (t *remoteMCPTool) Declaration() *coretool.Declaration {
	b, _ := json.Marshal(t.declaration)
	var d coretool.Declaration
	_ = json.Unmarshal(b, &d)
	return &d
}
func (t *remoteMCPTool) Call(ctx context.Context, args []byte) (any, error) {
	inv, ok := agentcore.InvocationFromContext(ctx)
	if !ok || inv == nil || inv.Session == nil || inv.Session.AppName != t.scope.StorageScope || inv.RunOptions.AppName != t.scope.StorageScope || len(args) > 65536 {
		return nil, secret.ErrForbidden
	}
	cfg, err := loadMCPCredential(ctx, t.store, t.scope.TenantID, t.spec.CredentialRef)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(cfg.AllowedTools, t.name) || mcpHash([]byte(cfg.URL)) != t.endpointHash || (t.readOnly && !slices.Contains(cfg.ReadOnlyTools, t.name)) {
		return nil, secret.ErrForbidden
	}
	var arguments map[string]any
	d := json.NewDecoder(bytes.NewReader(args))
	d.UseNumber()
	if d.Decode(&arguments) != nil || d.Decode(new(any)) != io.EOF || arguments == nil {
		return nil, errors.New("MCP arguments must be one JSON object")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutSeconds)*time.Second)
	defer cancel()
	client, closeClient, err := openMCP(ctx, cfg, t.name)
	if err != nil {
		return nil, err
	}
	defer closeClient()
	tools, err := listMCPTools(ctx, client)
	if err != nil {
		return nil, err
	}
	remote, ok := tools[t.name]
	if !ok || mcpSchemaHash(remote) != t.schemaHash {
		return nil, errors.New("MCP tool schema changed; publish a new revision")
	}
	request := &mcp.CallToolRequest{}
	request.Params.Name = t.name
	request.Params.Arguments = arguments
	result, err := client.CallTool(ctx, request)
	if err != nil || result == nil {
		return nil, errors.New("MCP tool outcome unavailable")
	}
	if result.IsError {
		return nil, errors.New("MCP tool reported an execution error")
	}
	var payload any = result.Content
	if result.StructuredContent != nil {
		payload = map[string]any{"content": result.Content, "structured_content": result.StructuredContent}
	}
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > 65536 {
		return nil, errors.New("MCP result exceeds safe output bound")
	}
	var out any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&out) != nil {
		return nil, errors.New("invalid MCP result")
	}
	out, err = scrubMCPValue(out, cfg, 0)
	if err != nil {
		return nil, err
	}
	clean, err := json.Marshal(out)
	if err != nil || len(clean) > 65536 {
		return nil, errors.New("sanitized MCP result exceeds safe output bound")
	}
	return out, nil
}
func BuildMCPTools(ctx context.Context, store secret.Store, scope runtimecontext.Scope, servers []MCPServerSpec, allowed []string) ([]coretool.Tool, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	var out []coretool.Tool
	for _, spec := range servers {
		var requested []string
		for _, name := range spec.Tools {
			if slices.Contains(allowed, MCPToolName(spec.Name, name)) {
				requested = append(requested, name)
			}
		}
		if len(requested) == 0 {
			continue
		}
		cfg, err := loadMCPCredential(ctx, store, scope.TenantID, spec.CredentialRef)
		if err != nil {
			return nil, err
		}
		for _, name := range requested {
			if !slices.Contains(cfg.AllowedTools, name) {
				return nil, secret.ErrForbidden
			}
		}
		fetchCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutSeconds)*time.Second)
		client, closeClient, err := openMCP(fetchCtx, cfg, "")
		if err != nil {
			cancel()
			return nil, err
		}
		remote, err := listMCPTools(fetchCtx, client)
		closeClient()
		cancel()
		if err != nil {
			return nil, err
		}
		for _, name := range requested {
			tool, ok := remote[name]
			if !ok {
				return nil, errors.New("configured MCP tool not advertised")
			}
			raw := tool.RawInputSchema
			if len(raw) == 0 {
				raw, _ = json.Marshal(tool.InputSchema)
			}
			var schema coretool.Schema
			if len(raw) > 65536 || json.Unmarshal(raw, &schema) != nil {
				return nil, errors.New("unsupported MCP tool schema")
			}
			description := scrubMCP(tool.Description, cfg)
			if len(description) > 4096 {
				description = description[:4096]
			}
			out = append(out, &remoteMCPTool{declaration: &coretool.Declaration{Name: MCPToolName(spec.Name, name), Description: description, InputSchema: &schema}, scope: scope, spec: spec, name: name, endpointHash: mcpHash([]byte(cfg.URL)), schemaHash: mcpSchemaHash(tool), readOnly: slices.Contains(cfg.ReadOnlyTools, name), store: store})
		}
	}
	return out, nil
}
func mcpHash(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }
func mcpSchemaHash(t mcp.Tool) string {
	raw := t.RawInputSchema
	if len(raw) == 0 {
		raw, _ = json.Marshal(t.InputSchema)
	}
	var normalized any
	_ = json.Unmarshal(raw, &normalized)
	b, _ := json.Marshal(normalized)
	return mcpHash(b)
}
func listMCPTools(ctx context.Context, client *mcp.Client) (map[string]mcp.Tool, error) {
	out := map[string]mcp.Tool{}
	cursor := mcp.Cursor("")
	seen := map[mcp.Cursor]bool{}
	for page := 0; page < 20; page++ {
		req := &mcp.ListToolsRequest{}
		req.Params.Cursor = cursor
		res, err := client.ListTools(ctx, req)
		if err != nil || res == nil {
			return nil, errors.New("MCP tool discovery failed")
		}
		for _, t := range res.Tools {
			if _, ok := out[t.Name]; ok {
				return nil, errors.New("duplicate MCP tool declaration")
			}
			out[t.Name] = t
		}
		if len(out) > 1000 {
			return nil, errors.New("MCP discovery limit exceeded")
		}
		if res.NextCursor == "" {
			return out, nil
		}
		if seen[res.NextCursor] {
			return nil, errors.New("MCP discovery cursor repeated")
		}
		seen[res.NextCursor] = true
		cursor = res.NextCursor
	}
	return nil, errors.New("MCP discovery page limit exceeded")
}
func openMCP(ctx context.Context, cfg MCPCredential, call string) (*mcp.Client, func(), error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = time.Duration(cfg.TimeoutSeconds) * time.Second
	h := &mcpHTTP{cfg: cfg, allowedCall: call, client: &http.Client{Transport: transport, Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("MCP redirects disabled") }}}
	u, _ := url.Parse(cfg.URL)
	client, err := mcp.NewClient(cfg.URL, mcp.Implementation{Name: "trpc-agent-service-tools", Version: "1"}, mcp.WithClientPath(u.Path), mcp.WithClientGetSSEEnabled(false), mcp.WithClientLogger(mcpSilentLogger{}), mcp.WithHTTPReqHandler(h))
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, errors.New("MCP client construction failed")
	}
	closeClient := func() { _ = client.Close(); transport.CloseIdleConnections() }
	if _, err = client.Initialize(ctx, &mcp.InitializeRequest{}); err != nil {
		closeClient()
		return nil, nil, errors.New("MCP initialization failed")
	}
	return client, closeClient, nil
}
func scrubMCP(value string, cfg MCPCredential) string {
	for _, s := range []string{cfg.BearerToken, cfg.URL} {
		if s != "" {
			value = strings.ReplaceAll(value, s, "[REDACTED]")
			encoded, _ := json.Marshal(s)
			value = strings.ReplaceAll(value, string(encoded[1:len(encoded)-1]), "[REDACTED]")
		}
	}
	return platformlog.Redact(value)
}

// Scrub decoded values, not serialized JSON: regex replacement across escaped
// quotes/newlines can consume delimiters and corrupt a valid MCP result. Text
// blocks can themselves contain JSON, so preserve that inner structure too.
func scrubMCPValue(value any, cfg MCPCredential, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("MCP result nesting exceeds safe bound")
	}
	switch typed := value.(type) {
	case string:
		trimmed := strings.TrimSpace(typed)
		if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[' || trimmed[0] == '"') && json.Valid([]byte(trimmed)) {
			var nested any
			d := json.NewDecoder(strings.NewReader(trimmed))
			d.UseNumber()
			if err := d.Decode(&nested); err != nil {
				return nil, errors.New("invalid nested MCP JSON")
			}
			clean, err := scrubMCPValue(nested, cfg, depth+1)
			if err != nil {
				return nil, err
			}
			raw, err := json.Marshal(clean)
			if err != nil {
				return nil, errors.New("cannot encode sanitized MCP text")
			}
			return string(raw), nil
		}
		return scrubMCP(typed, cfg), nil
	case []any:
		out := make([]any, len(typed))
		for n, item := range typed {
			clean, err := scrubMCPValue(item, cfg, depth+1)
			if err != nil {
				return nil, err
			}
			out[n] = clean
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			cleanKey := scrubMCP(key, cfg)
			if _, exists := out[cleanKey]; exists {
				return nil, errors.New("MCP field names collide after redaction")
			}
			lower := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "_", ""), "-", ""))
			switch lower {
			case "authorization", "password", "secret", "token", "apikey", "accesstoken", "refreshtoken", "bearertoken", "clientsecret", "secretaccesskey", "credentials", "dsn":
				out[cleanKey] = "[REDACTED]"
				continue
			}
			clean, err := scrubMCPValue(item, cfg, depth+1)
			if err != nil {
				return nil, err
			}
			out[cleanKey] = clean
		}
		return out, nil
	default:
		return value, nil
	}
}

type mcpHTTP struct {
	cfg         MCPCredential
	client      *http.Client
	allowedCall string
	called      atomic.Bool
}

func (h *mcpHTTP) Handle(ctx context.Context, _ *http.Client, req *http.Request) (*http.Response, error) {
	if req.URL.String() != h.cfg.URL {
		return nil, errors.New("MCP destination rejected")
	}
	if req.Method != http.MethodPost && req.Method != http.MethodDelete {
		return nil, errors.New("MCP HTTP method rejected")
	}
	if req.Method == http.MethodPost {
		b, err := io.ReadAll(io.LimitReader(req.Body, 131073))
		_ = req.Body.Close()
		if err != nil || len(b) > 131072 {
			return nil, errors.New("MCP request limit exceeded")
		}
		var e struct {
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		if json.Unmarshal(b, &e) != nil {
			return nil, errors.New("invalid MCP request")
		}
		switch e.Method {
		case "initialize", "notifications/initialized", "tools/list":
		case "tools/call":
			if h.allowedCall == "" || e.Params.Name != h.allowedCall || !h.called.CompareAndSwap(false, true) {
				return nil, secret.ErrForbidden
			}
		default:
			return nil, secret.ErrForbidden
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	request := req.Clone(ctx)
	request.Header.Del("baggage")
	// Continue the Tool span over the real HTTP MCP hop. Propagation includes
	// trace context only, not credentials or tool arguments.
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(request.Header))
	if h.cfg.BearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+h.cfg.BearerToken)
	}
	res, err := h.client.Do(request)
	if err != nil {
		return nil, errors.New("MCP HTTP request failed")
	}
	res.Body = &boundedMCPBody{ReadCloser: res.Body, left: 2 << 20}
	return res, nil
}

type boundedMCPBody struct {
	io.ReadCloser
	left int
}

func (b *boundedMCPBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, errors.New("MCP response limit exceeded")
	}
	if len(p) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= n
	return n, err
}

type mcpSilentLogger struct{}

func (mcpSilentLogger) Debug(...interface{})          {}
func (mcpSilentLogger) Debugf(string, ...interface{}) {}
func (mcpSilentLogger) Info(...interface{})           {}
func (mcpSilentLogger) Infof(string, ...interface{})  {}
func (mcpSilentLogger) Warn(...interface{})           {}
func (mcpSilentLogger) Warnf(string, ...interface{})  {}
func (mcpSilentLogger) Error(...interface{})          {}
func (mcpSilentLogger) Errorf(string, ...interface{}) {}
func (mcpSilentLogger) Fatal(...interface{})          {}
func (mcpSilentLogger) Fatalf(string, ...interface{}) {}
