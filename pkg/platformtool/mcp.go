package platformtool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	frameworkmcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
)

var (
	// ErrMCPProfileConfig identifies an invalid operator-owned MCP catalog.
	ErrMCPProfileConfig = errors.New("MCP profile configuration is invalid")
	// ErrMCPProfileUnavailable deliberately hides transport URLs, credentials,
	// and provider error bodies from Worker construction errors.
	ErrMCPProfileUnavailable = errors.New("MCP profile is unavailable")
	// ErrMCPProfileClose identifies a best-effort MCP client cleanup failure.
	ErrMCPProfileClose = errors.New("MCP profile cleanup failed")
	// ErrMCPToolCall prevents an untrusted remote JSON-RPC error body from
	// crossing into model context, audit logs, or caller-visible responses.
	ErrMCPToolCall = errors.New("MCP tool call failed")
)

const (
	maxMCPProfiles        = 32
	maxMCPToolsPerProfile = 64
	maxMCPHeaders         = 16
	defaultMCPTimeout     = 10 * time.Second
	minMCPTimeout         = 100 * time.Millisecond
	maxMCPTimeout         = 2 * time.Minute
)

var (
	mcpProfileIDPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,15}$`)
	mcpToolNamePattern  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
	mcpHeaderPattern    = regexp.MustCompile(`^[!#$%&'*+.^_` + "`" + `|~0-9A-Za-z-]+$`)
)

// MCPProfile is public, operator-owned metadata. Credentials are references
// resolved only inside Worker; Admin can validate the exact exposed tool names
// without gaining access to an MCP bearer token.
type MCPProfile struct {
	ID            string            `json:"id"`
	Transport     string            `json:"transport"`
	ServerURL     string            `json:"serverUrl"`
	Timeout       string            `json:"timeout,omitempty"`
	Tools         []string          `json:"tools"`
	HeaderRefs    map[string]string `json:"headerRefs,omitempty"`
	AllowInsecure bool              `json:"allowInsecure,omitempty"`
}

type mcpToolReference struct {
	profileID string
	remote    string
}

type mcpRuntimeProfile struct {
	spec    MCPProfile
	timeout time.Duration
	mu      sync.Mutex
	set     *frameworkmcp.ToolSet
	tools   map[string]tool.Tool
}

// MCPResolver combines the fixed built-in catalog with exact, prefixed MCP
// tools. Runtime profiles connect lazily so one unavailable MCP server does not
// prevent unrelated tenants from being served.
type MCPResolver struct {
	catalog  *Catalog
	refs     map[string]mcpToolReference
	runtimes map[string]*mcpRuntimeProfile
	secrets  tenant.SecretResolver
	runtime  bool

	lifecycle sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

// NewMCPAdmissionResolver creates the Admin-side catalog. It parses the same
// operator profiles as Worker but neither resolves credentials nor opens a
// network connection.
func NewMCPAdmissionResolver(raw string) (*MCPResolver, error) {
	return newMCPResolver(raw, nil, false)
}

// NewMCPRuntimeResolver creates the Worker-side lazy resolver. A non-empty
// HeaderRefs entry requires a SecretResolver when that profile is first used.
func NewMCPRuntimeResolver(raw string, secrets tenant.SecretResolver) (*MCPResolver, error) {
	return newMCPResolver(raw, secrets, true)
}

func newMCPResolver(raw string, secrets tenant.SecretResolver, runtime bool) (*MCPResolver, error) {
	profiles, err := parseMCPProfiles(raw)
	if err != nil {
		return nil, err
	}
	resolver := &MCPResolver{
		catalog: NewBuiltinCatalog(), refs: make(map[string]mcpToolReference),
		secrets: secrets, runtime: runtime,
	}
	if runtime {
		resolver.runtimes = make(map[string]*mcpRuntimeProfile, len(profiles))
	}
	for _, profile := range profiles {
		timeout, _ := effectiveMCPTimeout(profile.Timeout)
		if runtime {
			resolver.runtimes[profile.ID] = &mcpRuntimeProfile{spec: profile, timeout: timeout}
		}
		for _, remote := range profile.Tools {
			name := MCPToolName(profile.ID, remote)
			if _, duplicate := resolver.refs[name]; duplicate {
				return nil, fmt.Errorf("%w: duplicate exposed tool %q", ErrMCPProfileConfig, name)
			}
			resolver.refs[name] = mcpToolReference{profileID: profile.ID, remote: remote}
			if err := resolver.catalog.Register(&mcpAdmissionTool{name: name}); err != nil {
				return nil, fmt.Errorf("%w: register exposed tool %q", ErrMCPProfileConfig, name)
			}
		}
	}
	return resolver, nil
}

// MCPToolName returns the stable name used by AgentVersion.Tools and the
// tenant ToolPolicy. Profile and remote names have already been validated.
func MCPToolName(profileID, remote string) string {
	return "mcp_" + profileID + "_" + remote
}

// Resolve retains compatibility with the platform ToolResolver contract.
func (r *MCPResolver) Resolve(names []string) ([]tool.Tool, error) {
	return r.ResolveContext(context.Background(), names)
}

// ResolveContext preserves Worker construction cancellation while lazily
// initializing only the MCP profiles referenced by this immutable AgentVersion.
func (r *MCPResolver) ResolveContext(ctx context.Context, names []string) ([]tool.Tool, error) {
	if r == nil || r.catalog == nil {
		return nil, fmt.Errorf("platform tool resolver is not configured")
	}
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if r.closed {
		return nil, ErrMCPProfileUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resolved := make([]tool.Tool, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("tool %q is configured more than once", name)
		}
		seen[name] = struct{}{}
		ref, isMCP := r.refs[name]
		if !isMCP || !r.runtime {
			values, err := r.catalog.Resolve([]string{name})
			if err != nil {
				return nil, err
			}
			resolved = append(resolved, values[0])
			continue
		}
		profile := r.runtimes[ref.profileID]
		if profile == nil {
			return nil, fmt.Errorf("%w: profile %q", ErrMCPProfileUnavailable, ref.profileID)
		}
		value, err := profile.resolve(ctx, ref.remote, r.secrets)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, value)
	}
	return resolved, nil
}

// Close releases lazily opened MCP sessions. Errors are intentionally opaque.
func (r *MCPResolver) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.lifecycle.Lock()
		defer r.lifecycle.Unlock()
		r.closed = true
		for _, profile := range r.runtimes {
			profile.mu.Lock()
			set := profile.set
			profile.set = nil
			profile.tools = nil
			profile.mu.Unlock()
			if set != nil {
				if err := set.Close(); err != nil {
					r.closeErr = ErrMCPProfileClose
				}
			}
		}
	})
	return r.closeErr
}

func (p *mcpRuntimeProfile) resolve(ctx context.Context, remote string, secrets tenant.SecretResolver) (tool.Tool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tools == nil {
		if err := p.initialize(ctx, secrets); err != nil {
			return nil, err
		}
	}
	value, ok := p.tools[remote]
	if !ok {
		return nil, fmt.Errorf("%w: profile %q did not advertise required tool", ErrMCPProfileUnavailable, p.spec.ID)
	}
	return value, nil
}

func (p *mcpRuntimeProfile) initialize(ctx context.Context, secrets tenant.SecretResolver) error {
	headers := make(map[string]string, len(p.spec.HeaderRefs))
	for name, rawRef := range p.spec.HeaderRefs {
		if secrets == nil {
			return fmt.Errorf("%w: profile %q credential resolver is absent", ErrMCPProfileUnavailable, p.spec.ID)
		}
		value, err := secrets.Resolve(ctx, tenant.SecretRef(rawRef))
		if err != nil || len(value) == 0 || strings.ContainsAny(string(value), "\r\n") {
			return fmt.Errorf("%w: profile %q credential cannot be resolved", ErrMCPProfileUnavailable, p.spec.ID)
		}
		headers[name] = string(value)
		for i := range value {
			value[i] = 0
		}
	}
	set := frameworkmcp.NewMCPToolSet(
		frameworkmcp.ConnectionConfig{
			Transport: p.spec.Transport, ServerURL: p.spec.ServerURL,
			Headers: headers, Timeout: p.timeout,
		},
		frameworkmcp.WithName(p.spec.ID),
		frameworkmcp.WithToolFilterFunc(tool.NewIncludeToolNamesFilter(p.spec.Tools...)),
		frameworkmcp.WithSessionReconnect(2),
	)
	if err := set.Init(ctx); err != nil {
		_ = set.Close()
		return fmt.Errorf("%w: profile %q initialization failed", ErrMCPProfileUnavailable, p.spec.ID)
	}
	discovered := set.Tools(ctx)
	tools := make(map[string]tool.Tool, len(discovered))
	for _, value := range discovered {
		declaration, ok := safeDeclaration(value)
		if !ok {
			_ = set.Close()
			return fmt.Errorf("%w: profile %q returned an invalid declaration", ErrMCPProfileUnavailable, p.spec.ID)
		}
		if !containsString(p.spec.Tools, declaration.Name) {
			continue
		}
		callable, ok := value.(tool.CallableTool)
		if !ok {
			_ = set.Close()
			return fmt.Errorf("%w: profile %q returned a non-callable tool", ErrMCPProfileUnavailable, p.spec.ID)
		}
		copy := *declaration
		copy.Name = MCPToolName(p.spec.ID, declaration.Name)
		tools[declaration.Name] = &mcpCallableTool{inner: callable, declaration: &copy}
	}
	for _, required := range p.spec.Tools {
		if _, ok := tools[required]; !ok {
			_ = set.Close()
			return fmt.Errorf("%w: profile %q did not advertise every configured tool", ErrMCPProfileUnavailable, p.spec.ID)
		}
	}
	p.set = set
	p.tools = tools
	return nil
}

type mcpAdmissionTool struct{ name string }

func (t *mcpAdmissionTool) Declaration() *tool.Declaration {
	return &tool.Declaration{Name: t.name, Description: "Operator-approved MCP capability"}
}

func (*mcpAdmissionTool) ToolMetadata() tool.ToolMetadata {
	return tool.ToolMetadata{OpenWorld: true}
}

type mcpCallableTool struct {
	inner       tool.CallableTool
	declaration *tool.Declaration
}

func (t *mcpCallableTool) Declaration() *tool.Declaration { return t.declaration }

func (t *mcpCallableTool) Call(ctx context.Context, arguments []byte) (any, error) {
	result, err := t.inner.Call(ctx, arguments)
	if err != nil {
		return nil, ErrMCPToolCall
	}
	return result, nil
}

func (t *mcpCallableTool) ToolMetadata() tool.ToolMetadata {
	metadata := tool.MetadataOf(t.inner)
	// Every MCP tool crosses a process/network trust boundary even if the
	// provider omitted annotations.
	metadata.OpenWorld = true
	return metadata
}

func parseMCPProfiles(raw string) ([]MCPProfile, error) {
	if strings.TrimSpace(raw) == "" {
		raw = "[]"
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var profiles []MCPProfile
	if err := decoder.Decode(&profiles); err != nil {
		return nil, fmt.Errorf("%w: decode catalog", ErrMCPProfileConfig)
	}
	if profiles == nil {
		return nil, fmt.Errorf("%w: catalog must be a JSON array", ErrMCPProfileConfig)
	}
	if err := rejectTrailingJSON(decoder); err != nil {
		return nil, err
	}
	if len(profiles) > maxMCPProfiles {
		return nil, fmt.Errorf("%w: at most %d profiles are allowed", ErrMCPProfileConfig, maxMCPProfiles)
	}
	seen := make(map[string]struct{}, len(profiles))
	for i := range profiles {
		if err := validateMCPProfile(&profiles[i]); err != nil {
			return nil, err
		}
		if _, duplicate := seen[profiles[i].ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate profile %q", ErrMCPProfileConfig, profiles[i].ID)
		}
		seen[profiles[i].ID] = struct{}{}
	}
	return profiles, nil
}

func rejectTrailingJSON(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON value", ErrMCPProfileConfig)
	}
	return nil
}

func validateMCPProfile(profile *MCPProfile) error {
	if profile == nil || !mcpProfileIDPattern.MatchString(profile.ID) {
		return fmt.Errorf("%w: profile id is invalid", ErrMCPProfileConfig)
	}
	switch profile.Transport {
	case "streamable_http":
		profile.Transport = "streamable"
	case "streamable", "sse":
	default:
		return fmt.Errorf("%w: profile %q transport must be streamable or sse", ErrMCPProfileConfig, profile.ID)
	}
	parsed, err := url.Parse(profile.ServerURL)
	if err != nil || parsed == nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
		return fmt.Errorf("%w: profile %q server URL is invalid", ErrMCPProfileConfig, profile.ID)
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && profile.AllowInsecure) {
		return fmt.Errorf("%w: profile %q requires HTTPS", ErrMCPProfileConfig, profile.ID)
	}
	if _, err := effectiveMCPTimeout(profile.Timeout); err != nil {
		return fmt.Errorf("%w: profile %q timeout is invalid", ErrMCPProfileConfig, profile.ID)
	}
	if len(profile.Tools) == 0 || len(profile.Tools) > maxMCPToolsPerProfile {
		return fmt.Errorf("%w: profile %q must expose 1..%d tools", ErrMCPProfileConfig, profile.ID, maxMCPToolsPerProfile)
	}
	seenTools := make(map[string]struct{}, len(profile.Tools))
	for _, remote := range profile.Tools {
		name := MCPToolName(profile.ID, remote)
		if !mcpToolNamePattern.MatchString(remote) || len(name) > 64 {
			return fmt.Errorf("%w: profile %q has an invalid tool name", ErrMCPProfileConfig, profile.ID)
		}
		if _, duplicate := seenTools[remote]; duplicate {
			return fmt.Errorf("%w: profile %q repeats a tool", ErrMCPProfileConfig, profile.ID)
		}
		seenTools[remote] = struct{}{}
	}
	if len(profile.HeaderRefs) > maxMCPHeaders {
		return fmt.Errorf("%w: profile %q has too many headers", ErrMCPProfileConfig, profile.ID)
	}
	canonicalHeaders := make(map[string]string, len(profile.HeaderRefs))
	for name, rawRef := range profile.HeaderRefs {
		canonical := http.CanonicalHeaderKey(name)
		if !mcpHeaderPattern.MatchString(name) || canonical == "" || forbiddenMCPHeader(canonical) {
			return fmt.Errorf("%w: profile %q has a forbidden header", ErrMCPProfileConfig, profile.ID)
		}
		if err := tenant.SecretRef(rawRef).Validate(); err != nil {
			return fmt.Errorf("%w: profile %q has an invalid header reference", ErrMCPProfileConfig, profile.ID)
		}
		if _, exists := canonicalHeaders[canonical]; exists {
			return fmt.Errorf("%w: profile %q repeats a header", ErrMCPProfileConfig, profile.ID)
		}
		canonicalHeaders[canonical] = rawRef
	}
	profile.HeaderRefs = canonicalHeaders
	return nil
}

func forbiddenMCPHeader(name string) bool {
	switch name {
	case "Host", "Content-Length", "Transfer-Encoding", "Connection", "Proxy-Connection", "Upgrade":
		return true
	default:
		return false
	}
}

func effectiveMCPTimeout(raw string) (time.Duration, error) {
	if raw == "" {
		return defaultMCPTimeout, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < minMCPTimeout || value > maxMCPTimeout {
		return 0, fmt.Errorf("timeout must be between %s and %s", minMCPTimeout, maxMCPTimeout)
	}
	return value, nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
