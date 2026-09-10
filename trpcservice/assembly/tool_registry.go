package assembly

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/netpolicy"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/duckduckgo"
)

const defaultRemoteToolTimeout = 30 * time.Second

// ToolSurface is the complete immutable tool surface for one tenant config
// version. Static/function tools and native framework ToolSets share one seam.
type ToolSurface struct {
	Tools    []agenttool.Tool
	ToolSets []agenttool.ToolSet
}

// ToolRegistry owns the one platform tool registry and builds tenant-scoped
// HTTP/MCP tools from immutable configuration snapshots.
type ToolRegistry struct {
	registry   map[string]agenttool.CallableTool
	secrets    credential.SecretResolver
	httpClient *http.Client
	timeout    time.Duration
}

type ToolRegistryOption func(*ToolRegistry)

func WithToolSecretResolver(resolver credential.SecretResolver) ToolRegistryOption {
	return func(registry *ToolRegistry) { registry.secrets = resolver }
}

// WithToolHTTPClient is primarily a test seam. Production should use the
// default public-HTTPS client created by NewToolRegistry.
func WithToolHTTPClient(client *http.Client) ToolRegistryOption {
	return func(registry *ToolRegistry) {
		if client != nil {
			registry.httpClient = client
		}
	}
}

func WithToolTimeout(timeout time.Duration) ToolRegistryOption {
	return func(registry *ToolRegistry) {
		if timeout > 0 {
			registry.timeout = timeout
		}
	}
}

// ToolDescriptor is the user-facing metadata for one built-in selectable tool.
type ToolDescriptor struct {
	Name        string
	Description string
}

// NewToolRegistry constructs the single platform registry. DuckDuckGo is a
// framework-native built-in; callers may add additional platform-owned tools.
func NewToolRegistry(registry map[string]agenttool.CallableTool, opts ...ToolRegistryOption) (*ToolRegistry, error) {
	tools := &ToolRegistry{
		registry: make(map[string]agenttool.CallableTool, len(registry)+1),
		timeout:  defaultRemoteToolTimeout,
	}
	search := duckduckgo.NewTool()
	tools.registry[search.Declaration().Name] = search
	for name, registered := range registry {
		if registered == nil || registered.Declaration() == nil || registered.Declaration().Name == "" {
			return nil, fmt.Errorf("registered tool %q has no valid declaration", name)
		}
		tools.registry[registered.Declaration().Name] = registered
	}
	for _, opt := range opts {
		opt(tools)
	}
	if tools.httpClient == nil {
		tools.httpClient = netpolicy.NewPublicHTTPSClient(tools.timeout)
	}
	return tools, nil
}

func (r *ToolRegistry) Names() []string {
	if r == nil {
		return []string{}
	}
	names := make([]string, 0, len(r.registry))
	for name := range r.registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *ToolRegistry) Catalog() []ToolDescriptor {
	if r == nil {
		return []ToolDescriptor{}
	}
	names := r.Names()
	catalog := make([]ToolDescriptor, 0, len(names))
	for _, name := range names {
		declaration := r.registry[name].Declaration()
		catalog = append(catalog, ToolDescriptor{Name: name, Description: declaration.Description})
	}
	return catalog
}

// Surface resolves all tools enabled by one immutable tenant configuration.
// MCP remains a native framework ToolSet; HTTP endpoints become native
// Function Tools with the configured JSON Schema.
func (r *ToolRegistry) Surface(ctx context.Context, tenantConfig config.TenantConfig) (ToolSurface, error) {
	if r == nil {
		return ToolSurface{}, fmt.Errorf("tool registry is required")
	}
	allowed := make(map[string]struct{}, len(tenantConfig.Tools.Allowed))
	consumed := make(map[string]struct{}, len(tenantConfig.Tools.Allowed))
	for _, name := range tenantConfig.Tools.Allowed {
		allowed[name] = struct{}{}
	}
	surface := ToolSurface{}
	for _, name := range r.Names() {
		if _, ok := allowed[name]; ok {
			surface.Tools = append(surface.Tools, r.registry[name])
			consumed[name] = struct{}{}
		}
	}
	for _, cfg := range tenantConfig.Tools.HTTP {
		name := strings.TrimSpace(cfg.Name)
		if _, ok := allowed[name]; !ok {
			continue
		}
		if _, exists := consumed[name]; exists {
			return ToolSurface{}, fmt.Errorf("tenant %q exposes duplicate tool %q", tenantConfig.AppName(), name)
		}
		tool, err := r.newHTTPTool(cfg)
		if err != nil {
			return ToolSurface{}, fmt.Errorf("build HTTP tool %q: %w", name, err)
		}
		surface.Tools = append(surface.Tools, tool)
		consumed[name] = struct{}{}
	}
	for _, cfg := range tenantConfig.Tools.MCP {
		remoteNames := make([]string, 0)
		for name := range allowed {
			remoteName, matches := cfg.RemoteToolName(name)
			if !matches {
				continue
			}
			if _, exists := consumed[name]; exists {
				return ToolSurface{}, fmt.Errorf("tenant %q exposes duplicate tool %q", tenantConfig.AppName(), name)
			}
			remoteNames = append(remoteNames, remoteName)
			consumed[name] = struct{}{}
		}
		if len(remoteNames) == 0 {
			continue
		}
		sort.Strings(remoteNames)
		toolSet, err := r.newMCPToolSet(ctx, cfg, remoteNames)
		if err != nil {
			return ToolSurface{}, err
		}
		surface.ToolSets = append(surface.ToolSets, toolSet)
	}
	for name := range allowed {
		if _, ok := consumed[name]; !ok {
			return ToolSurface{}, fmt.Errorf("tenant %q allows unregistered tool %q", tenantConfig.AppName(), name)
		}
	}
	return surface, nil
}
