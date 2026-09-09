package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
	toolmcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
	tmcp "trpc.group/trpc-go/trpc-mcp-go"
)

const defaultRemoteTimeout = 10 * time.Second

// SecretResolver resolves an immutable SecretRef without exposing its value.
type SecretResolver func(tenant.SecretRef) (string, error)
type ScopedSecretResolver func(context.Context, string, string, tenant.SecretRef) (string, error)

type mcpToolSet interface {
	Init(context.Context) error
	Tools(context.Context) []trpctool.Tool
	Close() error
}

type mcpFactory func(tenant.MCPServer, map[string]string) mcpToolSet

// CatalogRegistry builds version-pinned external tools for one Runtime Bundle.
// Models cannot add servers or endpoints; all targets originate in a validated
// published AgentApp configuration.
type CatalogRegistry struct {
	resolve       SecretResolver
	resolveScoped ScopedSecretResolver
	executions    ExecutionStore
	mcpFactory    mcpFactory
	httpClient    *http.Client
}

// SetScopedResolver enables tenant-scoped credential resolution for production
// bundles while retaining the simple resolver used by unit tests.
func (registry *CatalogRegistry) SetScopedResolver(resolve ScopedSecretResolver) {
	if registry != nil {
		registry.resolveScoped = resolve
	}
}

func (registry *CatalogRegistry) SetExecutionStore(store ExecutionStore) {
	if registry != nil {
		registry.executions = store
	}
}

// Catalog owns the external resources and model-visible tools for one Bundle.
type Catalog struct {
	tools   map[string]trpctool.Tool
	closers []mcpToolSet
}

// NewCatalogRegistry constructs the production tool registry.
func NewCatalogRegistry(resolve SecretResolver) (*CatalogRegistry, error) {
	if resolve == nil {
		return nil, errors.New("tool catalog: secret resolver is required")
	}
	registry := &CatalogRegistry{resolve: resolve, httpClient: defaultBusinessHTTPClient()}
	registry.mcpFactory = func(server tenant.MCPServer, headers map[string]string) mcpToolSet {
		timeout := configuredTimeout(server.TimeoutSeconds)
		return toolmcp.NewMCPToolSet(toolmcp.ConnectionConfig{
			Transport: "streamable", ServerURL: server.Endpoint, Headers: headers,
			Timeout: timeout, Description: "tenant-published MCP server",
		}, toolmcp.WithName(server.ID), toolmcp.WithToolFilterFunc(trpctool.NewIncludeToolNamesFilter(server.AllowedTools...)),
			toolmcp.WithMCPOptions(tmcp.WithClientGetSSEEnabled(false)), toolmcp.WithSessionReconnect(3))
	}
	return registry, nil
}

// Build resolves credentials, initializes named MCP sessions, verifies the
// configured discovery surface, and constructs fixed HTTPS business tools.
func (registry *CatalogRegistry) Build(ctx context.Context, app tenant.AgentApp) (*Catalog, error) {
	return registry.build(ctx, "", "", app)
}

func (registry *CatalogRegistry) BuildForScope(ctx context.Context, tenantID, appID string, app tenant.AgentApp) (*Catalog, error) {
	return registry.build(ctx, tenantID, appID, app)
}

func (registry *CatalogRegistry) build(ctx context.Context, tenantID, appID string, app tenant.AgentApp) (*Catalog, error) {
	if registry == nil || registry.resolve == nil || registry.mcpFactory == nil || registry.httpClient == nil || ctx == nil {
		return nil, errors.New("tool catalog: registry and context are required")
	}
	catalog := &Catalog{tools: make(map[string]trpctool.Tool)}
	failed := true
	defer func() {
		if failed {
			_ = catalog.Close()
		}
	}()
	for _, server := range app.MCPServers {
		if !server.Enabled {
			continue
		}
		headers, secretValues, err := registry.mcpHeadersForScope(ctx, tenantID, appID, server)
		if err != nil {
			return nil, fmt.Errorf("tool catalog: MCP server %q credential is unavailable", server.ID)
		}
		redactor := servicelog.NewRedactor(nil, secretValues)
		set := registry.mcpFactory(server, headers)
		if set == nil {
			return nil, fmt.Errorf("tool catalog: MCP server %q factory failed", server.ID)
		}
		catalog.closers = append(catalog.closers, set)
		serverCtx, cancel := context.WithTimeout(servicelog.WithRedactor(ctx, redactor), configuredTimeout(server.TimeoutSeconds))
		err = set.Init(serverCtx)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("tool catalog: MCP server %q initialization failed", server.ID)
		}
		discoveredTools := set.Tools(serverCtx)
		cancel()
		discovered := make(map[string]trpctool.Tool)
		for _, candidate := range discoveredTools {
			if candidate == nil || candidate.Declaration() == nil {
				return nil, fmt.Errorf("tool catalog: MCP server %q returned an invalid tool", server.ID)
			}
			discovered[candidate.Declaration().Name] = candidate
		}
		serverCallGate := make(chan struct{}, 1)
		for _, remoteName := range server.AllowedTools {
			candidate := discovered[remoteName]
			callable, ok := candidate.(trpctool.CallableTool)
			if !ok {
				return nil, fmt.Errorf("tool catalog: MCP server %q is missing a published tool", server.ID)
			}
			exposed := "mcp__" + server.ID + "__" + remoteName
			if _, exists := catalog.tools[exposed]; exists {
				return nil, fmt.Errorf("tool catalog: duplicate exposed tool %q", exposed)
			}
			catalog.tools[exposed] = registry.wrapExecution(exposed, &safeRemoteTool{delegate: callable, declaration: renamedDeclaration(candidate.Declaration(), exposed), redactor: redactor, callGate: serverCallGate})
		}
	}
	for _, configured := range app.BusinessTools {
		if !configured.Enabled {
			continue
		}
		credential, err := registry.resolveRef(ctx, tenantID, appID, configured.Credential)
		if err != nil {
			return nil, fmt.Errorf("tool catalog: business tool %q credential is unavailable", configured.Name)
		}
		if _, exists := catalog.tools[configured.Name]; exists {
			return nil, fmt.Errorf("tool catalog: duplicate exposed tool %q", configured.Name)
		}
		catalog.tools[configured.Name] = registry.wrapExecution(configured.Name, &HTTPJSONTool{config: configured, credential: credential, client: registry.httpClient, redactor: servicelog.NewRedactor(nil, []string{credential})})
	}
	failed = false
	return catalog, nil
}

func (registry *CatalogRegistry) wrapExecution(name string, delegate trpctool.Tool) trpctool.Tool {
	if registry == nil || registry.executions == nil {
		return delegate
	}
	return &executionLedgerTool{name: name, delegate: delegate, store: registry.executions}
}

type executionLedgerTool struct {
	name     string
	delegate trpctool.Tool
	store    ExecutionStore
}

func (tool *executionLedgerTool) Declaration() *trpctool.Declaration {
	if tool == nil || tool.delegate == nil {
		return nil
	}
	return tool.delegate.Declaration()
}

func (tool *executionLedgerTool) ToolMetadata() trpctool.ToolMetadata {
	if tool == nil || tool.delegate == nil {
		return trpctool.ToolMetadata{}
	}
	return trpctool.MetadataOf(tool.delegate)
}

func (tool *executionLedgerTool) Call(ctx context.Context, args []byte) (any, error) {
	if tool == nil || tool.delegate == nil || tool.store == nil {
		return nil, errors.New("tool: execution ledger is unavailable")
	}
	request, ok := policy.FromContext(ctx)
	if !ok || request.Request.RequestID == "" || request.Request.TenantID == "" {
		return nil, policy.ErrToolDenied
	}
	callID, ok := trpctool.ToolCallIDFromContext(ctx)
	if !ok || strings.TrimSpace(callID) == "" {
		return nil, errors.New("tool: framework tool call id is unavailable")
	}
	requestCtx := ctx
	record := ExecutionRecord{TenantID: request.Request.TenantID, RequestID: request.Request.RequestID, ToolCallID: callID, ToolName: tool.name, ArgumentsHash: hashBytes(args), IdempotencyKey: request.Request.RequestID + ":" + callID, TraceID: "", Status: ExecutionRunning}
	decision, err := tool.store.Begin(requestCtx, record)
	if err != nil {
		return nil, errors.New("tool: execution ledger unavailable")
	}
	if decision.Status == ExecutionCompleted {
		return decision.Result, nil
	}
	if !decision.Created && (decision.Status == ExecutionRunning || decision.Status == ExecutionOutcomeUnknown) {
		return nil, errors.New("tool: execution outcome requires reconciliation")
	}
	callable, ok := tool.delegate.(trpctool.CallableTool)
	if !ok {
		_ = tool.store.Fail(context.Background(), record.TenantID, record.RequestID, record.ToolCallID, "tool_not_callable", ExecutionFailed)
		return nil, errors.New("tool: execution target is not callable")
	}
	result, err := callable.Call(requestCtx, args)
	if err != nil {
		status := ExecutionFailed
		if requestCtx.Err() != nil {
			status = ExecutionOutcomeUnknown
		}
		_ = tool.store.Fail(context.Background(), record.TenantID, record.RequestID, record.ToolCallID, "tool_call_failed", status)
		return nil, err
	}
	if completeErr := tool.store.Complete(context.Background(), record.TenantID, record.RequestID, record.ToolCallID, result); completeErr != nil {
		return result, errors.New("tool: execution ledger completion failed")
	}
	return result, nil
}

func (registry *CatalogRegistry) resolveRef(ctx context.Context, tenantID, appID string, ref tenant.SecretRef) (string, error) {
	if registry.resolveScoped != nil && tenantID != "" && appID != "" {
		return registry.resolveScoped(ctx, tenantID, appID, ref)
	}
	return registry.resolve(ref)
}

// Preflight builds and closes an App catalog, including MCP Initialize/ListTools.
func (registry *CatalogRegistry) Preflight(ctx context.Context, app tenant.AgentApp) error {
	catalog, err := registry.Build(ctx, app)
	if err != nil {
		return err
	}
	return catalog.Close()
}

func (registry *CatalogRegistry) PreflightForScope(ctx context.Context, tenantID, appID string, app tenant.AgentApp) error {
	catalog, err := registry.BuildForScope(ctx, tenantID, appID, app)
	if err != nil {
		return err
	}
	return catalog.Close()
}

// Tools returns a defensive map copy owned by the caller's Bundle.
func (catalog *Catalog) Tools() map[string]trpctool.Tool {
	if catalog == nil {
		return nil
	}
	result := make(map[string]trpctool.Tool, len(catalog.tools))
	for name, candidate := range catalog.tools {
		result[name] = candidate
	}
	return result
}

// Close releases every MCP session. Errors are intentionally generic so a
// remote server cannot place credentials or internal topology in shutdown logs.
func (catalog *Catalog) Close() error {
	if catalog == nil {
		return nil
	}
	var closeErr error
	for index := len(catalog.closers) - 1; index >= 0; index-- {
		if err := catalog.closers[index].Close(); err != nil {
			closeErr = errors.Join(closeErr, errors.New("tool catalog: MCP session close failed"))
		}
	}
	catalog.closers = nil
	return closeErr
}

func (registry *CatalogRegistry) mcpHeaders(server tenant.MCPServer) (map[string]string, []string, error) {
	return registry.mcpHeadersForScope(context.Background(), "", "", server)
}

func (registry *CatalogRegistry) mcpHeadersForScope(ctx context.Context, tenantID, appID string, server tenant.MCPServer) (map[string]string, []string, error) {
	if server.Credential.IsZero() {
		return nil, nil, nil
	}
	credential, err := registry.resolveRef(ctx, tenantID, appID, server.Credential)
	if err != nil {
		return nil, nil, err
	}
	header := strings.TrimSpace(server.CredentialHeader)
	if header == "" {
		header = "Authorization"
	}
	value := credential
	if strings.EqualFold(header, "Authorization") {
		scheme := strings.TrimSpace(server.CredentialScheme)
		if scheme == "" {
			scheme = "Bearer"
		}
		value = scheme + " " + credential
	}
	return map[string]string{header: value}, []string{credential, value}, nil
}

func configuredTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		return defaultRemoteTimeout
	}
	return time.Duration(seconds) * time.Second
}

func renamedDeclaration(original *trpctool.Declaration, name string) *trpctool.Declaration {
	return &trpctool.Declaration{Name: name, Description: original.Description, InputSchema: original.InputSchema, OutputSchema: original.OutputSchema}
}

type safeRemoteTool struct {
	delegate    trpctool.CallableTool
	declaration *trpctool.Declaration
	redactor    *servicelog.Redactor
	callGate    chan struct{}
}

func (tool *safeRemoteTool) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool *safeRemoteTool) ToolMetadata() trpctool.ToolMetadata {
	if tool == nil {
		return trpctool.ToolMetadata{}
	}
	metadata := trpctool.MetadataOf(tool.delegate)
	metadata.OpenWorld = true
	return metadata
}
func (tool *safeRemoteTool) Call(ctx context.Context, args []byte) (any, error) {
	if tool == nil || tool.delegate == nil || tool.redactor == nil || tool.callGate == nil || ctx == nil {
		return nil, errors.New("tool: remote MCP tool is unavailable")
	}
	requestPolicy, ok := policy.FromContext(ctx)
	if !ok || requestPolicy.Request.RequestID == "" {
		return nil, policy.ErrToolDenied
	}
	select {
	case tool.callGate <- struct{}{}:
		defer func() { <-tool.callGate }()
	case <-ctx.Done():
		return nil, errors.New("tool: remote MCP call canceled")
	}
	result, err := tool.delegate.Call(servicelog.WithRedactor(ctx, tool.redactor), args)
	if err != nil {
		return nil, errors.New("tool: remote MCP call failed")
	}
	sanitized, err := sanitizeRemoteValue(tool.redactor, result)
	if err != nil {
		return nil, errors.New("tool: remote MCP result is invalid")
	}
	wrapped := &safeRemoteResult{value: sanitized}
	compatible := false
	if provider, ok := result.(interface{ GetCallbackResult() any }); ok {
		compatible = true
		wrapped.callback, err = sanitizeRemoteValue(tool.redactor, provider.GetCallbackResult())
		if err != nil {
			return nil, errors.New("tool: remote MCP result is invalid")
		}
	} else {
		wrapped.callback = sanitized
	}
	if provider, ok := result.(interface{ GetMeta() map[string]any }); ok {
		compatible = true
		if redacted := tool.redactor.RedactValue(provider.GetMeta()); redacted != nil {
			wrapped.meta, _ = redacted.(map[string]any)
		}
	}
	if provider, ok := result.(interface{ RetryResultError() bool }); ok {
		compatible = true
		wrapped.retryError = provider.RetryResultError()
	}
	if compatible {
		return wrapped, nil
	}
	return sanitized, nil
}

func sanitizeRemoteValue(redactor *servicelog.Redactor, value any) (any, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var structured any
	if err := json.Unmarshal(payload, &structured); err != nil {
		return nil, err
	}
	return redactor.RedactValue(structured), nil
}

var _ trpctool.CallableTool = (*safeRemoteTool)(nil)
var _ trpctool.MetadataProvider = (*safeRemoteTool)(nil)

type safeRemoteResult struct {
	value      any
	callback   any
	meta       map[string]any
	retryError bool
}

func (result *safeRemoteResult) MarshalJSON() ([]byte, error) { return json.Marshal(result.value) }
func (result *safeRemoteResult) GetCallbackResult() any       { return result.callback }
func (result *safeRemoteResult) GetMeta() map[string]any      { return result.meta }
func (result *safeRemoteResult) RetryResultError() bool       { return result.retryError }
