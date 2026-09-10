package assembly

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/netpolicy"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	toolmcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
	tmcp "trpc.group/trpc-go/trpc-mcp-go"
)

func (r *ToolRegistry) newMCPToolSet(ctx context.Context, cfg config.MCPToolConfig, remoteNames []string) (agenttool.ToolSet, error) {
	if err := netpolicy.ValidatePublicHTTPSURL(cfg.URL); err != nil {
		return nil, fmt.Errorf("MCP server %q: %w", cfg.Name, err)
	}
	transport := strings.ToLower(strings.TrimSpace(cfg.Transport))
	if transport == "" {
		transport = "streamable"
	}
	options := []tmcp.ClientOption{tmcp.WithHTTPReqHandler(mcpHTTPHandler{client: r.httpClient})}
	if strings.TrimSpace(cfg.CredentialRef) != "" {
		options = append(options, tmcp.WithHTTPBeforeRequest(func(callCtx context.Context, request *http.Request) error {
			return r.applyBearerCredential(callCtx, request, cfg.CredentialRef)
		}))
	}
	toolSet := toolmcp.NewMCPToolSet(
		toolmcp.ConnectionConfig{Transport: transport, ServerURL: cfg.URL, Timeout: r.timeout, Description: cfg.Description},
		toolmcp.WithName(strings.TrimSpace(cfg.Name)),
		toolmcp.WithToolFilterFunc(agenttool.NewIncludeToolNamesFilter(remoteNames...)),
		toolmcp.WithSessionReconnect(3),
		toolmcp.WithMCPOptions(options...),
	)
	if err := toolSet.Init(ctx); err != nil {
		_ = toolSet.Close()
		return nil, fmt.Errorf("initialize MCP server %q: %w", cfg.Name, err)
	}
	return toolSet, nil
}

func (r *ToolRegistry) applyBearerCredential(ctx context.Context, request *http.Request, reference string) error {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return nil
	}
	if r.secrets == nil {
		return fmt.Errorf("resolve remote tool credential: secret resolver is not configured")
	}
	secret, err := r.secrets.Resolve(ctx, reference)
	if err != nil {
		return fmt.Errorf("resolve remote tool credential: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+secret)
	return nil
}

type mcpHTTPHandler struct{ client *http.Client }

func (h mcpHTTPHandler) Handle(ctx context.Context, _ *http.Client, request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil {
		return nil, fmt.Errorf("MCP request URL is required")
	}
	if err := netpolicy.ValidatePublicHTTPSURL(request.URL.String()); err != nil {
		return nil, err
	}
	return h.client.Do(request.WithContext(ctx))
}
