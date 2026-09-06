package wecommcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

// Runtime calls do not create sampling files or expose raw SDK/provider errors.
// A client can invoke exactly one preselected tool with exact arguments; no
// discovery, files, subscription, generic message_send or automatic retry.
func callRuntime(ctx context.Context, endpoint string, httpClient *http.Client, name string, args map[string]any) (json.RawMessage, bool, error) {
	if err := ValidateEndpoint(endpoint); err != nil {
		return nil, false, err
	}
	if name != messagesTool && name != replyTool {
		return nil, false, errors.New("unsupported WeCom MCP runtime tool")
	}
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	var transport *http.Transport
	if httpClient == nil {
		transport = probeTransport()
		defer transport.CloseIdleConnections()
		httpClient = &http.Client{Transport: transport, Timeout: 20 * time.Second}
	}
	copyClient := *httpClient
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("MCP redirects disabled") }
	guard := &discoveryHTTP{client: &copyClient, endpoint: endpoint, methods: map[string]int{}, allowedCalls: map[string]map[string]any{name: args}, callBudget: map[string]int{name: 1}}
	u, _ := url.Parse(endpoint)
	client, err := mcp.NewClient(endpoint, mcp.Implementation{Name: "trpc-agent-service-channel", Version: "0.1.0"}, mcp.WithClientPath(u.Path), mcp.WithClientLogger(silentLogger{}), mcp.WithClientGetSSEEnabled(false), mcp.WithHTTPReqHandler(guard))
	if err != nil {
		return nil, false, errors.New("cannot create WeCom MCP client")
	}
	defer func() { _ = client.Close() }()
	if _, err = client.Initialize(ctx, &mcp.InitializeRequest{}); err != nil {
		return nil, false, errors.New("WeCom MCP initialization failed")
	}
	request := &mcp.CallToolRequest{}
	request.Params.Name = name
	request.Params.Arguments = args
	result, err := client.CallTool(ctx, request)
	if err != nil || result == nil {
		return nil, false, errors.New("WeCom MCP tool result unavailable")
	}
	payload, err := samplePayload(result)
	if err != nil {
		return nil, false, errors.New("invalid WeCom MCP result")
	}
	return json.RawMessage(scrub(string(payload), endpoint)), result.IsError, nil
}
