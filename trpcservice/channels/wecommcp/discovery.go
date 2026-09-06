// Package wecommcp connects to WeCom's hosted MCP message service. Discovery
// deliberately exposes no business-tool invocation or message subscription.
package wecommcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

const endpointPath = "/mcp/v2/bot/msg"

func ValidateEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "qyapi.weixin.qq.com" || u.Path != endpointPath || u.RawPath != "" || u.User != nil || u.Fragment != "" {
		return errors.New("WECOM_MCP_URL must be the official HTTPS WeCom message MCP URL")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(q) != 1 || len(q["apikey"]) != 1 || strings.TrimSpace(q.Get("apikey")) == "" {
		return errors.New("WECOM_MCP_URL requires exactly one nonempty apikey parameter")
	}
	return nil
}

type ToolDescription struct {
	Name         string               `json:"name"`
	Description  string               `json:"description,omitempty"`
	InputSchema  json.RawMessage      `json:"input_schema"`
	OutputSchema json.RawMessage      `json:"output_schema,omitempty"`
	Annotations  *mcp.ToolAnnotations `json:"annotations,omitempty"`
}
type Discovery struct {
	ProtocolVersion string                 `json:"protocol_version"`
	ServerName      string                 `json:"server_name"`
	Instructions    string                 `json:"instructions,omitempty"`
	Capabilities    mcp.ServerCapabilities `json:"capabilities"`
	Tools           []ToolDescription      `json:"tools"`
	Methods         map[string]int         `json:"methods_sent"`
}

// Discover allows only initialize, notifications/initialized, and tools/list.
// It uses the same tRPC MCP SDK used by tRPC-Agent-Go's tool/mcp ToolSet, while
// preserving raw schemas needed to design a Channel Adapter (not model tools).
func Discover(ctx context.Context, endpoint string) (Discovery, error) {
	if err := ValidateEndpoint(endpoint); err != nil {
		return Discovery{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transport := (&http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second, IdleConnTimeout: 30 * time.Second}).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("MCP redirects are disabled") }}
	return discover(ctx, endpoint, client)
}

func discover(ctx context.Context, endpoint string, httpClient *http.Client) (Discovery, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return Discovery{}, errors.New("invalid MCP endpoint")
	}
	if httpClient == nil {
		return Discovery{}, errors.New("MCP HTTP client is required")
	}
	guardClient := *httpClient
	guardClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("MCP redirects are disabled") }
	guard := &discoveryHTTP{client: &guardClient, endpoint: endpoint, methods: map[string]int{}}
	client, err := mcp.NewClient(endpoint, mcp.Implementation{Name: "trpc-agent-service-discovery", Version: "0.1.0"},
		mcp.WithClientPath(u.Path), mcp.WithClientLogger(silentLogger{}), mcp.WithClientGetSSEEnabled(false), mcp.WithHTTPReqHandler(guard))
	if err != nil {
		return Discovery{}, errors.New("create MCP client failed (details omitted)")
	}
	defer func() { _ = client.Close() }()
	init, err := client.Initialize(ctx, &mcp.InitializeRequest{})
	if err != nil {
		return Discovery{}, guard.safeError("initialize", err)
	}
	if init == nil {
		return Discovery{}, errors.New("MCP returned no initialization result")
	}
	result := Discovery{ProtocolVersion: init.ProtocolVersion, ServerName: init.ServerInfo.Name, Instructions: init.Instructions, Capabilities: init.Capabilities, Tools: []ToolDescription{}}
	cursor := mcp.Cursor("")
	seen := map[mcp.Cursor]bool{}
	for page := 0; page < 20; page++ {
		request := &mcp.ListToolsRequest{}
		request.Params.Cursor = cursor
		listed, err := client.ListTools(ctx, request)
		if err != nil {
			return Discovery{}, guard.safeError("tools/list", err)
		}
		if listed == nil {
			return Discovery{}, errors.New("MCP returned no tool list")
		}
		for _, tool := range listed.Tools {
			input := tool.RawInputSchema
			if len(input) == 0 {
				input, _ = json.Marshal(tool.InputSchema)
			}
			output := tool.RawOutputSchema
			if len(output) == 0 && tool.OutputSchema != nil {
				output, _ = json.Marshal(tool.OutputSchema)
			}
			result.Tools = append(result.Tools, ToolDescription{Name: tool.Name, Description: tool.Description, InputSchema: input, OutputSchema: output, Annotations: tool.Annotations})
		}
		if len(result.Tools) > 200 {
			return Discovery{}, errors.New("MCP tool discovery limit exceeded")
		}
		cursor = listed.NextCursor
		if cursor == "" {
			guard.mu.Lock()
			result.Methods = maps.Clone(guard.methods)
			guard.mu.Unlock()
			// Scrub even successful metadata if a provider echoes its credential.
			data, err := json.Marshal(result)
			if err != nil {
				return Discovery{}, errors.New("MCP metadata encoding failed")
			}
			data = []byte(scrub(string(data), endpoint))
			if json.Unmarshal(data, &result) != nil {
				return Discovery{}, errors.New("MCP metadata sanitization failed")
			}
			return result, nil
		}
		if seen[cursor] {
			return Discovery{}, errors.New("MCP tool pagination cursor repeated")
		}
		seen[cursor] = true
	}
	return Discovery{}, errors.New("MCP tool pagination limit exceeded")
}

type discoveryHTTP struct {
	client     *http.Client
	endpoint   string
	mu         sync.Mutex
	methods    map[string]int
	lastStatus int
	// Nil for discovery. Sampling grants an exact tool + argument set and a
	// one-call budget; no generic tools/call permission is exposed to callers.
	allowedCalls map[string]map[string]any
	callBudget   map[string]int
}

func (g *discoveryHTTP) Handle(ctx context.Context, _ *http.Client, req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.URL.String() != g.endpoint {
		return nil, errors.New("non-discovery HTTP request blocked")
	}
	if req.Body == nil {
		return nil, errors.New("empty MCP discovery request")
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, (64<<10)+1))
	_ = req.Body.Close()
	if err != nil {
		return nil, errors.New("read MCP discovery request failed")
	}
	if len(data) > 64<<10 {
		return nil, errors.New("MCP discovery request too large")
	}
	var envelope struct {
		Method string `json:"method"`
		Params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		} `json:"params"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return nil, errors.New("invalid MCP discovery request")
	}
	switch envelope.Method {
	case "initialize", "notifications/initialized", "tools/list":
	case "tools/call":
		g.mu.Lock()
		expected, ok := g.allowedCalls[envelope.Params.Name]
		actualJSON, _ := json.Marshal(envelope.Params.Arguments)
		expectedJSON, _ := json.Marshal(expected)
		allowed := ok && bytes.Equal(actualJSON, expectedJSON) && g.callBudget[envelope.Params.Name] > 0
		if allowed {
			g.callBudget[envelope.Params.Name]--
		}
		g.mu.Unlock()
		if !allowed {
			return nil, errors.New("unapproved MCP sampling tool or arguments blocked")
		}
	default:
		return nil, errors.New("business MCP method blocked during discovery")
	}
	req = req.Clone(ctx)
	req.Body = io.NopCloser(bytes.NewReader(data))
	g.mu.Lock()
	methodName := envelope.Method
	if envelope.Method == "tools/call" {
		methodName += "/" + envelope.Params.Name
	}
	g.methods[methodName]++
	g.mu.Unlock()
	response, err := g.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, errors.New("MCP discovery HTTP request failed (details omitted)")
	}
	g.mu.Lock()
	g.lastStatus = response.StatusCode
	g.mu.Unlock()
	response.Body = &limitedBody{Reader: io.LimitReader(response.Body, 2<<20), Closer: response.Body}
	return response, nil
}
func (g *discoveryHTTP) safeError(stage string, err error) error {
	g.mu.Lock()
	status := g.lastStatus
	g.mu.Unlock()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("MCP %s cancelled or timed out", stage)
	}
	return fmt.Errorf("MCP %s failed (last HTTP status=%d; sensitive details omitted)", stage, status)
}

type limitedBody struct {
	io.Reader
	io.Closer
}

func scrub(text, endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "[invalid metadata]"
	}
	key := u.Query().Get("apikey")
	encodedURL, _ := json.Marshal(endpoint)
	if len(encodedURL) > 2 {
		text = strings.ReplaceAll(text, string(encodedURL[1:len(encodedURL)-1]), "[REDACTED MCP URL]")
	}
	text = strings.ReplaceAll(text, endpoint, "[REDACTED MCP URL]")
	if key != "" {
		encodedKey, _ := json.Marshal(key)
		if len(encodedKey) > 2 {
			text = strings.ReplaceAll(text, string(encodedKey[1:len(encodedKey)-1]), "[REDACTED]")
		}
		text = strings.ReplaceAll(text, key, "[REDACTED]")
		text = strings.ReplaceAll(text, url.QueryEscape(key), "[REDACTED]")
	}
	return text
}

type silentLogger struct{}

func (silentLogger) Debug(...interface{})          {}
func (silentLogger) Debugf(string, ...interface{}) {}
func (silentLogger) Info(...interface{})           {}
func (silentLogger) Infof(string, ...interface{})  {}
func (silentLogger) Warn(...interface{})           {}
func (silentLogger) Warnf(string, ...interface{})  {}
func (silentLogger) Error(...interface{})          {}
func (silentLogger) Errorf(string, ...interface{}) {}
func (silentLogger) Fatal(...interface{})          {}
func (silentLogger) Fatalf(string, ...interface{}) {}
