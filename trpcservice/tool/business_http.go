package tool

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	maxBusinessRequestBytes  = 64 << 10
	maxBusinessResponseBytes = 64 << 10
)

// HTTPJSONTool calls one immutable administrator-published HTTPS endpoint.
// The remote service must honor X-Idempotency-Key for side-effecting calls.
type HTTPJSONTool struct {
	config     tenant.HTTPBusinessTool
	credential string
	client     *http.Client
	redactor   *servicelog.Redactor
}

func (tool *HTTPJSONTool) Declaration() *trpctool.Declaration {
	if tool == nil {
		return nil
	}
	return &trpctool.Declaration{
		Name: tool.config.Name, Description: tool.config.Description,
		InputSchema:  &trpctool.Schema{Type: "object", AdditionalProperties: true},
		OutputSchema: &trpctool.Schema{Type: "object", AdditionalProperties: true},
	}
}

func (tool *HTTPJSONTool) Call(ctx context.Context, args []byte) (any, error) {
	if tool == nil || tool.client == nil || tool.redactor == nil || ctx == nil {
		return nil, errors.New("tool: business HTTP tool is unavailable")
	}
	requestPolicy, ok := policy.FromContext(ctx)
	if !ok || requestPolicy.Request.RequestID == "" {
		return nil, policy.ErrToolDenied
	}
	if len(args) == 0 || len(args) > maxBusinessRequestBytes {
		return nil, errors.New("tool: business request exceeds limit")
	}
	var object map[string]any
	if err := json.Unmarshal(args, &object); err != nil || object == nil {
		return nil, errors.New("tool: business request must be a JSON object")
	}
	timeoutCtx, cancel := context.WithTimeout(servicelog.WithRedactor(ctx, tool.redactor), configuredTimeout(tool.config.TimeoutSeconds))
	defer cancel()
	request, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, tool.config.Endpoint, bytes.NewReader(args))
	if err != nil {
		return nil, errors.New("tool: business request creation failed")
	}
	request.Header.Set("Authorization", "Bearer "+tool.credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "trpc-agent-service-business-tool/1")
	invocation := policy.InvocationFromContext(timeoutCtx)
	if invocation == 0 {
		invocation = requestPolicy.Invocations.Next()
	}
	if invocation == 0 {
		invocation = 1
	}
	request.Header.Set("X-Idempotency-Key", fmt.Sprintf("%s:%s:%d", requestPolicy.Request.RequestID, tool.config.Name, invocation))
	response, err := tool.client.Do(request)
	if err != nil {
		return nil, errors.New("tool: business endpoint unavailable")
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxBusinessResponseBytes+1))
	if err != nil || len(payload) > maxBusinessResponseBytes {
		return nil, errors.New("tool: business response exceeds limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, errors.New("tool: business endpoint rejected request")
	}
	if mediaType := strings.ToLower(response.Header.Get("Content-Type")); !strings.HasPrefix(mediaType, "application/json") {
		return nil, errors.New("tool: business endpoint returned non-JSON content")
	}
	var result map[string]any
	if err := json.Unmarshal(payload, &result); err != nil || result == nil {
		return nil, errors.New("tool: business endpoint returned invalid JSON")
	}
	return tool.redactor.RedactValue(result), nil
}

func defaultBusinessHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
	}
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

var _ trpctool.CallableTool = (*HTTPJSONTool)(nil)
