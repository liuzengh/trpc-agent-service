package assembly

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/netpolicy"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

const maxHTTPToolResponseBytes = int64(1 << 20)

func (r *ToolRegistry) newHTTPTool(cfg config.HTTPToolConfig) (agenttool.CallableTool, error) {
	if err := netpolicy.ValidatePublicHTTPSURL(cfg.URL); err != nil {
		return nil, err
	}
	schema := cfg.InputSchema
	if schema == nil {
		schema = &agenttool.Schema{Type: "object", AdditionalProperties: true}
	}
	return function.NewFunctionTool(
		func(ctx context.Context, input map[string]any) (map[string]any, error) {
			payload, err := json.Marshal(input)
			if err != nil {
				return nil, fmt.Errorf("encode tool request: %w", err)
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(payload))
			if err != nil {
				return nil, fmt.Errorf("create tool request: %w", err)
			}
			request.Header.Set("Content-Type", "application/json")
			if err := r.applyBearerCredential(ctx, request, cfg.CredentialRef); err != nil {
				return nil, err
			}
			response, err := r.httpClient.Do(request)
			if err != nil {
				return nil, fmt.Errorf("call remote tool: %w", err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(io.LimitReader(response.Body, maxHTTPToolResponseBytes+1))
			if err != nil {
				return nil, fmt.Errorf("read remote tool response: %w", err)
			}
			if int64(len(body)) > maxHTTPToolResponseBytes {
				return nil, fmt.Errorf("remote tool response exceeds %d bytes", maxHTTPToolResponseBytes)
			}
			var decoded any
			if len(body) > 0 && json.Unmarshal(body, &decoded) != nil {
				decoded = string(body)
			}
			result := map[string]any{"status_code": response.StatusCode, "body": decoded}
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				return result, fmt.Errorf("remote tool returned HTTP %d", response.StatusCode)
			}
			return result, nil
		},
		function.WithName(strings.TrimSpace(cfg.Name)),
		function.WithDescription(strings.TrimSpace(cfg.Description)),
		function.WithInputSchema(schema),
	), nil
}
