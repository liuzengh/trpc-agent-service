// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"errors"
	"time"

	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// Guarded enforces tenant policy again inside the final callable handler.
type Guarded struct{ Delegate trpctool.CallableTool }

func (tool Guarded) Declaration() *trpctool.Declaration {
	if tool.Delegate == nil {
		return nil
	}
	return tool.Delegate.Declaration()
}

// ToolMetadata preserves upstream safety annotations while the final callable
// guard adds authorization and telemetry.
func (tool Guarded) ToolMetadata() trpctool.ToolMetadata {
	return trpctool.MetadataOf(tool.Delegate)
}
func (tool Guarded) Call(ctx context.Context, args []byte) (result any, callErr error) {
	if tool.Delegate == nil {
		return nil, errors.New("tool: nil delegate")
	}
	request, ok := policy.FromContext(ctx)
	if !ok {
		return nil, policy.ErrToolDenied
	}
	name := ""
	if declaration := tool.Declaration(); declaration != nil {
		name = declaration.Name
	}
	if err := request.Engine.AuthorizeDirect(ctx, request.Request, name); err != nil {
		return nil, err
	}
	if telemetry, ok := servicemetrics.FromContext(ctx); ok {
		started := time.Now()
		toolCtx, toolSpan := telemetry.Telemetry.Start(ctx, "tool.call", telemetry.Fields)
		defer func() {
			toolSpan.End()
			status := "success"
			if callErr != nil {
				status = "failed"
			}
			telemetry.Telemetry.Request(toolCtx, servicemetrics.Labels{TenantID: telemetry.Fields.TenantID, AppID: telemetry.Fields.AppID, Channel: telemetry.Fields.Channel, Operation: "tool", Status: status}, time.Since(started), 0, 0)
		}()
		return tool.Delegate.Call(toolCtx, args)
	}
	return tool.Delegate.Call(ctx, args)
}

// WrapAll guards every callable platform tool and preserves other tool kinds,
// which remain protected by the per-run visibility and permission controls.
func WrapAll(tools []trpctool.Tool) []trpctool.Tool {
	result := make([]trpctool.Tool, 0, len(tools))
	for _, candidate := range tools {
		callable, ok := candidate.(trpctool.CallableTool)
		if ok {
			result = append(result, Guarded{Delegate: callable})
		} else {
			result = append(result, candidate)
		}
	}
	return result
}

var _ trpctool.CallableTool = Guarded{}
var _ trpctool.MetadataProvider = Guarded{}
