package trpcagent

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/mcptoolset"
	"sync"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var ErrMCP = errors.New("MCP tool execution failed")
var ErrMCPDependency = errors.New("MCP dependency unavailable")
var ErrMCPAuthentication = errors.New("MCP authentication rejected")

type MCPToolConfig struct {
	Resource   string
	Capability string
	Tool       tool.CallableTool
}
type mcpState struct {
	mu     sync.Mutex
	err    error
	cancel context.CancelFunc
}

func (s *mcpState) failure() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *mcpState) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

type boundMCPTool struct {
	tool.CallableTool
	name  string
	state *mcpState
}

func (t boundMCPTool) Declaration() *tool.Declaration {
	d := *t.CallableTool.Declaration()
	d.Name = t.name
	return &d
}
func (t boundMCPTool) Call(ctx context.Context, args []byte) (any, error) {
	result, err := t.CallableTool.Call(ctx, args)
	// SDK MCP IsError is a successful Go result carrying business-level failure;
	// argument validation also remains correctable by the model. Neither poisons
	// the Attempt. Infrastructure/auth errors stop it under existing retry policy.
	if err == nil || errors.Is(err, mcptoolset.ErrArguments) {
		return result, err
	}
	failure := ErrMCP
	if errors.Is(err, mcptoolset.ErrNetwork) || errors.Is(err, context.DeadlineExceeded) {
		failure = ErrMCPDependency
	}
	if errors.Is(err, mcptoolset.ErrAuthentication) {
		failure = ErrMCPAuthentication
	}
	if ctx.Err() != nil {
		failure = ctx.Err()
	}
	t.state.fail(failure)
	return nil, failure
}
func mcpToolAlias(resource string) string { return "mcp_" + resource }
