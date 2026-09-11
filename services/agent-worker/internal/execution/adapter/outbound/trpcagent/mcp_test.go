package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/mcptoolset"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type executionMCPTool struct {
	calls atomic.Int32
	call  func(context.Context, []byte, int32) (any, error)
}

func (t *executionMCPTool) Declaration() *tool.Declaration {
	return &tool.Declaration{Name: "remote_search", InputSchema: &tool.Schema{Type: "object", Properties: map[string]*tool.Schema{"q": {Type: "string"}}, Required: []string{"q"}, AdditionalProperties: false}}
}
func (t *executionMCPTool) Call(ctx context.Context, args []byte) (any, error) {
	n := t.calls.Add(1)
	if t.call != nil {
		return t.call(ctx, args, n)
	}
	return map[string]any{"text": "selected remote result"}, nil
}

type executionMCPBusinessResult struct {
	Text string `json:"text"`
}

func (executionMCPBusinessResult) RetryResultError() bool { return true }

func TestMCPExecutorMultipleAliasesAndKnowledge(t *testing.T) {
	names := []string{mcpSearchProvider, mcpAProvider, docsCallable}
	f, server := newMemoryHTTPFixture(t, names,
		memoryHTTPRound{tool: mcpSearchProvider, args: `{"q":"first"}`, fragmented: true},
		memoryHTTPRound{tool: docsCallable, args: `{"query":"orchid"}`, fragmented: true},
		memoryHTTPRound{tool: mcpAProvider, args: `{"q":"second"}`, fragmented: true},
		memoryHTTPRound{final: "MCP and Knowledge complete", before: func(req map[string]any) error {
			wire, _ := json.Marshal(req["messages"])
			text := string(wire)
			for _, name := range names {
				if !strings.Contains(text, name) {
					return fmt.Errorf("missing fixed provider history %s", name)
				}
			}
			for _, internal := range []string{"mcp_search", "mcp_a", "remote_search", sdkKnowledgeName} {
				if strings.Contains(text, internal) {
					return fmt.Errorf("internal name leaked to provider: %s", internal)
				}
			}
			return nil
		}})
	first, second, notSelected := &executionMCPTool{}, &executionMCPTool{}, &executionMCPTool{}
	kb := &callableKnowledge{}
	req := testRequest(server.URL)
	req.MaxToolCalls = 10
	req.Tools = []MCPToolConfig{{Resource: "search", Tool: first}, {Resource: "a", Tool: second}}
	req.Knowledge = &KnowledgeConfig{Resource: "docs", Service: kb}
	result, err := testExecutor().Execute(context.Background(), req)
	if err != nil || result.FinalText != "MCP and Knowledge complete" || len(result.Snapshot) == 0 {
		t.Fatalf("execution %v result=%+v", err, result)
	}
	if first.calls.Load() != 1 || second.calls.Load() != 1 || kb.calls.Load() != 1 || notSelected.calls.Load() != 0 {
		t.Fatal("selection/alias isolation")
	}
	snapshot := string(result.Snapshot)
	for _, internal := range []string{"mcp_search", "mcp_a", sdkKnowledgeName} {
		if !strings.Contains(snapshot, internal) {
			t.Fatal("internal accepted history missing", internal)
		}
	}
	if strings.Contains(snapshot, mcpSearchProvider) || strings.Contains(snapshot, mcpAProvider) || strings.Contains(snapshot, docsCallable) {
		t.Fatal("provider alias persisted in session")
	}
	f.check(t, 4)
}
func TestMCPExecutorCorrectableResults(t *testing.T) {
	for _, kind := range []string{"business-iserror", "arguments"} {
		t.Run(kind, func(t *testing.T) {
			remote := &executionMCPTool{call: func(_ context.Context, _ []byte, n int32) (any, error) {
				if n == 1 {
					if kind == "arguments" {
						return nil, mcptoolset.ErrArguments
					}
					return executionMCPBusinessResult{Text: "use a valid query"}, nil
				}
				return map[string]any{"text": "corrected result"}, nil
			}}
			f, server := newMemoryHTTPFixture(t, []string{mcpSearchProvider},
				memoryHTTPRound{tool: mcpSearchProvider, args: `{"q":"initial"}`},
				memoryHTTPRound{tool: mcpSearchProvider, args: `{"q":"corrected"}`, before: func(req map[string]any) error {
					wire, _ := json.Marshal(req["messages"])
					expected := "use a valid query"
					if kind == "arguments" {
						expected = mcptoolset.ErrArguments.Error()
					}
					if !strings.Contains(string(wire), expected) {
						return errors.New("model did not receive correctable error")
					}
					return nil
				}},
				memoryHTTPRound{final: "corrected successfully"})
			req := testRequest(server.URL)
			req.MaxToolCalls = 5
			req.Tools = []MCPToolConfig{{Resource: "search", Tool: remote}}
			result, err := testExecutor().Execute(context.Background(), req)
			if err != nil || result.FinalText != "corrected successfully" || remote.calls.Load() != 2 {
				t.Fatalf("correctable %v calls=%d", err, remote.calls.Load())
			}
			f.check(t, 3)
		})
	}
}
func TestMCPExecutorInfrastructureStopsFinal(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cause, want error
	}{
		{"network", mcptoolset.ErrNetwork, ErrMCPDependency},
		{"authentication", mcptoolset.ErrAuthentication, ErrMCPAuthentication},
		{"protocol", mcptoolset.ErrProtocol, ErrMCP},
		{"deadline", context.DeadlineExceeded, ErrMCPDependency},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := &executionMCPTool{call: func(context.Context, []byte, int32) (any, error) { return nil, tc.cause }}
			f, server := newMemoryHTTPFixture(t, []string{mcpSearchProvider}, memoryHTTPRound{tool: mcpSearchProvider, args: `{"q":"one"}`})
			req := testRequest(server.URL)
			req.MaxToolCalls = 5
			req.Tools = []MCPToolConfig{{Resource: "search", Tool: remote}}
			result, err := testExecutor().Execute(context.Background(), req)
			if !errors.Is(err, tc.want) || !reflect.DeepEqual(result, Result{}) || remote.calls.Load() != 1 {
				t.Fatalf("infra err=%v result=%+v calls=%d", err, result, remote.calls.Load())
			}
			f.check(t, 1)
		})
	}
}
func TestMCPBoundStateFirstFailureAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := &mcpState{cancel: cancel}
	remote := &executionMCPTool{call: func(context.Context, []byte, int32) (any, error) { return nil, mcptoolset.ErrAuthentication }}
	bound := boundMCPTool{CallableTool: remote, name: "mcp_search", state: state}
	if _, err := bound.Call(ctx, []byte("{}")); !errors.Is(err, ErrMCPAuthentication) || ctx.Err() != context.Canceled {
		t.Fatal("infrastructure did not cancel", err)
	}
	state.fail(ErrMCPDependency)
	if !errors.Is(state.failure(), ErrMCPAuthentication) {
		t.Fatal("sticky first failure overwritten")
	}
	if bound.Declaration().Name != "mcp_search" || remote.Declaration().Name != "remote_search" {
		t.Fatal("SDK declaration mutated")
	}
}

func TestMCPExecutorRejectsUnselectedProviderAndInvalidConfig(t *testing.T) {
	t.Run("unselected-provider", func(t *testing.T) {
		remote := &executionMCPTool{}
		f, server := newMemoryHTTPFixture(t, []string{mcpSearchProvider}, memoryHTTPRound{tool: mcpAProvider, args: `{"q":"unauthorized"}`, fragmented: true})
		req := testRequest(server.URL)
		req.MaxToolCalls = 5
		req.Tools = []MCPToolConfig{{Resource: "search", Tool: remote}}
		result, err := testExecutor().Execute(context.Background(), req)
		if err == nil || !reflect.DeepEqual(result, Result{}) || remote.calls.Load() != 0 {
			t.Fatalf("unselected response reached execution: err=%v calls=%d", err, remote.calls.Load())
		}
		f.check(t, 1)
	})
	for _, name := range []string{"duplicate", "invalid-resource", "nil-tool", "zero-tool-budget"} {
		t.Run(name, func(t *testing.T) {
			remote := &executionMCPTool{}
			f, server := newMemoryHTTPFixture(t, nil)
			req := testRequest(server.URL)
			req.MaxToolCalls = 5
			req.Tools = []MCPToolConfig{{Resource: "search", Tool: remote}}
			switch name {
			case "duplicate":
				req.Tools = append(req.Tools, req.Tools[0])
			case "invalid-resource":
				req.Tools[0].Resource = "search/extra"
			case "nil-tool":
				req.Tools[0].Tool = nil
			case "zero-tool-budget":
				req.MaxToolCalls = 0
			}
			result, err := testExecutor().Execute(context.Background(), req)
			if err == nil || !reflect.DeepEqual(result, Result{}) || remote.calls.Load() != 0 {
				t.Fatalf("invalid config reached execution: %v", err)
			}
			f.check(t, 0)
		})
	}
}
