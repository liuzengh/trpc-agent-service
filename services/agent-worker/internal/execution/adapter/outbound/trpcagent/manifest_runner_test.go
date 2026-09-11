package trpcagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorytool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// The model is deterministic, not an external Provider. The compiler fixture,
// SDK LLMAgent, Runner, SDK tools, scoped Attempt and OTLP exporter are real.
// This does not execute the production factory or commit a formal Memory revision.
type compiledMemoryModel struct{ calls atomic.Int32 }

func (*compiledMemoryModel) Info() model.Info { return model.Info{Name: "compiled-memory-fixture"} }
func (m *compiledMemoryModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(req.Tools) != 2 || req.Tools[memory.AddToolName] == nil || req.Tools[memory.LoadToolName] == nil {
		return nil, fmt.Errorf("unexpected exposed tools")
	}
	call := m.calls.Add(1)
	msg := model.NewAssistantMessage("memory cycle complete")
	switch call {
	case 1:
		msg = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "remember", Type: "function", Function: model.FunctionDefinitionParam{Name: memory.AddToolName, Arguments: []byte(`{"memory":"CANARY private prompt response arguments"}`)}}}}
	case 2:
		msg = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "recall", Type: "function", Function: model.FunctionDefinitionParam{Name: memory.LoadToolName, Arguments: []byte(`{}`)}}}}
	case 3:
		found := false
		for _, message := range req.Messages {
			if message.Role != model.RoleTool || message.ToolID != "recall" || message.ToolName != memory.LoadToolName {
				continue
			}
			var recalled memorytool.LoadMemoryResponse
			if json.Unmarshal([]byte(message.Content), &recalled) == nil && recalled.Count == 1 && len(recalled.Results) == 1 && recalled.Results[0].Memory == traceCanary {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("memory load result did not reach model")
		}
	default:
		return nil, fmt.Errorf("unexpected model retry")
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{ID: fmt.Sprint(call), Done: true, Choices: []model.Choice{{Message: msg}}}
	close(ch)
	return ch, nil
}

func TestCompiledManifestRunsSDKMemoryCycle(t *testing.T) {
	raw, err := os.ReadFile("testdata/compiler-memory-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.DecodeRuntimeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	content, err := protocol.VerifyRuntimeManifest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	rt, sink := newTraceRuntime(t)
	ctx, parent := rt.Tracer("worker-compiler-fixture").Start(context.Background(), "worker.runner.run")
	req := domain.Requested{Route: domain.Route{TenantID: content.TenantID, Provider: "telegram", AccountID: "fixture-bot"}}
	req.Input.SenderID = "fixture-social-user"
	scope, err := req.MemoryScopeID(content.Sources.Agent.AgentID)
	if err != nil {
		t.Fatal(err)
	}
	sdkKey := memory.UserKey{AppName: "compiler-fixture", UserID: "session-user"}
	boundKey := memory.UserKey{AppName: content.TenantID, UserID: scope}
	attempt, err := NewMemoryAttempt(ctx, sdkKey, boundKey, nil, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.Close()
	service, err := TraceMemoryService(attempt, rt.Tracer("worker-memory"))
	if err != nil {
		t.Fatal(err)
	}
	options, err := BuildManifestCapabilityOptions(content, content.AgentPlan.Root, CapabilityServices{Memory: service})
	if err != nil {
		t.Fatal(err)
	}
	node := content.AgentPlan.Nodes[content.AgentPlan.Root]
	fake := &compiledMemoryModel{}
	agentOptions := []llmagent.Option{llmagent.WithModel(fake), llmagent.WithInstruction(node.Instruction), llmagent.WithEnableCodeExecutionResponseProcessor(false), llmagent.WithCodeExecutor(nil)}
	a := llmagent.New(content.AgentPlan.Root, append(agentOptions, options.Agent...)...)
	r := runner.NewRunner(sdkKey.AppName, a, options.Runner...)
	defer r.Close()
	events, err := r.Run(ctx, sdkKey.UserID, "compiler-session", model.NewUserMessage("remember and recall"), agent.WithDetachedCancel(false))
	if err != nil {
		t.Fatal(err)
	}
	final := false
	for event := range events {
		if event.Response == nil {
			continue
		}
		if event.Error != nil {
			t.Fatalf("SDK error: %v", event.Error)
		}
		for _, choice := range event.Choices {
			if choice.Message.Content == "memory cycle complete" {
				final = true
			}
		}
	}
	parent.End()
	if !final || fake.calls.Load() != 3 {
		t.Fatalf("final=%v model_calls=%d", final, fake.calls.Load())
	}
	candidate, err := attempt.Seal(context.Background())
	if err != nil || candidate.Scope != boundKey || candidate.BaseRevision != 7 || len(candidate.Entries) != 1 || candidate.Entries[0].Memory.Memory != traceCanary {
		t.Fatalf("candidate mismatch: %v", err)
	}
	flushTrace(t, rt)
	spans := sink.snapshot()
	seenWrite, seenRead := false, false
	for _, span := range spans {
		if span.Name == "memory.write" || span.Name == "memory.read" {
			if !hasSpanAncestor(spans, span, parent.SpanContext().SpanID()) {
				t.Fatal("memory trace detached")
			}
			toolParent := false
			for _, ancestor := range spans {
				if strings.HasPrefix(ancestor.Name, "execute_tool memory_") && string(ancestor.SpanId) == string(span.ParentSpanId) {
					toolParent = true
				}
			}
			if !toolParent {
				t.Fatal("memory span does not descend directly from SDK tool")
			}
			seenWrite = seenWrite || span.Name == "memory.write"
			seenRead = seenRead || span.Name == "memory.read"
		}
	}
	if !seenWrite || !seenRead {
		t.Fatal("memory write/read trace missing")
	}
	t.Log("COMPILED_MANIFEST_SDK_MEMORY=PASS model_calls=3 sdk_tools=2 candidate_entries=1 trace_parentage=PASS formal_commit=NOT_RUN")
}

func TestCompiledMemoryModelRequiresRecallResult(t *testing.T) {
	for _, tc := range []struct{ name, id, tool, body string }{
		{"add echo only", "remember", memory.AddToolName, `{"memory":"CANARY private prompt response arguments"}`},
		{"empty recall with echoed query", "recall", memory.LoadToolName, `{"count":0,"results":[],"query":"CANARY private prompt response arguments"}`},
		{"wrong call id", "other", memory.LoadToolName, `{"count":1,"results":[{"memory":"CANARY private prompt response arguments"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &compiledMemoryModel{}
			m.calls.Store(2)
			req := &model.Request{Tools: map[string]tool.Tool{memory.AddToolName: namedCapabilityTool(memory.AddToolName), memory.LoadToolName: namedCapabilityTool(memory.LoadToolName)}, Messages: []model.Message{{Role: model.RoleTool, ToolID: tc.id, ToolName: tc.tool, Content: tc.body}}}
			if _, err := m.GenerateContent(context.Background(), req); err == nil {
				t.Fatal("final accepted without actual recall result")
			}
		})
	}
}
