package trpcagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type cloneProbeAgent struct {
	agent.Agent
	probe func(context.Context, *agent.Invocation) (<-chan *event.Event, error)
}

func (a cloneProbeAgent) Run(ctx context.Context, inv *agent.Invocation) (<-chan *event.Event, error) {
	return a.probe(ctx, inv)
}

// This test isolates the public Clone seam. Actual Chain/Runner event ordering,
// MCP tool dispatch, Memory read-own-writes and Summary filter consumption are
// exercised by sequence_test.go rather than substituted by this probe.
func TestIsolatedChainPublicClonePreservesExecutionContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	tracer := provider.Tracer("clone-test")
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(parentCtx, deadline)
	defer cancel()
	local, err := newOverlay("tenant", "session", nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sess := session.NewSession(local.key.AppName, local.key.UserID, local.key.SessionID)
	mem := &capabilityMemory{}
	artifacts := &capabilityArtifact{}

	var original *agent.Invocation
	var captured *agent.Invocation
	probe := cloneProbeAgent{Agent: llmagent.New("root"), probe: func(runCtx context.Context, owned *agent.Invocation) (<-chan *event.Event, error) {
		captured = owned
		got, ok := agent.InvocationFromContext(runCtx)
		if !ok || got != owned {
			t.Fatal("context did not bind owned invocation")
		}
		if owned == original || owned.InvocationID == original.InvocationID || owned.GetParentInvocation() != original {
			t.Fatal("SDK clone identity/parent edge lost")
		}
		if owned.Session != sess || owned.SessionService != local || owned.MemoryService != mem || owned.ArtifactService != artifacts {
			t.Fatal("service/session context lost")
		}
		if owned.Branch != original.Branch || owned.GetEventFilterKey() != original.GetEventFilterKey() {
			t.Fatal("branch/filter changed")
		}
		if owned.RunOptions.RequestID != "fixed-request" || owned.Message.Content != "fixed input" {
			t.Fatal("request/message lost")
		}
		gotDeadline, ok := runCtx.Deadline()
		if !ok || !gotDeadline.Equal(deadline) {
			t.Fatal("deadline changed")
		}
		if !trace.SpanContextFromContext(runCtx).Equal(parentSpan.SpanContext()) {
			t.Fatal("parent trace context lost")
		}
		_, childSpan := tracer.Start(runCtx, "owned-execution")
		childSpan.End()
		// Parent and clone must refer to the same registered append-notice channel.
		notice := original.AddNoticeChannel(runCtx, "shared-append")
		if owned.AddNoticeChannel(runCtx, "shared-append") != notice {
			t.Fatal("append notices not shared")
		}
		owned.NotifyCompletion(runCtx, "shared-append")
		select {
		case <-notice:
		default:
			t.Fatal("clone append acknowledgement not visible to parent")
		}
		evt := event.NewResponseEvent(owned.InvocationID, owned.AgentName, &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage("owned final")}}})
		agent.InjectIntoEvent(owned, evt)
		events := make(chan *event.Event, 1)
		events <- evt
		close(events)
		cancel()
		if !errors.Is(runCtx.Err(), context.Canceled) {
			t.Fatal("cancel detached")
		}
		return events, nil
	}}
	original = agent.NewInvocation(agent.WithInvocationAgent(probe), agent.WithInvocationSession(sess), agent.WithInvocationSessionService(local), agent.WithInvocationMemoryService(mem), agent.WithInvocationArtifactService(artifacts), agent.WithInvocationBranch("root"), agent.WithInvocationEventFilterKey("fixed-filter"), agent.WithInvocationMessage(model.NewUserMessage("fixed input")), agent.WithInvocationRunOptions(agent.RunOptions{RequestID: "fixed-request"}))
	defer original.CleanupNotice(context.Background())
	events, err := (isolatedChain{Agent: probe}).Run(agent.NewInvocationContext(ctx, original), original)
	if err != nil {
		t.Fatal(err)
	}
	evt := <-events
	if evt.InvocationID != captured.InvocationID || evt.ParentInvocationID != original.InvocationID || evt.Author != "root" || evt.Branch != "root" || evt.FilterKey != "fixed-filter" {
		t.Fatalf("event identity changed: %+v", evt)
	}
	if original.AgentName != "root" || original.GetParentInvocation() != nil {
		t.Fatal("original invocation mutated")
	}
	parentSpan.End()
	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("spans=%d", len(spans))
	}
	var child sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == "owned-execution" {
			child = span
		}
	}
	if child == nil || child.Parent().SpanID() != parentSpan.SpanContext().SpanID() || child.SpanContext().TraceID() != parentSpan.SpanContext().TraceID() {
		t.Fatal("child trace parent link broken")
	}
}

// Each resource selects its own service. Prior leaf output remains intentional
// Sequence context, but the next leaf's retrieval call cannot use that service.
type sequenceNamespaceKnowledge struct {
	namespace string
	calls     atomic.Int32
}

func (k *sequenceNamespaceKnowledge) Search(_ context.Context, req *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	k.calls.Add(1)
	if req.Query != "orchid" {
		return nil, errors.New("unexpected query")
	}
	d := &document.Document{ID: k.namespace, Content: k.namespace + " retrieved orchid"}
	return &knowledge.SearchResult{Text: d.Content, Document: d, Documents: []*knowledge.Result{{Document: d, Score: 1}}}, nil
}
func TestSequenceKnowledgeServicesAreLeafScoped(t *testing.T) {
	sum := sha256.Sum256([]byte("knowledge/second"))
	secondName := fmt.Sprintf("fn_%x", sum)[:63]
	checkTool := func(want, foreign string) func(map[string]any) error {
		return func(req map[string]any) error {
			messages, _ := req["messages"].([]any)
			found := false
			for _, raw := range messages {
				m, _ := raw.(map[string]any)
				if m["role"] != "tool" {
					continue
				}
				content, _ := m["content"].(string)
				if strings.Contains(content, foreign) {
					return errors.New("foreign service result")
				}
				if strings.Contains(content, want) {
					found = true
				}
			}
			if !found {
				return errors.New("selected service result missing")
			}
			return nil
		}
	}
	first, s1 := newMemoryHTTPFixture(t, []string{docsCallable}, memoryHTTPRound{tool: docsCallable, args: `{"query":"orchid"}`}, memoryHTTPRound{final: "first done", before: checkTool("FIRST_NAMESPACE", "SECOND_NAMESPACE")})
	second, s2 := newMemoryHTTPFixture(t, []string{secondName}, memoryHTTPRound{tool: secondName, args: `{"query":"orchid"}`}, memoryHTTPRound{final: "second done", before: checkTool("SECOND_NAMESPACE", "FIRST_NAMESPACE")})
	one, two := &sequenceNamespaceKnowledge{namespace: "FIRST_NAMESPACE"}, &sequenceNamespaceKnowledge{namespace: "SECOND_NAMESPACE"}
	req := testRequest(s1.URL)
	req.MaxToolCalls = 2
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": {Kind: "llm", Model: req.Model, Knowledge: &KnowledgeConfig{Resource: "docs", Service: one}}, "last": {Kind: "llm", Model: testRequest(s2.URL).Model, Knowledge: &KnowledgeConfig{Resource: "second", Service: two}}}
	result, err := testExecutor().Execute(context.Background(), req)
	if err != nil || result.FinalText != "second done" || one.calls.Load() != 1 || two.calls.Load() != 1 {
		t.Fatalf("result=%+v err=%v servicecalls=%d/%d", result, err, one.calls.Load(), two.calls.Load())
	}
	first.check(t, 2)
	second.check(t, 2)
}
