package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestProjectExecutionTraceKeepsStructureAndDropsSensitiveSnapshots(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	framework := &trace.Trace{
		RootAgentName:    "assistant",
		RootInvocationID: "inv-root",
		SessionID:        "tenant-a/support/web/session-1",
		StartedAt:        started,
		EndedAt:          started.Add(2 * time.Second),
		Status:           trace.TraceStatusFailed,
		Input:            &trace.Snapshot{Text: "user secret"},
		Output:           &trace.Snapshot{Text: "assistant secret"},
		Usage:            &model.Usage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7},
		Steps: []trace.Step{{
			StepID: "step-1", InvocationID: "inv-root", AgentName: "assistant",
			NodeID: "assistant#model", NodeType: "llm", Branch: "main",
			StartedAt: started, EndedAt: started.Add(time.Second),
			PredecessorStepIDs: []string{"entry"}, AppliedSurfaceIDs: []string{"assistant#instruction"},
			Input: &trace.Snapshot{Text: "tool token=secret"}, Output: &trace.Snapshot{Text: "private output"},
			Usage: &model.Usage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7}, Error: "authorization=Bearer secret",
		}},
	}

	projected := projectExecutionTrace(framework)
	if projected == nil || projected.Status != "failed" || len(projected.Steps) != 1 {
		t.Fatalf("projectExecutionTrace() = %#v", projected)
	}
	step := projected.Steps[0]
	if !step.Failed || step.NodeType != "llm" || step.Usage == nil || step.Usage.TotalTokens != 7 {
		t.Fatalf("projected step = %#v", step)
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatalf("marshal projection: %v", err)
	}
	text := string(encoded)
	for _, secret := range []string{"user secret", "assistant secret", "tool token=secret", "private output", "Bearer secret"} {
		if strings.Contains(text, secret) {
			t.Fatalf("projection leaked %q: %s", secret, text)
		}
	}
}

func TestCollectRunKeepsFailureTraceFromRunnerCompletion(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event, 3)
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "partial")
	events <- &event.Event{Response: &model.Response{Done: true, Object: model.ObjectTypeError, Error: &model.ResponseError{Message: "boom"}}}
	completion := responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
	completion.ExecutionTrace = &trace.Trace{Status: trace.TraceStatusFailed, RootInvocationID: "inv-failed"}
	events <- completion
	close(events)

	outcome := collectRun(context.Background(), events, nil)
	if !errors.Is(outcome.err, ErrAgentExecutionFailed) {
		t.Fatalf("collectRun() error = %v, want ErrAgentExecutionFailed", outcome.err)
	}
	if outcome.trace == nil || outcome.trace.RootInvocationID != "inv-failed" {
		t.Fatalf("collectRun() trace = %#v", outcome.trace)
	}
}

type executionTraceRunner struct {
	enabled bool
	failed  bool
}

func (r *executionTraceRunner) Run(_ context.Context, _ string, sessionID string, _ model.Message, options ...frameworkagent.RunOption) (<-chan *event.Event, error) {
	var runOptions frameworkagent.RunOptions
	for _, option := range options {
		option(&runOptions)
	}
	r.enabled = runOptions.ExecutionTraceEnabled
	events := make(chan *event.Event, 3)
	if r.failed {
		events <- &event.Event{Response: &model.Response{Done: true, Object: model.ObjectTypeError, Error: &model.ResponseError{Message: "private failure"}}}
	} else {
		events <- responseEvent(false, model.ObjectTypeChatCompletion, "runtime reply", "")
	}
	completion := responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
	status := trace.TraceStatusCompleted
	if r.failed {
		status = trace.TraceStatusFailed
	}
	completion.ExecutionTrace = &trace.Trace{
		RootAgentName: "assistant", RootInvocationID: "framework-invocation", SessionID: sessionID,
		Status: status, StartedAt: time.Now().Add(-time.Second), EndedAt: time.Now(),
		Input: &trace.Snapshot{Text: "private input"}, Output: &trace.Snapshot{Text: "private output"},
	}
	events <- completion
	close(events)
	return events, nil
}

func (*executionTraceRunner) Close() error { return nil }

func TestRuntimeEnablesAndPersistsFrameworkExecutionTrace(t *testing.T) {
	t.Parallel()
	stateStore := storage.NewMemoryStateStore()
	runner := &executionTraceRunner{}
	runtime, err := NewRuntime(tenant.NewMemoryRepository(), staticRunnerProvider{runner: runner}, storage.NewMemoryIdempotencyStore(), stateStore, time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1}}
	_, err = runtime.Handle(WithConfigurationSnapshot(context.Background(), snapshot), "web-console", channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Web, ConversationID: "conversation-1", SenderID: "user-1", WebOwnerID: "user-1", Text: "private input",
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !runner.enabled {
		t.Fatal("Runtime did not enable framework ExecutionTrace")
	}
	record, err := stateStore.GetExecutionTrace(context.Background(), "tenant-a", "web", "web-console", "message-1")
	if err != nil || record.Trace.RootInvocationID != "framework-invocation" || record.Trace.Status != "completed" {
		t.Fatalf("GetExecutionTrace() = %#v, %v", record, err)
	}
}

func TestRuntimePersistsFailedFrameworkTraceWithoutOutbox(t *testing.T) {
	t.Parallel()
	stateStore := storage.NewMemoryStateStore()
	runtime, err := NewRuntime(tenant.NewMemoryRepository(), staticRunnerProvider{runner: &executionTraceRunner{failed: true}}, storage.NewMemoryIdempotencyStore(), stateStore, time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1}}
	_, err = runtime.Handle(WithConfigurationSnapshot(context.Background(), snapshot), "web-console", channels.InboundMessage{
		MessageID: "message-failed", Channel: channels.Web, ConversationID: "conversation-failed", SenderID: "user-1", WebOwnerID: "user-1", Text: "private input",
	})
	if !errors.Is(err, ErrAgentExecutionFailed) {
		t.Fatalf("Handle() error = %v, want ErrAgentExecutionFailed", err)
	}
	record, traceErr := stateStore.GetExecutionTrace(context.Background(), "tenant-a", "web", "web-console", "message-failed")
	if traceErr != nil || record.Trace.Status != "failed" {
		t.Fatalf("failed execution trace = %#v, %v", record, traceErr)
	}
	if pendingOutboxCount(t, stateStore, "tenant-a") != 0 {
		t.Fatal("failed execution created an outbox reply")
	}
}
