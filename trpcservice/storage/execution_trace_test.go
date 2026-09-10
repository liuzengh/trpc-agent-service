package storage

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestMemoryStateStorePersistsExecutionTraceProjection(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	started := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	want := ExecutionTraceRecord{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", BindingID: "bot-a", MessageID: "message-1", TraceID: "trace-1",
		Trace: AgentExecutionTrace{
			Status: "completed", RootAgentName: "assistant", RootInvocationID: "inv-1", SessionID: "session-1",
			StartedAt: started, EndedAt: started.Add(time.Second),
			Usage: &ExecutionTraceUsage{PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5},
			Steps: []ExecutionTraceStep{{StepID: "step-1", NodeID: "assistant#model", NodeType: "llm", StartedAt: started, EndedAt: started.Add(time.Second)}},
		},
	}
	if err := store.RecordExecutionTrace(context.Background(), want); err != nil {
		t.Fatalf("RecordExecutionTrace() error = %v", err)
	}
	got, err := store.GetExecutionTrace(context.Background(), "tenant-a", "telegram", "bot-a", "message-1")
	if err != nil {
		t.Fatalf("GetExecutionTrace() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetExecutionTrace() = %#v, want %#v", got, want)
	}
	if _, err := store.GetExecutionTrace(context.Background(), "tenant-b", "telegram", "bot-a", "message-1"); err != ErrExecutionTraceNotFound {
		t.Fatalf("cross-tenant GetExecutionTrace() error = %v, want ErrExecutionTraceNotFound", err)
	}
	otherChannel := want
	otherChannel.Channel = "wecom"
	otherChannel.BindingID = "wecom-a"
	otherChannel.TraceID = "trace-2"
	otherChannel.Trace.RootInvocationID = "inv-2"
	if err := store.RecordExecutionTrace(context.Background(), otherChannel); err != nil {
		t.Fatalf("RecordExecutionTrace(other channel) error = %v", err)
	}
	got, err = store.GetExecutionTrace(context.Background(), "tenant-a", "telegram", "bot-a", "message-1")
	if err != nil || got.Trace.RootInvocationID != "inv-1" {
		t.Fatalf("telegram trace after same message ID on another channel = %#v, %v", got, err)
	}
	got, err = store.GetExecutionTrace(context.Background(), "tenant-a", "wecom", "wecom-a", "message-1")
	if err != nil || got.Trace.RootInvocationID != "inv-2" {
		t.Fatalf("wecom trace with same message ID = %#v, %v", got, err)
	}
}

func TestMemoryStateStoreListsExecutionTracesByMessageRefs(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	first := ExecutionTraceRecord{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", BindingID: "bot-a", MessageID: "message-1", TraceID: "trace-1",
		Trace: AgentExecutionTrace{Status: "completed", Usage: &ExecutionTraceUsage{PromptTokens: 8, CompletionTokens: 2, TotalTokens: 10, CachedTokens: 6}},
	}
	second := first
	second.Channel = "web"
	second.BindingID = "web-console"
	second.MessageID = "message-2"
	second.TraceID = "trace-2"
	second.Trace.Usage = &ExecutionTraceUsage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7, ReasoningTokens: 1}
	if err := store.RecordExecutionTrace(context.Background(), first); err != nil {
		t.Fatalf("RecordExecutionTrace(first) error = %v", err)
	}
	if err := store.RecordExecutionTrace(context.Background(), second); err != nil {
		t.Fatalf("RecordExecutionTrace(second) error = %v", err)
	}
	records, err := store.ListExecutionTraces(context.Background(), "tenant-a", []ExecutionTraceRef{
		{Channel: "web", BindingID: "web-console", MessageID: "message-2"},
		{Channel: "telegram", BindingID: "bot-a", MessageID: "missing"},
		{Channel: "telegram", BindingID: "bot-a", MessageID: "message-1"},
	})
	if err != nil {
		t.Fatalf("ListExecutionTraces() error = %v", err)
	}
	if len(records) != 2 || records[0].TraceID != "trace-2" || records[1].TraceID != "trace-1" {
		t.Fatalf("ListExecutionTraces() = %#v", records)
	}
	foreign, err := store.ListExecutionTraces(context.Background(), "tenant-b", []ExecutionTraceRef{{Channel: "telegram", BindingID: "bot-a", MessageID: "message-1"}})
	if err != nil || len(foreign) != 0 {
		t.Fatalf("cross-tenant ListExecutionTraces() = %#v, %v", foreign, err)
	}
}
