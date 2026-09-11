package storage

import (
	"context"
	"errors"
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

func TestMemoryStateStoreExecutionTraceValidationAndCloneIsolation(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	base := ExecutionTraceRecord{
		TenantID: "tenant-a", AppCode: "support", Channel: "web", BindingID: "web-console", MessageID: "message-1", TraceID: "trace-1",
		Trace: AgentExecutionTrace{
			Status: "completed",
			Usage:  &ExecutionTraceUsage{PromptTokens: 10},
			Steps: []ExecutionTraceStep{{
				StepID: "step-1", PredecessorStepIDs: []string{"before"}, AppliedSurfaceIDs: []string{"surface-1"},
				Usage: &ExecutionTraceUsage{CompletionTokens: 4},
			}},
		},
	}
	if err := store.RecordExecutionTrace(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	base.Trace.Usage.PromptTokens = 999
	base.Trace.Steps[0].PredecessorStepIDs[0] = "mutated"
	base.Trace.Steps[0].Usage.CompletionTokens = 999
	got, err := store.GetExecutionTrace(context.Background(), "tenant-a", "web", "web-console", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Trace.Usage.PromptTokens != 10 || got.Trace.Steps[0].PredecessorStepIDs[0] != "before" || got.Trace.Steps[0].Usage.CompletionTokens != 4 {
		t.Fatalf("stored trace mutated through caller input: %#v", got)
	}
	got.Trace.Steps[0].AppliedSurfaceIDs[0] = "changed"
	again, err := store.GetExecutionTrace(context.Background(), "tenant-a", "web", "web-console", "message-1")
	if err != nil || again.Trace.Steps[0].AppliedSurfaceIDs[0] != "surface-1" {
		t.Fatalf("stored trace mutated through returned clone: %#v, %v", again, err)
	}

	for _, mutate := range []func(*ExecutionTraceRecord){
		func(record *ExecutionTraceRecord) { record.TenantID = "" },
		func(record *ExecutionTraceRecord) { record.AppCode = "" },
		func(record *ExecutionTraceRecord) { record.Channel = "" },
		func(record *ExecutionTraceRecord) { record.BindingID = "" },
		func(record *ExecutionTraceRecord) { record.MessageID = "" },
		func(record *ExecutionTraceRecord) { record.TraceID = "" },
		func(record *ExecutionTraceRecord) { record.Trace.Status = "unknown" },
	} {
		invalid := base
		invalid.TenantID, invalid.AppCode, invalid.Channel, invalid.BindingID, invalid.MessageID, invalid.TraceID = "tenant-a", "support", "web", "web-console", "message-x", "trace-x"
		invalid.Trace.Status = "completed"
		mutate(&invalid)
		if err := store.RecordExecutionTrace(context.Background(), invalid); err == nil {
			t.Fatal("RecordExecutionTrace() accepted invalid record")
		}
	}
}

func TestMemoryStateStoreExecutionTraceValidatesReadsAndContext(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	for _, args := range [][4]string{
		{"", "web", "web-console", "message"},
		{"tenant-a", "", "web-console", "message"},
		{"tenant-a", "web", "", "message"},
		{"tenant-a", "web", "web-console", ""},
	} {
		if _, err := store.GetExecutionTrace(context.Background(), args[0], args[1], args[2], args[3]); err == nil {
			t.Fatalf("GetExecutionTrace(%q,%q,%q,%q) error = nil", args[0], args[1], args[2], args[3])
		}
	}
	if records, err := store.ListExecutionTraces(context.Background(), "tenant-a", nil); err != nil || records != nil {
		t.Fatalf("ListExecutionTraces(empty) = %#v, %v", records, err)
	}
	if _, err := store.ListExecutionTraces(context.Background(), "", []ExecutionTraceRef{{Channel: "web", BindingID: "b", MessageID: "m"}}); err == nil {
		t.Fatal("ListExecutionTraces() accepted blank tenant")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.RecordExecutionTrace(cancelled, ExecutionTraceRecord{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled record error = %v", err)
	}
	if _, err := store.GetExecutionTrace(cancelled, "tenant-a", "web", "b", "m"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled get error = %v", err)
	}
	if _, err := store.ListExecutionTraces(cancelled, "tenant-a", []ExecutionTraceRef{{Channel: "web", BindingID: "b", MessageID: "m"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled list error = %v", err)
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
