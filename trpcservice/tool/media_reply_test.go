package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestMediaReplyToolRespectsRevisionAuthorization(t *testing.T) {
	registry := DefaultRegistry()
	tools, err := registry.Resolve([]appmodel.ToolAuthorization{{ToolID: SendTestImageID, Required: true}})
	if err != nil || len(tools) != 1 || tools[0].Declaration().Name != SendTestImageID {
		t.Fatalf("authorized tools = %#v, err=%v", tools, err)
	}
	if tools, err := registry.Resolve([]appmodel.ToolAuthorization{{ToolID: "unknown"}}); err != nil || len(tools) != 0 {
		t.Fatalf("optional unknown tools = %#v, err=%v", tools, err)
	}
	if _, err := registry.Resolve([]appmodel.ToolAuthorization{{ToolID: "unknown", Required: true}}); !errors.Is(err, ErrRequiredUnavailable) {
		t.Fatalf("required unknown tool error = %v", err)
	}
}

func TestMediaReplyToolStoresBoundAttachmentWithoutExposingIt(t *testing.T) {
	store := runtimestorageinmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-a", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-a", SessionID: "session-a", BindingID: "binding-a", ExternalMessageID: "message-a"}); err != nil {
		t.Fatal(err)
	}
	writer := &toolAuditWriter{}
	collector := NewReplyCollector()
	ctx := WithExecutionContext(context.Background(), ExecutionContext{
		TenantID: "tenant-a", EventID: "event-a", RequestID: "request-a", TraceID: "trace-a",
		Attachments: store, Replies: collector, Audit: audit.NewRecorder(writer, "tenant-a"),
	})
	callable := resolveTestImageTool(t)
	queued := callTestImageTool(t, callable, ctx)
	encoded := marshalTestImageResult(t, queued)
	intent := assertQueuedImageIntent(t, collector)
	assertTestImageResultDoesNotExposeAttachment(t, encoded, intent)
	assertToolStoredBoundImage(t, store, intent)
	assertToolReplayIsIdempotent(t, callable, ctx, collector)
	assertToolAuditEvents(t, writer)
}

func resolveTestImageTool(t *testing.T) trpctool.CallableTool {
	t.Helper()
	tools, err := DefaultRegistry().Resolve([]appmodel.ToolAuthorization{{ToolID: SendTestImageID}})
	if err != nil || len(tools) != 1 {
		t.Fatalf("tool resolution = %#v, err=%v", tools, err)
	}
	callable, ok := tools[0].(trpctool.CallableTool)
	if !ok {
		t.Fatal("tool resolution did not return a callable tool")
	}
	return callable
}

func callTestImageTool(t *testing.T, callable trpctool.CallableTool, ctx context.Context) sendTestImageResult {
	t.Helper()
	result, err := callable.Call(ctx, []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	queued, ok := result.(sendTestImageResult)
	if !ok || queued.Status != "queued" {
		t.Fatalf("tool result = %#v", result)
	}
	return queued
}

func marshalTestImageResult(t *testing.T, queued sendTestImageResult) []byte {
	t.Helper()
	encoded, err := json.Marshal(queued)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func assertQueuedImageIntent(t *testing.T, collector *ReplyCollector) ReplyIntent {
	t.Helper()
	intents := collector.Intents()
	if len(intents) != 1 || intents[0].Kind != runtimestorage.ReplyKindImage || intents[0].Fallback != testImageFallback {
		t.Fatalf("reply intents = %#v", intents)
	}
	return intents[0]
}

func assertTestImageResultDoesNotExposeAttachment(t *testing.T, encoded []byte, intent ReplyIntent) {
	t.Helper()
	if bytes.Contains(encoded, []byte(intent.Attachment.ID)) || bytes.Contains(encoded, []byte(intent.Attachment.ProviderID)) {
		t.Fatalf("tool result exposed attachment metadata: %s", encoded)
	}
}

func assertToolStoredBoundImage(t *testing.T, store *runtimestorageinmemory.Store, intent ReplyIntent) {
	t.Helper()
	content, err := store.Load(context.Background(), "tenant-a", "event-a", intent.Attachment)
	if err != nil || !bytes.Equal(content.Data, testImagePNG) {
		t.Fatalf("bound image = %#v, err=%v", content, err)
	}
}

func assertToolReplayIsIdempotent(t *testing.T, callable trpctool.CallableTool, ctx context.Context, collector *ReplyCollector) {
	t.Helper()
	if _, err := callable.Call(ctx, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if len(collector.Intents()) != 1 {
		t.Fatalf("duplicate media intent = %#v", collector.Intents())
	}
}

func assertToolAuditEvents(t *testing.T, writer *toolAuditWriter) {
	t.Helper()
	if len(writer.events) != 4 || writer.events[0].EventType != audit.EventToolAllowed || writer.events[1].EventType != audit.EventToolExecuted || writer.events[0].ToolName != SendTestImageID || writer.events[1].ToolName != SendTestImageID {
		t.Fatalf("tool audit events = %#v", writer.events)
	}
}

func TestMediaReplyToolFailsClosedWithoutDurableContext(t *testing.T) {
	if _, err := sendTestImage(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("missing execution context error = %v", err)
	}
}

func TestMediaReplyToolRetriesWithDurableAudit(t *testing.T) {
	store := runtimestorageinmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-a", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-a", SessionID: "session-a", BindingID: "binding-a", ExternalMessageID: "message-a"}); err != nil {
		t.Fatal(err)
	}
	auditStore, err := audit.NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	var ticks atomic.Int64
	collector := NewReplyCollector()
	ctx := WithExecutionContext(context.Background(), ExecutionContext{
		TenantID: "tenant-a", EventID: "event-a", RequestID: "request-a", TraceID: "trace-a", Attachments: store, Replies: collector,
		Audit: audit.NewRecorder(auditStore, "tenant-a", audit.WithRecorderClock(func() time.Time { return time.Unix(0, ticks.Add(1)).UTC() })),
	})
	callable := resolveTestImageTool(t)
	assertParallelTestImageCallsSucceed(t, callable, ctx)
	if len(collector.Intents()) != 1 {
		t.Fatalf("parallel intents = %#v", collector.Intents())
	}
	events, err := auditStore.List(context.Background(), audit.Query{})
	if err != nil || ticks.Load() != 1 || len(events) != 2 || !sameAuditOccurrence(events) || !hasToolAuditEvents(events) {
		t.Fatalf("parallel audit events = %#v, err=%v", events, err)
	}
}

func assertParallelTestImageCallsSucceed(t *testing.T, callable trpctool.CallableTool, ctx context.Context) {
	t.Helper()
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for index := 0; index < 2; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := callable.Call(ctx, []byte("{}"))
			errs <- err
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("parallel tool call = %v", err)
		}
	}
}

func sameAuditOccurrence(events []audit.Event) bool {
	return len(events) == 2 && events[0].OccurredAt.Equal(events[1].OccurredAt)
}

func hasToolAuditEvents(events []audit.Event) bool {
	types := map[audit.EventType]struct{}{}
	for _, event := range events {
		types[event.EventType] = struct{}{}
	}
	_, allowed := types[audit.EventToolAllowed]
	_, executed := types[audit.EventToolExecuted]
	return allowed && executed
}

type toolAuditWriter struct {
	events []audit.Event
}

func (writer *toolAuditWriter) Append(_ context.Context, event audit.Event) (audit.AppendResult, error) {
	writer.events = append(writer.events, event)
	return audit.AppendResult{Event: event}, nil
}
