package agent

import (
	"context"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestTelemetryCallbacksCloseModelAndToolOperations(t *testing.T) {
	provider := &recordingTelemetry{}
	modelCallbacks := withTelemetryModelCallbacks(nil, provider)
	before, err := modelCallbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{Request: &model.Request{Messages: []model.Message{{Role: model.RoleUser, Content: "hello"}}}})
	if err != nil || before == nil || before.Context == nil {
		t.Fatalf("before model result=%#v err=%v", before, err)
	}
	if _, err := modelCallbacks.RunAfterModel(before.Context, &model.AfterModelArgs{}); err != nil {
		t.Fatal(err)
	}
	toolCallbacks := withTelemetryToolCallbacks(nil, provider)
	toolBefore, err := toolCallbacks.RunBeforeTool(context.Background(), &tool.BeforeToolArgs{ToolCallID: "call", ToolName: "safe"})
	if err != nil || toolBefore == nil || toolBefore.Context == nil {
		t.Fatalf("before tool result=%#v err=%v", toolBefore, err)
	}
	if _, err := toolCallbacks.RunAfterTool(toolBefore.Context, &tool.AfterToolArgs{ToolCallID: "call", ToolName: "safe"}); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.operations) != 2 || provider.operations[0] != telemetry.OperationModelGenerate || provider.operations[1] != telemetry.OperationToolExecute {
		t.Fatalf("operations=%v", provider.operations)
	}
	if provider.ends != 2 {
		t.Fatalf("ended spans=%d", provider.ends)
	}
}

func TestFinishToolOperationIsIdempotentForPermissionShortCircuit(t *testing.T) {
	provider := &recordingTelemetry{}
	callbacks := withTelemetryToolCallbacks(nil, provider)
	before, err := callbacks.RunBeforeTool(context.Background(), &tool.BeforeToolArgs{ToolCallID: "call", ToolName: "safe"})
	if err != nil || before == nil || !FinishToolOperation(before.Context, context.Canceled) || FinishToolOperation(context.Background(), nil) {
		t.Fatalf("before=%#v err=%v", before, err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.ends != 1 {
		t.Fatalf("ended spans=%d", provider.ends)
	}
}

func TestInstrumentedModelAndToolFinishFunctionErrors(t *testing.T) {
	provider := &recordingTelemetry{}
	modelValue := instrumentModel(provider, errorModel{})
	if _, err := modelValue.GenerateContent(context.Background(), &model.Request{Messages: []model.Message{{Role: model.RoleUser, Content: "hello"}}}); err == nil {
		t.Fatal("expected model error")
	}
	toolValue := instrumentedCallable{inner: errorCallable{}, provider: provider}
	if _, err := toolValue.Call(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("expected tool error")
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.operations) != 2 || provider.operations[0] != telemetry.OperationModelGenerate || provider.operations[1] != telemetry.OperationToolExecute || provider.ends != 2 {
		t.Fatalf("operations=%v ends=%d", provider.operations, provider.ends)
	}
}

type recordingTelemetry struct {
	mu         sync.Mutex
	operations []telemetry.Operation
	ends       int
}

func (r *recordingTelemetry) StartSpan(ctx context.Context, operation telemetry.Operation, _ ...telemetry.Attribute) (context.Context, telemetry.Span) {
	r.mu.Lock()
	r.operations = append(r.operations, operation)
	r.mu.Unlock()
	return ctx, recordingSpan{owner: r}
}
func (*recordingTelemetry) Counter(telemetry.MetricDescriptor) telemetry.Counter {
	return recordingCounter{}
}
func (*recordingTelemetry) Histogram(telemetry.MetricDescriptor) telemetry.Histogram {
	return recordingHistogram{}
}
func (*recordingTelemetry) Logger(telemetry.Component) telemetry.Logger { return recordingLogger{} }
func (*recordingTelemetry) Shutdown(context.Context) error              { return nil }

type recordingSpan struct{ owner *recordingTelemetry }

func (s recordingSpan) End(error) {
	s.owner.mu.Lock()
	s.owner.ends++
	s.owner.mu.Unlock()
}

type recordingCounter struct{}

func (recordingCounter) Add(context.Context, int64, ...telemetry.Attribute) {}

type recordingHistogram struct{}

func (recordingHistogram) Record(context.Context, float64, ...telemetry.Attribute) {}

type recordingLogger struct{}

func (recordingLogger) Info(context.Context, telemetry.LogEvent, ...telemetry.Attribute)  {}
func (recordingLogger) Error(context.Context, telemetry.LogEvent, ...telemetry.Attribute) {}

type errorModel struct{}

func (errorModel) Info() model.Info { return model.Info{Name: "error"} }
func (errorModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	return nil, context.Canceled
}

type errorCallable struct{}

func (errorCallable) Declaration() *tool.Declaration            { return &tool.Declaration{Name: "safe"} }
func (errorCallable) Call(context.Context, []byte) (any, error) { return nil, context.Canceled }
func (errorCallable) GovernanceToolRef() governance.VersionedRef {
	return governance.VersionedRef{ID: "safe", Version: 1}
}

var _ model.Model = errorModel{}
var _ tool.CallableTool = errorCallable{}
var _ governance.VersionedTool = errorCallable{}
