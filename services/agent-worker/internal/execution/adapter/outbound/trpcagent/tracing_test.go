package trpcagent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"go.opentelemetry.io/otel/trace"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	sdklog "trpc.group/trpc-go/trpc-agent-go/log"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

const traceCanary = "CANARY private prompt response arguments"

const sdkToolCancelChildEnv = "WORKER_TEST_SDK_TOOL_CANCEL_CHILD"

func sdkToolCancelChild(t *testing.T) bool {
	return os.Getenv(sdkToolCancelChildEnv) == "1" && t.Name() == "TestTracingSDKToolContract/cancel"
}

type traceSink struct {
	mu    sync.Mutex
	spans []*tracepb.Span
}

func newTraceRuntime(t *testing.T) (*telemetrytrace.Runtime, *traceSink) {
	t.Helper()
	sink := &traceSink{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if bytes.Contains(data, []byte(traceCanary)) {
			t.Error("SDK content exported")
		}
		var request collector.ExportTraceServiceRequest
		if err := proto.Unmarshal(data, &request); err != nil {
			t.Error(err)
		}
		sink.mu.Lock()
		defer sink.mu.Unlock()
		for _, rs := range request.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				sink.spans = append(sink.spans, ss.Spans...)
			}
		}
	}))
	t.Cleanup(server.Close)
	cfg := telemetrytrace.Config{TracesEndpoint: server.URL + "/v1/traces", SamplingRatio: 1, ExportTimeout: "1s", BatchTimeout: "1h", MaxQueueSize: 256, MaxExportBatchSize: 64}
	rt, err := telemetrytrace.New(context.Background(), &cfg, telemetrytrace.Identity{Service: "agent-worker", Instance: "sdk-test"})
	if err != nil {
		t.Fatal(err)
	}
	oldProvider, oldTracer := agenttrace.TracerProvider, agenttrace.Tracer
	BindTracing(rt.Provider())
	restoreLog := BindLogging(func(context.Context, string) {})
	// Cancellation can leave the SDK flow running after its public stream and
	// agent span finish. The isolated child keeps globals stable until exit.
	if !sdkToolCancelChild(t) {
		t.Cleanup(restoreLog)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := rt.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if !sdkToolCancelChild(t) {
			agenttrace.TracerProvider, agenttrace.Tracer = oldProvider, oldTracer
			agenttrace.SetSpanAttributePolicy(agenttrace.SpanAttributePolicy{})
		}
	})
	return rt, sink
}
func (s *traceSink) snapshot() []*tracepb.Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*tracepb.Span(nil), s.spans...)
}
func flushTrace(t *testing.T, rt *telemetrytrace.Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestTracingRealSDKParentageAndStreamLifetime(t *testing.T) {
	rt, sink := newTraceRuntime(t)
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-release:
			writeAnswer(w, traceCanary)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	req := testRequest(server.URL)
	req.InputText = traceCanary
	req.Instruction = traceCanary
	e := testExecutor()
	e.Tracer = rt.Tracer("agent-worker/execution-v1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx, parent := rt.Tracer("agent-worker").Start(ctx, "worker.run.attempt")
	done := make(chan error, 1)
	go func() { _, err := e.Execute(ctx, req); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("model never called")
	}
	flushTrace(t, rt)
	for _, s := range sink.snapshot() {
		if s.Name == "worker.runner.run" {
			t.Fatal("Runner span ended before event stream")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	parent.End()
	flushTrace(t, rt)
	spans := sink.snapshot()
	byID := map[string]*tracepb.Span{}
	names := map[string]*tracepb.Span{}
	for _, s := range spans {
		byID[string(s.SpanId)] = s
		names[s.Name] = s
	}
	run, invoke, chat := names["worker.runner.run"], names["invoke_agent root"], names["chat fixture-model"]
	if run == nil || invoke == nil || chat == nil {
		t.Fatalf("missing SDK spans names=%v", names)
	}
	for _, child := range []*tracepb.Span{invoke, chat} {
		if !bytes.Equal(child.TraceId, run.TraceId) {
			t.Fatal("SDK trace disconnected")
		}
		current := child
		found := false
		for i := 0; i < len(spans); i++ {
			if bytes.Equal(current.ParentSpanId, run.SpanId) {
				found = true
				break
			}
			current = byID[string(current.ParentSpanId)]
			if current == nil {
				break
			}
		}
		if !found {
			t.Fatal("SDK span has no Runner ancestor")
		}
	}

	if names["session.overlay.get"] == nil || names["session.overlay.append"] == nil {
		t.Fatal("actual SDK session operations missing")
	}
	aggregates := map[string]int64{}
	for _, a := range run.Attributes {
		aggregates[a.Key] = a.Value.GetIntValue()
	}
	if aggregates["app.session.overlay.appends"] < 1 || aggregates["app.session.overlay.bytes"] < 1 {
		t.Fatal("runner overlay counters missing in real OTLP")
	}
	t.Log("SDK_SESSION_OTLP=PASS actual_sdk=true overlay_spans=true aggregate_counts=true content_filtered=true")
	t.Log("SDK_TRACE=PASS actual_http_model=true runner_agent_llm_parentage=true stream_lifetime=true content_filtered=true")
}

type traceToolModel struct {
	calls    atomic.Int32
	parallel bool
}

func (*traceToolModel) Info() model.Info { return model.Info{Name: "fixture-tool-model"} }
func (m *traceToolModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	call := m.calls.Add(1)
	message := model.NewAssistantMessage(traceCanary)
	if call == 1 {
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "call-1", Type: "function", Function: model.FunctionDefinitionParam{Name: "lookup", Arguments: []byte(`{"input":"CANARY private prompt response arguments"}`)}}}}
	}
	if call == 1 && m.parallel {
		second := message.ToolCalls[0]
		second.ID = "call-2"
		message.ToolCalls = append(message.ToolCalls, second)
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{ID: "fixture-response", Done: true, Choices: []model.Choice{{Message: message}}}
	close(ch)
	return ch, nil
}
func TestTracingSDKToolContract(t *testing.T) {
	for _, mode := range []string{"ok", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "cancel" && !sdkToolCancelChild(t) {
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				// Reuse the current binary, including race instrumentation. Only the
				// cancellation case runs in the child; no SDK globals change here.
				cmd := exec.CommandContext(ctx, binary, "-test.run=^TestTracingSDKToolContract$/^cancel$", "-test.count=1", "-test.timeout=20s", "-test.v")
				cmd.Env = append(os.Environ(), sdkToolCancelChildEnv+"=1")
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("isolated SDK cancellation failed: %v\n%s", err, output)
				}
				if !bytes.Contains(output, []byte("SDK_TOOL_TRACE=PASS mode=cancel")) {
					t.Fatalf("isolated SDK cancellation did not execute assertions:\n%s", output)
				}
				t.Logf("isolated SDK cancellation passed:\n%s", output)
				return
			}
			rt, sink := newTraceRuntime(t)
			type input struct {
				Input string `json:"input"`
			}
			type output struct {
				Result string `json:"result"`
			}
			entered := make(chan struct{}, 1)
			fn := function.NewFunctionTool(func(ctx context.Context, in input) (output, error) {
				entered <- struct{}{}
				switch mode {
				case "error":
					return output{}, errors.New(traceCanary)
				case "cancel":
					<-ctx.Done()
					return output{}, ctx.Err()
				}
				return output{Result: traceCanary}, nil
			}, function.WithName("lookup"), function.WithDescription(traceCanary))
			ag := llmagent.New("tool-fixture", llmagent.WithModel(&traceToolModel{}), llmagent.WithTools([]tool.Tool{fn}))
			run := runner.NewRunner("trace-fixture", ag, runner.WithMemoryService(nil))
			defer run.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			ctx, span := rt.Tracer("agent-worker").Start(ctx, "worker.runner.run")
			events, err := run.Run(ctx, "user-1", "session-1", model.NewUserMessage(traceCanary), agent.WithDetachedCancel(false))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" {
				select {
				case <-entered:
					cancel()
				case <-ctx.Done():
					t.Fatal("tool not entered")
				}
			}
			for range events {
			}
			span.End()
			// Cancellation can close Runner's public stream before the Tool goroutine's
			// deferred span.End. Wait for the actual SDK span; do not fabricate one.
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				flushTrace(t, rt)
				observed := false
				spans := sink.snapshot()
				for _, s := range spans {
					if strings.HasPrefix(s.Name, "execute_tool lookup") && hasSpanAncestor(spans, s, span.SpanContext().SpanID()) {
						observed = true
					}
				}
				if observed {
					break
				}
				select {
				case <-ticker.C:
				case <-deadline.C:
					t.Fatal("actual SDK Tool span or Runner ancestry did not finish")
				}
			}
			found := false
			spans := sink.snapshot()
			for _, s := range spans {
				if strings.HasPrefix(s.Name, "execute_tool lookup") {
					found = true
					traceID := trace.SpanContextFromContext(ctx).TraceID()
					if !bytes.Equal(s.TraceId, traceID[:]) || !hasSpanAncestor(spans, s, span.SpanContext().SpanID()) {
						t.Fatal("Tool has no actual Runner ancestor")
					}
					if s.Status != nil && s.Status.Message != "" {
						t.Fatal("raw Tool status")
					}
					if mode != "ok" && (s.Status == nil || s.Status.Code != tracepb.Status_STATUS_CODE_ERROR) {
						t.Fatalf("Tool %s status not error: %v", mode, s.Status)
					}
				}
			}
			if !found {
				t.Fatalf("missing actual tool span mode=%s", mode)
			}
			t.Logf("SDK_TOOL_TRACE=PASS mode=%s actual_tool=true runner_ancestor=true content_filtered=true", mode)
		})
	}
}

type logCanary struct{}

func (logCanary) String() string { panic("SDK logging must not format private arguments") }
func TestSDKLoggingDropsFreeTextAtSource(t *testing.T) {
	var levels []string
	restore := BindLogging(func(_ context.Context, level string) { levels = append(levels, level) })
	defer restore()
	sdklog.Errorf("secret format %v", logCanary{})
	sdklog.WarnContext(context.Background(), logCanary{})
	sdklog.Infof("private %v", logCanary{})
	if strings.Join(levels, ",") != "error,warn,info" {
		t.Fatal(levels)
	}
}

// hasSpanAncestor checks actual parent edges, not merely matching trace IDs.
// A bounded walk rejects orphaned chains and cycles in malformed evidence.
func hasSpanAncestor(spans []*tracepb.Span, child *tracepb.Span, ancestor trace.SpanID) bool {
	byID := make(map[string]*tracepb.Span, len(spans))
	for _, span := range spans {
		byID[string(span.SpanId)] = span
	}
	for i := 0; i < len(spans); i++ {
		parent := byID[string(child.ParentSpanId)]
		if parent == nil || !bytes.Equal(parent.TraceId, child.TraceId) {
			return false
		}
		if bytes.Equal(parent.SpanId, ancestor[:]) {
			return true
		}
		child = parent
	}
	return false
}

func TestToolAncestryRejectsSameTraceOrphanAndCycle(t *testing.T) {
	ancestor := trace.SpanID{1}
	root := &tracepb.Span{SpanId: ancestor[:], TraceId: []byte{9}}
	child := &tracepb.Span{SpanId: []byte{2}, ParentSpanId: ancestor[:], TraceId: []byte{9}}
	if !hasSpanAncestor([]*tracepb.Span{root, child}, child, ancestor) {
		t.Fatal("actual parent edge rejected")
	}
	child.ParentSpanId = []byte{3}
	if hasSpanAncestor([]*tracepb.Span{root, child}, child, ancestor) {
		t.Fatal("same trace orphan accepted")
	}
	child.ParentSpanId = child.SpanId
	if hasSpanAncestor([]*tracepb.Span{root, child}, child, ancestor) {
		t.Fatal("parent cycle accepted")
	}
	child.ParentSpanId = ancestor[:]
	child.TraceId = []byte{8}
	if hasSpanAncestor([]*tracepb.Span{root, child}, child, ancestor) {
		t.Fatal("cross trace parent accepted")
	}
}

func TestTracingSDKParallelToolParentage(t *testing.T) {
	rt, sink := newTraceRuntime(t)
	type input struct {
		Input string `json:"input"`
	}
	type output struct {
		Result string `json:"result"`
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var active atomic.Int32
	fn := function.NewFunctionTool(func(ctx context.Context, _ input) (output, error) {
		active.Add(1)
		defer active.Add(-1)
		entered <- struct{}{}
		select {
		case <-release:
			return output{Result: traceCanary}, nil
		case <-ctx.Done():
			return output{}, ctx.Err()
		}
	}, function.WithName("lookup"), function.WithDescription(traceCanary))
	ag := llmagent.New("parallel-tool-fixture", llmagent.WithModel(&traceToolModel{parallel: true}), llmagent.WithTools([]tool.Tool{fn}), llmagent.WithEnableParallelTools(true))
	run := runner.NewRunner("trace-fixture", ag, runner.WithMemoryService(nil))
	defer run.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx, parent := rt.Tracer("agent-worker").Start(ctx, "worker.runner.run")
	events, err := run.Run(ctx, "user-1", "session-1", model.NewUserMessage(traceCanary), agent.WithDetachedCancel(false))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for range events {
		}
		close(done)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("SDK did not start both Tool calls concurrently")
		}
	}
	if active.Load() != 2 {
		t.Fatal("Tool overlap not observed")
	}
	unblock()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("parallel Tool stream did not finish")
	}
	parent.End()
	flushTrace(t, rt)
	spans := sink.snapshot()
	var calls []*tracepb.Span
	for _, s := range spans {
		if strings.HasPrefix(s.Name, "execute_tool lookup") {
			if !hasSpanAncestor(spans, s, parent.SpanContext().SpanID()) {
				t.Fatal("parallel Tool has no Runner ancestor")
			}
			calls = append(calls, s)
		}
	}
	if len(calls) != 2 || bytes.Equal(calls[0].SpanId, calls[1].SpanId) || !bytes.Equal(calls[0].ParentSpanId, calls[1].ParentSpanId) {
		t.Fatal("parallel Tool spans are not distinct siblings")
	}
	if calls[0].StartTimeUnixNano >= calls[1].EndTimeUnixNano || calls[1].StartTimeUnixNano >= calls[0].EndTimeUnixNano {
		t.Fatal("actual parallel Span intervals do not overlap")
	}
	t.Log("SDK_PARALLEL_TOOL_TRACE=PASS actual_overlap=true distinct_siblings=true runner_ancestor=true content_filtered=true")
}
