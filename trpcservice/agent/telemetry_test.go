package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestTelemetryModelClosesStreamingOperationOnce(t *testing.T) {
	provider := &runtimeTelemetryProvider{}
	options := applyTelemetryOptions(t, provider)
	var samples []budget.Usage
	ctx := WithUsageObserver(context.Background(), func(_ context.Context, usage budget.Usage) {
		samples = append(samples, usage)
	})
	before, err := options.ModelCallbacks.RunBeforeModel(ctx, &trpcmodel.BeforeModelArgs{})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := wrapTelemetryModel(streamingModel{responses: []*trpcmodel.Response{
		{IsPartial: true, Usage: &trpcmodel.Usage{PromptTokens: 1, CompletionTokens: 2}},
		{Done: true, Usage: &trpcmodel.Usage{PromptTokens: 5, CompletionTokens: 7}},
	}})
	responses, err := wrapped.GenerateContent(before.Context, &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}
	if _, err := options.ModelCallbacks.RunAfterModel(before.Context, &trpcmodel.AfterModelArgs{Response: &trpcmodel.Response{Done: true}}); err != nil {
		t.Fatal(err)
	}
	if len(provider.spans) != 1 || provider.spans[0].status != observability.StatusOK || !provider.spans[0].ended {
		t.Fatalf("stream span = %+v", provider.spans)
	}
	assertTelemetryMetric(t, provider, metrics.TokensTotal, 5, map[string]string{"component": "model", "provider": "openai", "model_family": "gpt"})
	assertTelemetryMetric(t, provider, metrics.TokensTotal, 7, map[string]string{"component": "model", "provider": "openai", "model_family": "gpt"})
	if len(samples) != 1 || samples[0] != (budget.Usage{InputTokens: 5, OutputTokens: 7}) {
		t.Fatalf("usage observer samples = %+v; want one terminal sample", samples)
	}
	if countTelemetryMetrics(provider, metrics.OperationDuration) != 1 || countTelemetryMetrics(provider, metrics.RequestsTotal) != 2 {
		t.Fatalf("terminal model metrics = %#v", provider.metrics)
	}
}

func TestTelemetryModelMarksEarlyStreamCloseAndGenerationError(t *testing.T) {
	for _, test := range []struct {
		name  string
		model trpcmodel.Model
	}{
		{name: "early close", model: streamingModel{responses: []*trpcmodel.Response{{IsPartial: true}}}},
		{name: "generation error", model: streamingModel{err: errors.New("provider failure")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &runtimeTelemetryProvider{}
			options := applyTelemetryOptions(t, provider)
			before, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
			if err != nil {
				t.Fatal(err)
			}
			responses, generationErr := wrapTelemetryModel(test.model).GenerateContent(before.Context, &trpcmodel.Request{})
			if test.name == "generation error" {
				if generationErr == nil || responses != nil {
					t.Fatalf("generation result = %v/%v", responses, generationErr)
				}
			} else {
				if generationErr != nil {
					t.Fatal(generationErr)
				}
				for range responses {
				}
			}
			if len(provider.spans) != 1 || provider.spans[0].status != observability.StatusError || !provider.spans[0].ended || provider.spans[0].recordedError == nil {
				t.Fatalf("failed stream span = %+v", provider.spans)
			}
		})
	}
}

func TestTelemetryIterModelClosesOperationForTerminalAndIncompleteSequences(t *testing.T) {
	tests := []struct {
		name       string
		responses  []*trpcmodel.Response
		cancel     bool
		wantStatus observability.Status
		wantError  bool
	}{
		{name: "terminal response", responses: []*trpcmodel.Response{{Done: true, Usage: &trpcmodel.Usage{PromptTokens: 3, CompletionTokens: 4}}}, wantStatus: observability.StatusOK},
		{name: "incomplete sequence", responses: []*trpcmodel.Response{{IsPartial: true}}, wantStatus: observability.StatusError, wantError: true},
		{name: "canceled sequence", cancel: true, wantStatus: observability.StatusError, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &runtimeTelemetryProvider{}
			options := applyTelemetryOptions(t, provider)
			before, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := before.Context
			var cancel context.CancelFunc
			if test.cancel {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			wrapped := wrapTelemetryModel(telemetryIterTestModel{responses: test.responses})
			iterModel, ok := wrapped.(trpcmodel.IterModel)
			if !ok {
				t.Fatal("wrapped model does not implement IterModel")
			}
			seq, err := iterModel.GenerateContentIter(ctx, &trpcmodel.Request{})
			if err != nil {
				t.Fatal(err)
			}
			seq(func(*trpcmodel.Response) bool { return true })
			if len(provider.spans) != 1 || provider.spans[0].status != test.wantStatus || !provider.spans[0].ended || (provider.spans[0].recordedError != nil) != test.wantError {
				t.Fatalf("iter span = %+v", provider.spans)
			}
		})
	}
}

func TestTelemetryIterModelHandlesCreationFailures(t *testing.T) {
	for _, test := range []struct {
		name  string
		model telemetryIterTestModel
		want  error
	}{
		{name: "generation error", model: telemetryIterTestModel{err: errors.New("provider failure")}, want: errors.New("provider failure")},
		{name: "nil sequence", model: telemetryIterTestModel{nilSeq: true}, want: errModelResponseIncomplete},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &runtimeTelemetryProvider{}
			options := applyTelemetryOptions(t, provider)
			before, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
			if err != nil {
				t.Fatal(err)
			}
			wrapped := wrapTelemetryModel(test.model)
			iterModel, ok := wrapped.(trpcmodel.IterModel)
			if !ok {
				t.Fatal("wrapped model does not implement IterModel")
			}
			seq, gotErr := iterModel.GenerateContentIter(before.Context, &trpcmodel.Request{})
			if seq != nil || gotErr == nil || gotErr.Error() != test.want.Error() {
				t.Fatalf("iter result = %v, %v; want error %v", seq, gotErr, test.want)
			}
			if len(provider.spans) != 1 || provider.spans[0].status != observability.StatusError || !provider.spans[0].ended || provider.spans[0].recordedError == nil {
				t.Fatalf("failed iter span = %+v", provider.spans)
			}
		})
	}
}

func TestTelemetryModelWithModelRetryCallbacksDelegatesWhenSupported(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "input")
	before := func(context.Context, *trpcmodel.Request) (context.Context, *trpcmodel.Response, error) {
		return nil, nil, nil
	}
	after := func(context.Context, *trpcmodel.Request, *trpcmodel.Response) (context.Context, error) {
		return nil, nil
	}
	binder := &telemetryRetryBinderModel{result: context.WithValue(ctx, struct{}{}, "bound")}
	model := telemetryModel{delegate: binder}
	if got := model.WithModelRetryCallbacks(ctx, before, after); got != binder.result || binder.before == nil || binder.after == nil {
		t.Fatalf("retry callback binding = %v, before set=%t, after set=%t", got, binder.before != nil, binder.after != nil)
	}
	if got := (telemetryModel{delegate: streamingModel{}}).WithModelRetryCallbacks(ctx, before, after); got != ctx {
		t.Fatalf("unsupported retry callback binding = %v, want original context", got)
	}
}

type telemetryIterTestModel struct {
	responses []*trpcmodel.Response
	err       error
	nilSeq    bool
}

func (model telemetryIterTestModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "iter-test"} }

func (model telemetryIterTestModel) GenerateContent(context.Context, *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	return nil, nil
}

func (model telemetryIterTestModel) GenerateContentIter(context.Context, *trpcmodel.Request) (trpcmodel.Seq[*trpcmodel.Response], error) {
	if model.err != nil {
		return nil, model.err
	}
	if model.nilSeq {
		return nil, nil
	}
	return func(yield func(*trpcmodel.Response) bool) {
		for _, response := range model.responses {
			if !yield(response) {
				return
			}
		}
	}, nil
}

type telemetryRetryBinderModel struct {
	result context.Context
	before func(context.Context, *trpcmodel.Request) (context.Context, *trpcmodel.Response, error)
	after  func(context.Context, *trpcmodel.Request, *trpcmodel.Response) (context.Context, error)
}

func (model *telemetryRetryBinderModel) Info() trpcmodel.Info {
	return trpcmodel.Info{Name: "retry-test"}
}

func (model *telemetryRetryBinderModel) GenerateContent(context.Context, *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	return nil, nil
}

func (model *telemetryRetryBinderModel) WithModelRetryCallbacks(ctx context.Context, before func(context.Context, *trpcmodel.Request) (context.Context, *trpcmodel.Response, error), after func(context.Context, *trpcmodel.Request, *trpcmodel.Response) (context.Context, error)) context.Context {
	model.before, model.after = before, after
	if model.result == nil {
		return ctx
	}
	return model.result
}

func countTelemetryMetrics(provider *runtimeTelemetryProvider, name string) int {
	count := 0
	for _, metric := range provider.metrics {
		if metric.name == name {
			count++
		}
	}
	return count
}

type streamingModel struct {
	responses []*trpcmodel.Response
	err       error
}

func (model streamingModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "test"} }

func (model streamingModel) GenerateContent(context.Context, *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	if model.err != nil {
		return nil, model.err
	}
	responses := make(chan *trpcmodel.Response, len(model.responses))
	for _, response := range model.responses {
		responses <- response
	}
	close(responses)
	return responses, nil
}

//nolint:gocyclo // Table-like callback contract coverage intentionally exercises all telemetry outcomes.
func TestTelemetryOptionsRecordsModelAndToolOutcomes(t *testing.T) {
	t.Run("model success records usage", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		before, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
		if err != nil || before == nil || before.Context == nil {
			t.Fatalf("before model = %#v, %v", before, err)
		}
		if _, err := options.ModelCallbacks.RunAfterModel(before.Context, &trpcmodel.AfterModelArgs{Response: &trpcmodel.Response{Usage: &trpcmodel.Usage{PromptTokens: 7, CompletionTokens: 11}}}); err != nil {
			t.Fatal(err)
		}

		assertTelemetrySpan(t, provider, observability.OperationModelCall, observability.StatusOK, false)
		assertTelemetryMetric(t, provider, metrics.RequestsTotal, 1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt", "status": "started"})
		assertTelemetryMetric(t, provider, metrics.RequestsTotal, 1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt", "status": "success", "error_class": ""})
		assertTelemetryMetric(t, provider, metrics.TokensTotal, 7, map[string]string{"component": "model", "provider": "openai", "model_family": "gpt"})
		assertTelemetryMetric(t, provider, metrics.TokensTotal, 11, map[string]string{"component": "model", "provider": "openai", "model_family": "gpt"})
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt", "status": "success", "error_class": ""})
	})

	t.Run("model error records status", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		before, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := options.ModelCallbacks.RunAfterModel(before.Context, &trpcmodel.AfterModelArgs{Error: context.Canceled}); err != nil {
			t.Fatal(err)
		}
		assertTelemetrySpan(t, provider, observability.OperationModelCall, observability.StatusError, true)
		assertTelemetryMetric(t, provider, metrics.RequestsTotal, 1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt", "status": "canceled", "error_class": "canceled"})
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt", "status": "canceled", "error_class": "canceled"})
	})

	t.Run("tool success and error record outcomes", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		before, err := options.ToolCallbacks.RunBeforeTool(context.Background(), &trpctool.BeforeToolArgs{ToolName: "lookup"})
		if err != nil || before == nil || before.Context == nil {
			t.Fatalf("before tool = %#v, %v", before, err)
		}
		if _, err := options.ToolCallbacks.RunAfterTool(before.Context, &trpctool.AfterToolArgs{}); err != nil {
			t.Fatal(err)
		}
		before, err = options.ToolCallbacks.RunBeforeTool(context.Background(), &trpctool.BeforeToolArgs{ToolName: "lookup"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := options.ToolCallbacks.RunAfterTool(before.Context, &trpctool.AfterToolArgs{Error: context.DeadlineExceeded}); err != nil {
			t.Fatal(err)
		}

		if len(provider.spans) != 2 {
			t.Fatalf("tool spans = %d, want 2", len(provider.spans))
		}
		if provider.spans[0].status != observability.StatusOK || !provider.spans[0].ended || provider.spans[0].recordedError != nil {
			t.Fatalf("success tool span = %+v", provider.spans[0])
		}
		if provider.spans[1].status != observability.StatusError || !provider.spans[1].ended || provider.spans[1].recordedError == nil {
			t.Fatalf("error tool span = %+v", provider.spans[1])
		}
		assertTelemetryMetric(t, provider, metrics.RequestsTotal, 1, map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "started"})
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "success", "error_class": ""})
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "timeout", "error_class": "timeout"})
	})

	t.Run("nil callback args are ignored safely", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		modelBefore, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := options.ModelCallbacks.RunAfterModel(modelBefore.Context, nil); err != nil {
			t.Fatal(err)
		}
		if len(provider.spans) != 1 {
			t.Fatalf("spans = %d, want 1", len(provider.spans))
		}
		if span := provider.spans[0]; span.status != observability.StatusError || !span.ended || span.recordedError == nil {
			t.Fatalf("nil args span = %+v", span)
		}
	})
}

func applyTelemetryOptions(t *testing.T, provider observability.Provider) llmagent.Options {
	t.Helper()
	options := llmagent.Options{}
	for _, option := range telemetryOptions(provider, "openai", "gpt-family") {
		option(&options)
	}
	if options.ModelCallbacks == nil || options.ToolCallbacks == nil {
		t.Fatalf("telemetry callbacks = model %v, tool %v", options.ModelCallbacks, options.ToolCallbacks)
	}
	return options
}

func assertTelemetrySpan(t *testing.T, provider *runtimeTelemetryProvider, operation string, status observability.Status, wantError bool) {
	t.Helper()
	if len(provider.spans) == 0 {
		t.Fatal("no telemetry span recorded")
	}
	span := provider.spans[len(provider.spans)-1]
	if span.name != operation || span.status != status || !span.ended || (span.recordedError != nil) != wantError {
		t.Fatalf("span = %+v, want operation=%q status=%q error=%v", span, operation, status, wantError)
	}
}

func assertTelemetryMetric(t *testing.T, provider *runtimeTelemetryProvider, name string, wantValue float64, wantLabels map[string]string) {
	t.Helper()
	for _, metric := range provider.metrics {
		if metric.name == name && (wantValue < 0 || metric.value == wantValue) && telemetryLabelsEqual(metric.attrs, wantLabels) {
			return
		}
	}
	t.Fatalf("metric %q with value %v and labels %#v not found in %#v", name, wantValue, wantLabels, provider.metrics)
}

func telemetryLabelsEqual(attrs []observability.Attribute, want map[string]string) bool {
	if len(attrs) != len(want) {
		return false
	}
	for _, attr := range attrs {
		expected, ok := want[attr.Key]
		if !ok || expected != attr.Value {
			return false
		}
	}
	return true
}

func TestTelemetryLabelsEqualRejectsUnknownEmptyValue(t *testing.T) {
	attrs := []observability.Attribute{{Key: "unexpected", Value: ""}}
	if telemetryLabelsEqual(attrs, map[string]string{"expected": ""}) {
		t.Fatal("unknown empty-valued label was accepted")
	}
}

type runtimeTelemetryProvider struct {
	spans   []*runtimeTelemetrySpan
	metrics []runtimeTelemetryMetric
}

func (p *runtimeTelemetryProvider) Tracer(string) observability.Tracer {
	return runtimeTelemetryTracer{provider: p}
}
func (p *runtimeTelemetryProvider) Meter(string) observability.Meter {
	return runtimeTelemetryMeter{provider: p}
}
func (p *runtimeTelemetryProvider) Logger() observability.Logger   { return runtimeTelemetryLogger{} }
func (p *runtimeTelemetryProvider) Shutdown(context.Context) error { return nil }

type runtimeTelemetryTracer struct{ provider *runtimeTelemetryProvider }

func (t runtimeTelemetryTracer) Start(ctx context.Context, name string, attrs ...observability.Attribute) (context.Context, observability.Span) {
	span := &runtimeTelemetrySpan{name: name, attrs: append([]observability.Attribute(nil), attrs...)}
	t.provider.spans = append(t.provider.spans, span)
	return ctx, span
}

type runtimeTelemetrySpan struct {
	name          string
	attrs         []observability.Attribute
	status        observability.Status
	recordedError error
	ended         bool
}

func (s *runtimeTelemetrySpan) End() { s.ended = true }
func (s *runtimeTelemetrySpan) SetAttributes(attrs ...observability.Attribute) {
	s.attrs = append(s.attrs, attrs...)
}
func (s *runtimeTelemetrySpan) SetStatus(status observability.Status, _ string) { s.status = status }
func (s *runtimeTelemetrySpan) RecordError(err error)                           { s.recordedError = err }

type runtimeTelemetryMeter struct{ provider *runtimeTelemetryProvider }

func (m runtimeTelemetryMeter) Counter(name string) observability.Counter {
	return runtimeTelemetryCounter{provider: m.provider, name: name}
}
func (m runtimeTelemetryMeter) Histogram(name string) observability.Histogram {
	return runtimeTelemetryHistogram{provider: m.provider, name: name}
}
func (m runtimeTelemetryMeter) UpDownCounter(string) observability.UpDownCounter {
	return runtimeTelemetryUpDownCounter{}
}

type runtimeTelemetryMetric struct {
	name  string
	value float64
	attrs []observability.Attribute
}

type runtimeTelemetryCounter struct {
	provider *runtimeTelemetryProvider
	name     string
}

func (c runtimeTelemetryCounter) Add(_ context.Context, value int64, attrs ...observability.Attribute) {
	c.provider.metrics = append(c.provider.metrics, runtimeTelemetryMetric{name: c.name, value: float64(value), attrs: append([]observability.Attribute(nil), attrs...)})
}

type runtimeTelemetryHistogram struct {
	provider *runtimeTelemetryProvider
	name     string
}

func (h runtimeTelemetryHistogram) Record(_ context.Context, value float64, attrs ...observability.Attribute) {
	h.provider.metrics = append(h.provider.metrics, runtimeTelemetryMetric{name: h.name, value: value, attrs: append([]observability.Attribute(nil), attrs...)})
}

type runtimeTelemetryUpDownCounter struct{}

func (runtimeTelemetryUpDownCounter) Add(context.Context, int64, ...observability.Attribute) {}

type runtimeTelemetryLogger struct{}

func (runtimeTelemetryLogger) Log(context.Context, observability.Level, string, ...observability.Attribute) {
}
