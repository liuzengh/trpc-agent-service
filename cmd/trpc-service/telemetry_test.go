package main

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	agentmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
	agentsemconv "trpc.group/trpc-go/trpc-agent-go/telemetry/semconv/metrics"
)

func TestComposeTelemetryInitializesFrameworkMetricsAndPrometheus(t *testing.T) {
	runtime, err := composeTelemetry(context.Background(), mapEnvironment(map[string]string{
		"PROMETHEUS_ENABLED": "true",
	}))
	if err != nil {
		t.Fatalf("composeTelemetry() error = %v", err)
	}
	defer runtime.Close()

	if runtime.PrometheusHandler == nil {
		t.Fatal("Prometheus handler is nil")
	}
	if err := agentmetric.SetHistogramBuckets(
		agentsemconv.MeterNameChat,
		agentsemconv.MetricTRPCAgentGoClientTimeToFirstToken,
		[]float64{0.1, 0.5, 1, 5},
	); err != nil {
		t.Fatalf("framework GenAI metrics were not initialized: %v", err)
	}

	ctx, finish := runtime.Observer.StartExecution(context.Background(), metrics.ExecutionAttributes{
		TenantID: "tenant-a", AppCode: "support", Channel: "web", ConfigVersion: 1,
	})
	_ = ctx
	finish(nil)

	recorder := httptest.NewRecorder()
	runtime.PrometheusHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("Prometheus status = %d, want 200", recorder.Code)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "agent_execution") {
		t.Fatalf("Prometheus output does not contain platform execution metrics: %s", body)
	}
}

type telemetryStreamingModel struct{}

func (*telemetryStreamingModel) Info() model.Info {
	return model.Info{Name: "telemetry-streaming-model", ContextWindow: 4096}
}

func (*telemetryStreamingModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	responses := make(chan *model.Response, 2)
	go func() {
		defer close(responses)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Millisecond):
		}
		responses <- &model.Response{
			Object:    model.ObjectTypeChatCompletionChunk,
			Model:     "telemetry-streaming-model",
			IsPartial: true,
			Choices: []model.Choice{{
				Index: 0,
				Delta: model.Message{Role: model.RoleAssistant, Content: "hello "},
			}},
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Millisecond):
		}
		responses <- &model.Response{
			Object: model.ObjectTypeChatCompletion,
			Model:  "telemetry-streaming-model",
			Choices: []model.Choice{{
				Index:        0,
				Message:      model.NewAssistantMessage("hello world"),
				FinishReason: model.StringPtr("stop"),
			}},
			Usage:     &model.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
			Done:      true,
			IsPartial: false,
		}
	}()
	return responses, nil
}

func TestPrometheusExportsFrameworkTTFTAndDecodeRate(t *testing.T) {
	telemetry, err := composeTelemetry(context.Background(), mapEnvironment(map[string]string{
		"PROMETHEUS_ENABLED": "true",
	}))
	if err != nil {
		t.Fatalf("composeTelemetry() error = %v", err)
	}
	defer telemetry.Close()

	agent := llmagent.New("assistant",
		llmagent.WithModel(&telemetryStreamingModel{}),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: true}),
	)
	runnerInstance := runner.NewRunner("telemetry-test", agent)
	defer runnerInstance.Close()
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}

	recorder := httptest.NewRecorder()
	telemetry.PrometheusHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, metricName := range []string{
		"time_to_first_token",
		"time_per_output_token",
		"output_token_per_time",
	} {
		if !strings.Contains(body, metricName) {
			t.Fatalf("Prometheus output missing framework metric %q: %s", metricName, body)
		}
	}
}

func TestComposeTelemetryRejectsPartialLangfuseConfiguration(t *testing.T) {
	_, err := composeTelemetry(context.Background(), mapEnvironment(map[string]string{
		"LANGFUSE_PUBLIC_KEY": "pk-test",
	}))
	if err == nil || !strings.Contains(err.Error(), "must be configured together") {
		t.Fatalf("composeTelemetry() error = %v, want incomplete Langfuse configuration error", err)
	}
}

func TestComposeTelemetryExportsFrameworkSpansToLangfuse(t *testing.T) {
	type receivedRequest struct {
		path          string
		authorization string
		body          []byte
	}
	received := make(chan receivedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		received <- receivedRequest{
			path:          request.URL.Path,
			authorization: request.Header.Get("Authorization"),
			body:          body,
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	runtime, err := composeTelemetry(context.Background(), mapEnvironment(map[string]string{
		"LANGFUSE_PUBLIC_KEY": "pk-test",
		"LANGFUSE_SECRET_KEY": "sk-test",
		"LANGFUSE_HOST":       strings.TrimPrefix(server.URL, "http://"),
		"LANGFUSE_INSECURE":   "true",
	}))
	if err != nil {
		t.Fatalf("composeTelemetry() error = %v", err)
	}

	agent := llmagent.New("assistant",
		llmagent.WithModel(&telemetryStreamingModel{}),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: true}),
	)
	runnerInstance := runner.NewRunner("langfuse-test", agent)
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("private prompt"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	_ = runnerInstance.Close()
	runtime.Close()

	request := <-received
	if request.path != "/api/public/otel/v1/traces" {
		t.Fatalf("Langfuse path = %q", request.path)
	}
	wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("pk-test:sk-test"))
	if request.authorization != wantAuthorization {
		t.Fatalf("Langfuse Authorization = %q, want %q", request.authorization, wantAuthorization)
	}
	if len(request.body) == 0 {
		t.Fatal("Langfuse request body is empty")
	}
	var exportRequest collectortracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(request.body, &exportRequest); err != nil {
		t.Fatalf("decode Langfuse OTLP request: %v", err)
	}
	var input, output, usage string
	for _, resourceSpans := range exportRequest.ResourceSpans {
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				for _, attribute := range span.Attributes {
					switch attribute.Key {
					case "langfuse.observation.input":
						input = attribute.Value.GetStringValue()
					case "langfuse.observation.output":
						output = attribute.Value.GetStringValue()
					case "langfuse.observation.usage_details":
						usage = attribute.Value.GetStringValue()
					}
				}
			}
		}
	}
	if !strings.Contains(input, "private prompt") {
		t.Fatalf("Langfuse observation input = %q, want model prompt", input)
	}
	if !strings.Contains(output, "hello") || !strings.Contains(output, "world") {
		t.Fatalf("Langfuse observation output = %q, want streamed model output", output)
	}
	if !strings.Contains(usage, `"input":3`) || !strings.Contains(usage, `"output":2`) {
		t.Fatalf("Langfuse usage details = %q, want provider token usage", usage)
	}
}

func TestMountPrometheusKeepsMetricsOutsideApplicationHandler(t *testing.T) {
	applicationCalls := 0
	application := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		applicationCalls++
		writer.WriteHeader(http.StatusNoContent)
	})
	metricsHandler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("metrics"))
	})
	handler := mountPrometheus(application, metricsHandler)

	metricsRecorder := httptest.NewRecorder()
	handler.ServeHTTP(metricsRecorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metricsRecorder.Body.String() != "metrics" || applicationCalls != 0 {
		t.Fatalf("metrics route body=%q applicationCalls=%d", metricsRecorder.Body.String(), applicationCalls)
	}

	appRecorder := httptest.NewRecorder()
	handler.ServeHTTP(appRecorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if appRecorder.Code != http.StatusNoContent || applicationCalls != 1 {
		t.Fatalf("application route status=%d applicationCalls=%d", appRecorder.Code, applicationCalls)
	}
}
