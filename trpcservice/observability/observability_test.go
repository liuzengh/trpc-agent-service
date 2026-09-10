package observability

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestNoopProviderLifecycleAndCorrelation(t *testing.T) {
	provider := NewNoopProvider()
	ctx := WithCorrelation(context.Background(), "req-1", "trace-1")
	if RequestID(ctx) != "req-1" || TraceID(ctx) != "trace-1" {
		t.Fatalf("correlation values were not preserved")
	}
	next, span := provider.Tracer("test").Start(ctx, OperationGatewayDispatch, Attribute{Key: "component", Value: "gateway"})
	span.SetStatus(StatusOK, "")
	span.RecordError(errors.New("provider detail"))
	span.End()
	if next == nil {
		t.Fatal("tracer returned nil context")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRedactionRemovesCredentialsAndRawFields(t *testing.T) {
	input := "Authorization: Bearer abc123 dsn=postgres://user:pass@example/db token=secret"
	redacted := RedactString(input)
	for _, secret := range []string{"abc123", "user:pass", "secret"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("redacted value contains %q: %s", secret, redacted)
		}
	}
	fields := RedactFields(map[string]string{"message": "user text", "api_key": "key", "operation": "runner.execution"})
	if fields["message"] != "<redacted>" || fields["api_key"] != "<redacted>" || fields["operation"] != "runner.execution" {
		t.Fatalf("unexpected fields: %#v", fields)
	}
}

func TestErrorClassAndDuration(t *testing.T) {
	if ErrorClass(context.Canceled) != "canceled" || ErrorClass(context.DeadlineExceeded) != "timeout" || ErrorClass(errors.New("x")) != "error" {
		t.Fatal("unexpected error classes")
	}
	if DurationMilliseconds(time.Now().Add(-time.Millisecond)) <= 0 {
		t.Fatal("duration should be positive")
	}
}

func TestSanitizeAttributesEnforcesAllowedValuesAndCardinality(t *testing.T) {
	const testKey = "test_value"
	allowedAttributeKeys[testKey] = struct{}{}
	t.Cleanup(func() { delete(allowedAttributeKeys, testKey) })

	attrs := []Attribute{
		{Key: "operation", Value: OperationModelCall},
		{Key: "operation", Value: "model.unknown"},
		{Key: "status", Value: "success"},
		{Key: "status", Value: "unknown"},
		{Key: "tenant_hash", Value: "0123456789abcdef"},
		{Key: "tenant_hash", Value: "0123456789ABCDEf"},
		{Key: "app_hash", Value: "fedcba9876543210"},
		{Key: "app_hash", Value: "short"},
		{Key: "currency", Value: "usd"},
		{Key: "currency", Value: "USD"},
		{Key: "currency", Value: "xxx"},
		{Key: testKey, Value: "token=secret"},
		{Key: testKey, Value: "request-123"},
		{Key: testKey, Value: "line1\nline2"},
		{Key: testKey, Value: "https://example.com"},
		{Key: testKey, Value: strings.Repeat("x", 65)},
		{Key: "unknown", Value: "safe"},
	}
	got := sanitizeAttributes(attrs)
	want := []Attribute{
		{Key: "operation", Value: OperationModelCall},
		{Key: "status", Value: "success"},
		{Key: "tenant_hash", Value: "0123456789abcdef"},
		{Key: "app_hash", Value: "fedcba9876543210"},
		{Key: "currency", Value: "usd"},
		{Key: testKey, Value: "token=<redacted>"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sanitizeAttributes() = %#v, want %#v", got, want)
	}
}

func TestOTLPProviderFallsBackToNoopWithoutEndpoint(t *testing.T) {
	provider, err := NewOTLPProvider(context.Background(), OTLPConfig{Headers: map[string]string{"authorization": "Bearer secret"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartOperationProvidesGenericHook(t *testing.T) {
	ctx, _, finish := StartOperation(context.Background(), NewNoopProvider(), OperationToolCall, "tool")
	if ctx == nil {
		t.Fatal("operation context is nil")
	}
	finish(context.Canceled)
}

func TestTraceParentRoundTripRejectsInvalidValues(t *testing.T) {
	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := ContextWithTraceParent(context.Background(), valid)
	if got := TraceParentFromContext(ctx); got != valid {
		t.Fatalf("traceparent round trip = %q, want %q", got, valid)
	}
	for _, invalid := range []string{"", "not-a-traceparent", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"} {
		if got := TraceParentFromContext(ContextWithTraceParent(context.Background(), invalid)); got != "" {
			t.Fatalf("invalid traceparent %q was retained as %q", invalid, got)
		}
	}
	if got := NormalizeTraceParent(valid); got != valid {
		t.Fatalf("normalized traceparent = %q, want %q", got, valid)
	}
	if got := NormalizeTraceParent("malformed"); got != "" {
		t.Fatalf("malformed traceparent normalized as %q", got)
	}
	if got := TraceParentFromContext(ContextWithoutTraceParent(ctx)); got != "" {
		t.Fatalf("ambient traceparent retained after reset: %q", got)
	}
	if ContextWithoutTraceParent(nil) == nil {
		t.Fatal("nil context reset returned nil")
	}
}

func TestTelemetryAdaptersEnforceSafeFieldsAndWrapSDKPrimitives(t *testing.T) {
	var logs bytes.Buffer
	provider := NewProvider(Config{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
		Logger:         slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	ctx, span := provider.Tracer("test").Start(context.Background(), OperationToolCall,
		Attribute{Key: "operation", Value: OperationToolCall},
		Attribute{Key: "message", Value: "user text"},
		Attribute{Key: "token", Value: "secret"})
	span.SetAttributes(Attribute{Key: "status", Value: "ok"}, Attribute{Key: "raw", Value: "payload"})
	span.SetStatus(StatusError, "Authorization: Bearer secret")
	span.RecordError(errors.New("token=secret"))
	span.End()
	meter := provider.Meter("test")
	meter.Counter("requests").Add(ctx, 1, Attribute{Key: "component", Value: "tool"})
	meter.Histogram("duration").Record(ctx, 1, Attribute{Key: "operation", Value: OperationToolCall})
	meter.UpDownCounter("active").Add(ctx, 1, Attribute{Key: "status", Value: "ok"})
	provider.Logger().Log(ctx, LevelInfo, "Authorization: Bearer secret", Attribute{Key: "message", Value: "user text"}, Attribute{Key: "operation", Value: OperationToolCall})
	provider.Logger().Log(ctx, LevelDebug, "debug")
	provider.Logger().Log(ctx, LevelWarn, "warn")
	provider.Logger().Log(ctx, LevelError, "error")
	if strings.Contains(logs.String(), "user text") || strings.Contains(logs.String(), "secret") {
		t.Fatalf("unsafe log content: %s", logs.String())
	}
}

func TestProviderBranchesAndOTLPConstruction(t *testing.T) {
	otlp, err := NewOTLPProvider(context.Background(), OTLPConfig{Endpoint: "127.0.0.1:4318", Insecure: true, Headers: map[string]string{"authorization": "Bearer secret"}})
	if err != nil {
		t.Fatal(err)
	}
	// The test endpoint intentionally does not implement OTLP HTTP routes; the
	// provider must still construct and close without panicking. Export errors
	// are surfaced by Shutdown for production callers, so this test only checks
	// that the lifecycle call completes.
	_ = otlp.Shutdown(context.Background())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if canceledProvider, err := NewOTLPProvider(canceled, OTLPConfig{Endpoint: "127.0.0.1:4318"}); err == nil {
		_ = canceledProvider.Shutdown(context.Background())
	}
	var shutdowns int
	provider := NewProvider(Config{Shutdown: func(context.Context) error { shutdowns++; return nil }})
	if provider.Tracer("test") == nil || provider.Meter("test") == nil || provider.Logger() == nil {
		t.Fatal("provider components must be available")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if shutdowns != 1 {
		t.Fatalf("shutdown lifecycle = %d", shutdowns)
	}
	var discard discardWriter
	if n, err := discard.Write([]byte("x")); n != 1 || err != nil {
		t.Fatalf("discard writer = %d/%v", n, err)
	}
	ctx := WithCorrelation(context.TODO(), "req", "trace")
	if RequestID(ctx) != "req" || TraceID(ctx) != "trace" {
		t.Fatal("nil/correlation context handling failed")
	}
}

func TestOTLPProviderExportsMetricsToURLAndRejectsInvalidEndpoint(t *testing.T) {
	var metricsRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/metrics" {
			metricsRequests.Add(1)
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	provider, err := NewOTLPProvider(context.Background(), OTLPConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	provider.Meter("test").Counter("trpcservice_requests_total").Add(context.Background(), 1, Attribute{Key: "component", Value: "http"})
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("provider shutdown = %v", err)
	}
	if metricsRequests.Load() == 0 {
		t.Fatal("provider did not export an OTLP metrics request")
	}
	for _, endpoint := range []string{"collector/path", "http://", "ftp://collector:4318", "http://collector:bad path"} {
		if _, err := NewOTLPProvider(context.Background(), OTLPConfig{Endpoint: endpoint}); err == nil {
			t.Fatalf("invalid OTLP endpoint %q was accepted", endpoint)
		}
	}
}

func TestOTLPSignalEndpointURLAddsProtocolPaths(t *testing.T) {
	for _, test := range []struct {
		endpoint, suffix, want string
	}{
		{"http://collector:4318", "/v1/metrics", "http://collector:4318/v1/metrics"},
		{"http://collector:4318/", "/v1/traces", "http://collector:4318/v1/traces"},
		{"http://collector:4318/v1/otlp", "/v1/metrics", "http://collector:4318/v1/otlp/v1/metrics"},
		{"http://collector:4318/v1/metrics", "/v1/metrics", "http://collector:4318/v1/metrics"},
	} {
		if got := otlpSignalEndpointURL(test.endpoint, test.suffix); got != test.want {
			t.Fatalf("otlpSignalEndpointURL(%q, %q) = %q, want %q", test.endpoint, test.suffix, got, test.want)
		}
	}
}
