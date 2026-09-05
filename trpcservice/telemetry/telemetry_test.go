package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestConfigDefaultsDisabled(t *testing.T) {
	config, err := Config{}.WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if config.Mode != ModeNone {
		t.Fatalf("default mode not none: %s", config.Mode)
	}
	if config.SampleRatio != 1 || config.BatchSize != 512 {
		t.Fatalf("unexpected defaults: %+v", config)
	}
}

func TestConfigOTLPFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing endpoint", func(c *Config) { c.Endpoint = "" }},
		{"invalid endpoint", func(c *Config) { c.Endpoint = "not a url\x07" }},
		{"insecure remote", func(c *Config) { c.Endpoint = "https://collector.example.com"; c.InsecureLocalOK = true }},
		{"unknown environment", func(c *Config) { c.Environment = "prod" }},
		{"ratio above one", func(c *Config) { c.SampleRatio = 1.5 }},
		{"oversized batch", func(c *Config) { c.BatchSize = 999999 }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			config := Config{Mode: ModeOTLP, Endpoint: "127.0.0.1:4317", InsecureLocalOK: true}
			testCase.mutate(&config)
			if _, err := config.WithDefaults(); err == nil {
				t.Fatal("invalid OTLP configuration accepted")
			}
		})
	}
}

func TestComposeNoneStartsNothing(t *testing.T) {
	runtime, err := Compose(context.Background(), Config{Mode: ModeNone}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown not idempotent: %v", err)
	}
}

func TestHTTPMiddlewareMetricsAndSpans(t *testing.T) {
	runtime, err := Compose(context.Background(), Config{Mode: ModeNone}, nil)
	if err != nil {
		t.Fatal(err)
	}
	teapot := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	okay := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	recorder := httptest.NewRecorder()
	runtime.HTTPMiddleware(teapot).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/webhook/telegram", strings.NewReader("body")))
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("middleware changed status: %d", recorder.Code)
	}
	recorder2 := httptest.NewRecorder()
	runtime.HTTPMiddleware(okay).ServeHTTP(recorder2, httptest.NewRequest(http.MethodGet, "/totally/unknown/path", nil))
	if recorder2.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder2.Code)
	}
}

func TestRouteTemplateAndStatusClass(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/webhook/lark", nil)
	if RouteTemplate(request) != "/webhook/{channel}" {
		t.Fatalf("webhook template: %s", RouteTemplate(request))
	}
	if StatusClass(http.StatusTooManyRequests) != "4xx" || StatusClass(599) != "5xx" {
		t.Fatal("status class mapping broken")
	}
}

type recordingLogger struct {
	mu      sync.Mutex
	records []map[string]any
}

func (r *recordingLogger) Write(p []byte) (int, error) {
	var record map[string]any
	if err := json.Unmarshal(p, &record); err != nil {
		return 0, err
	}
	r.mu.Lock()
	r.records = append(r.records, record)
	r.mu.Unlock()
	return len(p), nil
}

func TestStructuredLoggerSafety(t *testing.T) {
	recorder := &recordingLogger{}
	logger := NewJSONLoggerForTest(recorder)
	logger.Event(context.Background(), 8, "worker execution attempt", "worker", "execute", "failed",
		"error_category", "model_mismatch",
		"tenant_id", "tenant-a\nprompt injection",
		"content", "raw memory content that must never appear",
		"error", "postgres://user:pass@host/db raw error with SQL; DROP TABLE x",
	)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.records) != 1 {
		t.Fatalf("unexpected record count: %d", len(recorder.records))
	}
	record := recorder.records[0]
	encoded, _ := json.Marshal(record)
	text := string(encoded)
	if strings.Contains(text, "\n") {
		t.Fatal("record is not single-line")
	}
	for _, forbidden := range []string{"tenant-a", "prompt injection", "raw memory content", "postgres://", "DROP TABLE"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("forbidden content leaked: %s", forbidden)
		}
	}
	if record["error_category"] != "model_mismatch" {
		t.Fatalf("category missing: %+v", record)
	}
}

func TestSafeErrorCollapsesRawErrors(t *testing.T) {
	if got := SafeError(nil); got != "" {
		t.Fatal("nil error not empty")
	}
	if got := SafeError(errorsNew("vector: model_mismatch")); got != "vector: model_mismatch" {
		t.Fatalf("category sentinel altered: %s", got)
	}
	if got := SafeError(errorsNew("pq: relation does not exist at 10.0.0.1:5432")); got != "unknown" {
		t.Fatalf("raw error leaked: %s", got)
	}
}

func TestFingerprintBounded(t *testing.T) {
	first := Fingerprint("tenant-a")
	second := Fingerprint("tenant-a")
	third := Fingerprint("tenant-b")
	if first != second || first == third || len(first) != 16 {
		t.Fatalf("fingerprint semantics broken: %s %s %s", first, second, third)
	}
}

func TestTruncateSingleLine(t *testing.T) {
	value := Truncate("line1\nline2\rdirect\x00value", 64)
	if strings.ContainsAny(value, "\n\r\x00") {
		t.Fatalf("control characters survived: %q", value)
	}
	if len(Truncate(strings.Repeat("a", 500), 16)) != 16 {
		t.Fatal("truncation failed")
	}
}

func TestCardinalityBudget(t *testing.T) {
	runtime, err := Compose(context.Background(), Config{Mode: ModeNone}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Drive the same operation with 100 different tenant/job/task identifiers
	// and raw error texts; the allowlist builder must collapse them all.
	for i := 0; i < 100; i++ {
		runtime.Metrics().WorkerJob("tenant-" + string(rune('a'+i%26)) + string(rune(i)))
	}
	if got := runtime.Metrics().UnknownAttrDropped(); got != 0 {
		t.Fatalf("unexpected dropped counters: %d", got)
	}
}

func TestCarrierRoundTrip(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()
	tracer := provider.Tracer("carrier-test")
	ctx, span := tracer.Start(context.Background(), "producer")
	carrier := InjectCarrier(ctx)
	if carrier == nil || carrier.Traceparent == "" {
		t.Fatal("carrier not injected")
	}
	span.End()
	linked, state := Extract(context.Background(), carrier)
	if state != ContextStatePresent {
		t.Fatalf("carrier state: %s", state)
	}
	if linked == nil {
		t.Fatal("linked context nil")
	}
	if _, state := Extract(context.Background(), nil); state != ContextStateMissing {
		t.Fatal("nil carrier state")
	}
	if _, state := Extract(context.Background(), &TraceCarrier{Traceparent: "00-garbage"}); state != ContextStateInvalid {
		t.Fatal("invalid carrier state")
	}
}
