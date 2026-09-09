package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestPublicHTTPMiddlewareStartsNewRoot(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() { _ = provider.Shutdown(context.Background()) }()

	const external = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var observed string
	handler := PublicHTTPMiddleware("public.webhook", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = TraceParentFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader("{}"))
	request.Header.Set("traceparent", external)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want 204", response.Code)
	}
	if observed == "" {
		t.Fatal("public middleware did not create a trace carrier")
	}
	if strings.Contains(observed, "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Fatalf("external trace parent was accepted: %q", observed)
	}
}

func TestSetupTracingPropagatesTraceContextWithoutBaggage(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := SetupTracing(context.Background(), "telemetry-test")
	if err != nil {
		t.Fatalf("setup tracing: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	member, err := baggage.NewMember("tenant_hint", "untrusted")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.FromContext(context.Background()).SetMember(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if got := carrier.Get("baggage"); got != "" {
		t.Fatalf("unexpected baggage propagation: %q", got)
	}

	restored := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"baggage":     "tenant_hint=untrusted",
	})
	if got := baggage.FromContext(restored).Len(); got != 0 {
		t.Fatalf("unexpected baggage extraction count: %d", got)
	}
}

func TestLoadOTLPTrustBundleRejectsMalformedPEM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOTLPTrustBundle(path); err == nil {
		t.Fatal("malformed OTLP CA bundle was accepted")
	}
}

func TestLoadOTLPTrustBundleRejectsMissingFile(t *testing.T) {
	if _, err := loadOTLPTrustBundle(filepath.Join(t.TempDir(), "missing.crt")); err == nil {
		t.Fatal("missing OTLP CA bundle was accepted")
	}
}
