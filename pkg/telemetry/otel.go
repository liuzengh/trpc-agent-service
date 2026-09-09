package telemetry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
)

// SetupTracing installs W3C propagation and, when an OTLP endpoint is
// configured, a batched trace exporter. An empty endpoint is an explicit
// local-development mode: propagation still works, but no export is claimed.
func SetupTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	// This platform persists and propagates only the W3C trace context. Baggage
	// has no business use here, and accepting arbitrary baggage headers would
	// expand the untrusted request surface without improving trace continuity.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	insecure, err := strconv.ParseBool(valueOrDefault(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"), "false"))
	if err != nil {
		return nil, fmt.Errorf("invalid OTEL_EXPORTER_OTLP_INSECURE: %w", err)
	}
	if insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	} else if certificatePath := os.Getenv("OTEL_EXPORTER_OTLP_CERTIFICATE"); certificatePath != "" {
		// The deployment contract mounts a private collector CA at this path.
		// Do not silently ignore it: without the trust bundle a TLS exporter can
		// fail only after the first batch is flushed, making telemetry loss look
		// like a transient collector outage.
		roots, err := loadOTLPTrustBundle(certificatePath)
		if err != nil {
			return nil, err
		}
		opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
		})))
	}
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	ratio, err := strconv.ParseFloat(valueOrDefault(os.Getenv("OTEL_TRACE_SAMPLE_RATIO"), "0.1"), 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return nil, fmt.Errorf("OTEL_TRACE_SAMPLE_RATIO must be between 0 and 1")
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, fmt.Errorf("create trace resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// loadOTLPTrustBundle loads a PEM-encoded CA bundle for a TLS-protected OTLP
// collector. It intentionally does not enable system trust replacement: the
// mounted bundle is appended to the platform roots so public and private
// collector certificates both remain usable.
func loadOTLPTrustBundle(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, fmt.Errorf("OTEL_EXPORTER_OTLP_CERTIFICATE is empty")
	}
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read OTLP collector CA bundle: %w", err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("OTLP collector CA bundle contains no certificates")
	}
	return roots, nil
}

// HTTPMiddleware extracts the incoming W3C context and starts one server span.
func HTTPMiddleware(spanName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("enterprise/http").Start(ctx, spanName)
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// PublicHTTPMiddleware starts a fresh root span for an unauthenticated public
// endpoint. External traceparent and baggage headers are intentionally ignored:
// otherwise an attacker can force sampling or attach a webhook to another
// tenant's trace before signature verification. The gateway persists the
// resulting trusted trace carrier after the request enters its own span.
func PublicHTTPMiddleware(spanName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("enterprise/http").Start(r.Context(), spanName, trace.WithNewRoot())
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// InjectHTTP propagates the active context to an outbound request.
func InjectHTTP(ctx context.Context, req *http.Request) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
}

// ContextWithTraceParent restores a durable trace carrier read from Inbox or
// Outbox storage. This is what joins asynchronous hops into one trace.
func ContextWithTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{"traceparent": traceParent}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// TraceParentFromContext serializes the active span for a durable async hop.
func TraceParentFromContext(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

func valueOrDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
