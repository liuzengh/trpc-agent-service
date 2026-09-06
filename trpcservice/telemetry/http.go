package telemetry

import (
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// HTTPMiddleware extracts traceparent and starts the callback/Gateway span.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(
			r.Context(), propagation.HeaderCarrier(r.Header),
		)
		route := traceRoute(r.URL.Path)
		ctx, span := otel.Tracer("trpc-agent-service/http").Start(
			ctx, r.Method+" "+route, trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()
		writer := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		spanContext := span.SpanContext()
		if spanContext.IsValid() {
			writer.Header().Set("X-Trace-ID", spanContext.TraceID().String())
		}
		next.ServeHTTP(writer, r.WithContext(ctx))
		span.SetAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("http.route", route),
			attribute.Int("http.response.status_code", writer.status),
		)
		if writer.status >= 500 {
			span.SetStatus(codes.Error, strconv.Itoa(writer.status))
		}
	})
}

func traceRoute(path string) string {
	if strings.HasPrefix(path, "/callbacks/") {
		parts := strings.Split(path, "/")
		if len(parts) == 4 && (parts[2] == "telegram" || parts[2] == "wecom") {
			return "/callbacks/" + parts[2] + "/{callback_key}"
		}
		return "/callbacks/{channel}/{callback_key}"
	}
	if strings.HasPrefix(path, "/admin/") {
		return "/admin/{operation}"
	}
	switch path {
	case "/chat", "/inbound", "/healthz", "/readyz":
		return path
	default:
		return "/{unmatched}"
	}
}
