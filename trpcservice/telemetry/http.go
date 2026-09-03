package telemetry

import (
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
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
		ctx, span := otel.Tracer("trpc-agent-service/http").Start(
			ctx, r.Method+" "+r.URL.Path,
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
			attribute.String("url.path", r.URL.Path),
			attribute.Int("http.response.status_code", writer.status),
		)
		if writer.status >= 500 {
			span.SetStatus(codes.Error, strconv.Itoa(writer.status))
		}
	})
}
