package telemetrytrace

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// IMHandler starts a local root for public IM callbacks. It never extracts
// arbitrary external headers or records the URL, account path or request body.
// Durable propagation after Admission is a separate application responsibility.
func (r *Runtime) IMHandler(next http.Handler) http.Handler {
	if r.sdk == nil {
		return next
	}
	tracer := r.Tracer("channel-gateway")
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasPrefix(req.URL.Path, "/v1/telegram/") {
			next.ServeHTTP(w, req)
			return
		}
		ctx, span := tracer.Start(req.Context(), "gateway.im.callback", trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindServer))
		response := &callbackResponse{ResponseWriter: w}
		completed := false
		defer func() {
			status := response.status
			if completed && status == 0 {
				status = http.StatusOK
			}
			if status != 0 {
				span.SetAttributes(attribute.Int("http.response.status_code", status))
			}
			if !completed || status >= 500 {
				span.SetStatus(codes.Error, "")
				span.SetAttributes(attribute.String("error.type", "failed"))
			}
			End(span, req.Context().Err())
		}()
		next.ServeHTTP(response, req.WithContext(ctx))
		completed = true
	})
}

// callbackResponse captures only the final response code, never payload content.
// Unwrap preserves access to the underlying writer via http.ResponseController.
type callbackResponse struct {
	http.ResponseWriter
	status int
}

func (w *callbackResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *callbackResponse) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	if w.status == 0 && (status >= 200 || status == http.StatusSwitchingProtocols) {
		w.status = status
	}
}
func (w *callbackResponse) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}
func (w *callbackResponse) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
