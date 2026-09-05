package telemetry

import (
	"net/http"
	"time"
)

// HTTPMiddleware wraps an HTTP handler with a server span (fresh server root
// span: external traceparent/tracestate/baggage are not trusted) and
// low-cardinality request metrics. Telemetry failures never affect the
// response.
func (r *Runtime) HTTPMiddleware(next http.Handler) http.Handler {
	if r == nil || next == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestCtx, span := r.Tracer().Start(request.Context(), routeName(request))
		defer span.End()
		route := RouteTemplate(request)
		inflightAttrs := Attrs{Component: "http", Route: route}
		r.Metrics().HTTPInflight(1, inflightAttrs)
		defer r.Metrics().HTTPInflight(-1, inflightAttrs)
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			attrs := Attrs{Component: "http", Method: request.Method, Route: route, StatusClass: StatusClass(recorder.status)}
			r.Metrics().HTTPRequestDuration(attrs, time.Since(started).Seconds())
		}()
		next.ServeHTTP(recorder, request.WithContext(requestCtx))
	})
}

// HTTPInflight adjusts the in-flight gauge. Split from the registry so the
// middleware controls the window precisely.
func (r *Runtime) HTTPInflight(delta int, attrs Attrs) {
	if r == nil {
		return
	}
	r.meter.HTTPInflight(delta, attrs)
}

// HTTPRequestDuration records the request latency histogram.
func (r *Runtime) HTTPRequestDuration(attrs Attrs, seconds float64) {
	if r == nil {
		return
	}
	r.meter.HTTPRequestDuration(attrs, seconds)
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if !r.wroteHeader {
		r.status = status
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(body)
}

func routeName(request *http.Request) string {
	if request == nil {
		return "http"
	}
	return "http " + RouteTemplate(request)
}
