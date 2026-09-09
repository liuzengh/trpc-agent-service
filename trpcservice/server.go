package trpcservice

import (
	"encoding/json"
	"net/http"
)

// HTTPOptions contains optional feature handlers assembled by main.
type HTTPOptions struct {
	AdminHandler   http.Handler
	MetricsHandler http.Handler
}

// NewHTTPHandler creates the node HTTP handler. Feature-specific routes are
// registered here as their implementation tasks are completed.
func NewHTTPHandler(options ...HTTPOptions) http.Handler {
	return NewHTTPMux(options...)
}

// NewHTTPMux creates an extensible root mux for channel adapters.
func NewHTTPMux(options ...HTTPOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", statusHandler("ok"))
	mux.HandleFunc("GET /readyz", statusHandler("ready"))
	if len(options) > 0 && options[0].AdminHandler != nil {
		for _, method := range []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		} {
			mux.Handle(method+" /api/v1/", options[0].AdminHandler)
		}
	}
	if len(options) > 0 && options[0].MetricsHandler != nil {
		mux.Handle("GET /metrics", options[0].MetricsHandler)
	}
	return mux
}

func statusHandler(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  status,
			"version": Version,
		})
	}
}
