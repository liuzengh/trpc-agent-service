package health

import "net/http"

type ReadinessChecker interface{ Ready() bool }

// Handler exposes process liveness and lifecycle readiness. A draining worker
// is not ready, allowing the load balancer to stop sending new work while the
// Consumer finishes its bounded drain window.
type Handler struct{ Checker ReadinessChecker }

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	switch r.URL.Path {
	case "/livez":
		w.WriteHeader(http.StatusOK)
	case "/readyz":
		if h.Checker != nil && h.Checker.Ready() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		http.NotFound(w, r)
	}
}
