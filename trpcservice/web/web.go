// Package web serves the admin/chat pages of the platform.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
	"time"
)

// Health probe paths, exported so main's -healthcheck self-probe, the Compose
// healthcheck, and the Kubernetes manifests all agree on the URLs instead of
// repeating string literals in four places.
const (
	HealthPath = "/healthz"
	ReadyPath  = "/readyz"
)

// readyTimeout bounds one readiness evaluation. The probe implementations set
// their own timeouts as well; this is the outer backstop that keeps a slow
// dependency from holding the probe (and the orchestrator's probe worker)
// hostage past its own deadline.
const readyTimeout = 3 * time.Second

//go:embed index.html
var static embed.FS

// NewServer assembles the public HTTP handler: the chat page at "/", the
// tenant list API, the health probes, the admin API under /admin/, and the
// channel gateway under /callback and /webchat. tenantIDs is queried per
// request so admin hot updates show up without a restart.
//
// ready reports whether the process can serve traffic right now; nil means
// always ready, which keeps the walking skeleton and the tests wiring-free.
// It is queried per request, so a dependency that dies mid-flight starts
// failing readiness on the next beat and recovers the same way.
func NewServer(gateway, admin http.Handler, tenantIDs func() []string, ready func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := static.ReadFile("index.html")
		if err != nil {
			http.Error(w, "page unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/api/tenants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(tenantIDs())
	})
	mux.HandleFunc(HealthPath, handleLiveness)
	mux.HandleFunc(ReadyPath, handleReadiness(tenantIDs, ready))
	mux.Handle("/admin/", admin)
	mux.Handle("/callback/", gateway)
	mux.Handle("/webchat/", gateway)
	return mux
}

// healthBody is the probe response. Reasons stay empty on success so the
// happy path is a few bytes; on failure they name every unmet condition at
// once, because an operator reading a 503 should not have to fix them one
// round trip at a time.
type healthBody struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons,omitempty"`
}

func writeHealth(w http.ResponseWriter, code int, body healthBody) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// handleLiveness answers the liveness probe (proposal doc 3.6). It touches no
// dependency on purpose: a Redis outage must not make the orchestrator restart
// a process that is still able to serve and to recover by itself. Shedding
// traffic during such an outage is readiness's job, not liveness's.
//
// Neither probe starts a trace span. They fire every few seconds forever, and
// their spans would bury the message traces that actually carry the end-to-end
// link of proposal doc 3.5.
func handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writeHealth(w, http.StatusOK, healthBody{Status: "ok"})
}

// handleReadiness collects every unmet readiness condition and answers 503
// with the list, or 200 once all of them hold.
func handleReadiness(tenantIDs func() []string, ready func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var reasons []string
		// A process with zero tenants cannot answer any message, so it must not
		// be handed traffic even though it is perfectly alive.
		if tenantIDs != nil && len(tenantIDs()) == 0 {
			reasons = append(reasons, "no tenant configured")
		}
		if ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
			if err := ready(ctx); err != nil {
				reasons = append(reasons, err.Error())
			}
			cancel()
		}
		if len(reasons) > 0 {
			writeHealth(w, http.StatusServiceUnavailable, healthBody{Status: "unavailable", Reasons: reasons})
			return
		}
		writeHealth(w, http.StatusOK, healthBody{Status: "ready"})
	}
}
