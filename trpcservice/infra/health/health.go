// Package health exposes a liveness/readiness probe handler.
package health

import (
	"encoding/json"
	"net/http"
	"sync"
)

// processHealth is the process health: ok until a dependency (e.g. MySQL)
// fails to come up, at which point SetDegraded flips it. It is mutex-guarded
// because the probe handler and a startup goroutine may touch it concurrently.
type processHealth struct {
	mu       sync.Mutex
	degraded bool
	reason   string
}

var health = &processHealth{}

// SetDegraded marks the service degraded with the given reason (e.g. a MySQL
// connection failure at startup). The /healthz probe then reports 503 instead
// of 200, so orchestration/liveness checks surface the degraded state.
func SetDegraded(reason string) {
	health.mu.Lock()
	health.degraded = true
	health.reason = reason
	health.mu.Unlock()
}

// ClearDegraded clears a previously reported degradation.
func ClearDegraded() {
	health.mu.Lock()
	health.degraded = false
	health.reason = ""
	health.mu.Unlock()
}

// Handler returns an HTTP handler that reports service health.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		health.mu.Lock()
		degraded := health.degraded
		reason := health.reason
		health.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if degraded {
			w.WriteHeader(http.StatusServiceUnavailable)
			body, _ := json.Marshal(map[string]string{"status": "degraded", "reason": reason})
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
}
