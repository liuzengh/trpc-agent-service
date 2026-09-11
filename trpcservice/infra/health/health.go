// Package health exposes a liveness/readiness probe handler.
//
// Degradation is tracked as a set of named issues rather than a single flag,
// because the producers fail independently: the MySQL warm-up, the inbound
// consumer and the IM outbound follower each lose their own dependency, and one
// of them recovering must not clear another's outage. A single flag got this
// wrong — a Redis reconnect cleared the probe while the database was still
// unreachable.
package health

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// issues holds one open reason per producer, keyed by issue name.
var issues = struct {
	mu      sync.Mutex
	reasons map[string]string
}{reasons: map[string]string{}}

// Report records (or updates) an open issue with its reason. Producers call it
// with a stable name so recovery can resolve exactly what they opened.
func Report(issue, reason string) {
	issues.mu.Lock()
	issues.reasons[issue] = reason
	issues.mu.Unlock()
}

// Resolve clears an issue. Resolving an unknown issue is a no-op, which keeps
// recovery paths free of existence checks.
func Resolve(issue string) {
	issues.mu.Lock()
	delete(issues.reasons, issue)
	issues.mu.Unlock()
}

// Degraded reports whether any issue is open and, when so, why. Recovery loops
// use it to avoid re-asserting a state they already set, and tests use it to
// assert that a failing dependency is visible from outside.
func Degraded() (bool, string) {
	issues.mu.Lock()
	defer issues.mu.Unlock()
	if len(issues.reasons) == 0 {
		return false, ""
	}
	names := make([]string, 0, len(issues.reasons))
	for name := range issues.reasons {
		names = append(names, name)
	}
	sort.Strings(names) // stable probe output: issue order must not flap
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+" "+issues.reasons[name])
	}
	return true, strings.Join(parts, "; ")
}

// Handler returns an HTTP handler that reports service health.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		degraded, reason := Degraded()

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
