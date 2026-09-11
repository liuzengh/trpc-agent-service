package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// stubHandler records that a mounted sub-handler was reached, so the health
// tests can assert the probes were added without stealing an existing route.
func stubHandler(name string, hits *int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*hits++
		w.Header().Set("X-Stub", name)
		_, _ = w.Write([]byte(name))
	})
}

func do(t *testing.T, h http.Handler, path string) (*httptest.ResponseRecorder, healthBody) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body healthBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: body %q is not the health JSON: %v", path, rec.Body.String(), err)
	}
	return rec, body
}

// TestHealthzTouchesNoDependency pins the liveness contract: it answers 200
// even with zero tenants and a failing readiness check, because restarting the
// process would not fix a dependency outage (proposal doc 3.6).
func TestHealthzTouchesNoDependency(t *testing.T) {
	h := NewServer(stubHandler("gw", new(int)), stubHandler("admin", new(int)),
		func() []string { return nil },
		func(context.Context) error { return errors.New("redis is down") },
	)
	rec, body := do(t, h, HealthPath)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200 regardless of dependencies", rec.Code)
	}
	if body.Status != "ok" || len(body.Reasons) != 0 {
		t.Fatalf("healthz body = %+v", body)
	}
}

func TestReadyzNilReadyIsAlwaysReady(t *testing.T) {
	h := NewServer(stubHandler("gw", new(int)), stubHandler("admin", new(int)), func() []string { return []string{"demo"} }, nil)
	rec, body := do(t, h, ReadyPath)
	if rec.Code != http.StatusOK || body.Status != "ready" {
		t.Fatalf("readyz = %d %+v, want 200 ready when nothing is wired", rec.Code, body)
	}
}

func TestReadyzWithoutTenants(t *testing.T) {
	h := NewServer(stubHandler("gw", new(int)), stubHandler("admin", new(int)), func() []string { return nil }, nil)
	rec, body := do(t, h, ReadyPath)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503 with zero tenants", rec.Code)
	}
	if len(body.Reasons) != 1 || !strings.Contains(body.Reasons[0], "tenant") {
		t.Fatalf("readyz reasons = %+v, want the missing-tenant reason", body.Reasons)
	}
}

func TestReadyzReportsDependencyFailure(t *testing.T) {
	boom := errors.New("storage: session backend read: connection refused")
	h := NewServer(stubHandler("gw", new(int)), stubHandler("admin", new(int)), func() []string { return []string{"demo"} },
		func(context.Context) error { return boom })
	rec, body := do(t, h, ReadyPath)
	if rec.Code != http.StatusServiceUnavailable || body.Status != "unavailable" {
		t.Fatalf("readyz = %d %+v, want 503 unavailable", rec.Code, body)
	}
	if len(body.Reasons) != 1 || body.Reasons[0] != boom.Error() {
		t.Fatalf("readyz reasons = %+v, want the underlying error verbatim", body.Reasons)
	}
}

// TestReadyzCollectsEveryReason asserts an operator sees all unmet conditions
// at once instead of fixing them one round trip at a time.
func TestReadyzCollectsEveryReason(t *testing.T) {
	h := NewServer(stubHandler("gw", new(int)), stubHandler("admin", new(int)), func() []string { return []string{} },
		func(context.Context) error { return errors.New("redis down") })
	rec, body := do(t, h, ReadyPath)
	if rec.Code != http.StatusServiceUnavailable || len(body.Reasons) != 2 {
		t.Fatalf("readyz = %d %+v, want 503 with two reasons", rec.Code, body)
	}
}

// TestReadyzRecoversWithoutRestart is the D4 drill in unit form: readiness is
// re-evaluated per request, so a dependency that comes back puts the process
// back into rotation with no restart and no rewiring.
func TestReadyzRecoversWithoutRestart(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	h := NewServer(stubHandler("gw", new(int)), stubHandler("admin", new(int)), func() []string { return []string{"demo"} },
		func(context.Context) error {
			if down.Load() {
				return errors.New("redis down")
			}
			return nil
		})

	if rec, _ := do(t, h, ReadyPath); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz while down = %d, want 503", rec.Code)
	}
	down.Store(false)
	if rec, body := do(t, h, ReadyPath); rec.Code != http.StatusOK || body.Status != "ready" {
		t.Fatalf("readyz after recovery = %d %+v, want 200 ready", rec.Code, body)
	}
	down.Store(true)
	if rec, _ := do(t, h, ReadyPath); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz after a second outage = %d, want 503", rec.Code)
	}
}

// TestReadyzBoundsTheProbe checks the readiness context carries a deadline, so
// a probe implementation that forgets its own timeout cannot hang the probe.
func TestReadyzBoundsTheProbe(t *testing.T) {
	var sawDeadline bool
	h := NewServer(stubHandler("gw", new(int)), stubHandler("admin", new(int)), func() []string { return []string{"demo"} },
		func(ctx context.Context) error {
			_, sawDeadline = ctx.Deadline()
			return nil
		})
	if rec, _ := do(t, h, ReadyPath); rec.Code != http.StatusOK {
		t.Fatalf("readyz = %d, want 200", rec.Code)
	}
	if !sawDeadline {
		t.Fatal("ready must receive a context with a deadline")
	}
}

// TestHealthProbesDoNotStealRoutes guards the routes that existed before the
// probes were added: a mux pattern mistake here would silently break the chat
// page or the admin API.
func TestHealthProbesDoNotStealRoutes(t *testing.T) {
	var gwHits, adminHits int
	h := NewServer(stubHandler("gw", &gwHits), stubHandler("admin", &adminHits),
		func() []string { return []string{"demo"} }, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<") {
		t.Fatalf("index = %d, want the embedded chat page", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("index content type = %q", ct)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/tenants", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `["demo"]` {
		t.Fatalf("tenants = %d %q", rec.Code, rec.Body.String())
	}

	for _, path := range []string{"/admin/tenants", "/callback/webchat/demo", "/webchat/stream"} {
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Header().Get("X-Stub") == "" {
			t.Fatalf("%s did not reach its sub-handler (code %d)", path, rec.Code)
		}
	}
	if gwHits != 2 || adminHits != 1 {
		t.Fatalf("sub-handler hits = gw %d admin %d, want gw 2 admin 1", gwHits, adminHits)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path = %d, want 404", rec.Code)
	}
}
