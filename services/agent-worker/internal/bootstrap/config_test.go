package bootstrap

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	var c Config
	abs, err := filepath.Abs("example.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = readJSONFile(abs, false, &c); err != nil {
		t.Fatal(err)
	}
	c.DatabaseURL = "postgres://worker_runtime:fixture@127.0.0.1/database?sslmode=disable"
	c.MigrationDatabaseURL = "postgres://worker_migrator:fixture@127.0.0.1/database?sslmode=disable"
	if err = c.Validate(); err != nil {
		t.Fatal(err)
	}
	return c
}
func TestWorkerConfigClosedAndExplicit(t *testing.T) {
	c := testConfig(t)
	raw, err := os.ReadFile("example.json")
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"unknown": strings.Replace(string(raw), `"worker_id"`, `"unknown"`, 1), "case alias": strings.Replace(string(raw), `"worker_id"`, `"Worker_ID"`, 1), "duplicate": strings.Replace(string(raw), `"worker_id":`, `"worker_id":"second","worker_id":`, 1), "null explicit skew": strings.Replace(string(raw), `"max_future_skew": "30s"`, `"max_future_skew": null`, 1), "missing limit": strings.Replace(string(raw), `"max_active_attempts": 4,`, "", 1)} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			os.WriteFile(path, []byte(body), 0600)
			var value Config
			if readJSONFile(path, false, &value) == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*Config){"database": func(c *Config) { c.MigrationDatabaseURL = "" }, "contract": func(c *Config) { c.PlatformContractDigest = "latest" }, "http control": func(c *Config) { c.ControlURL = "http://control" }, "role overlap": func(c *Config) { c.GatewayPrincipals = c.ControlPrincipals }, "renewal": func(c *Config) { c.Policy.RenewalInterval = c.Policy.LeaseTTL }, "snapshot capacity": func(c *Config) { c.Limits.MaxSnapshotBytes = 0 }, "unbounded request": func(c *Config) { c.Timing.RequestTimeout = 0 }} {
		t.Run(name, func(t *testing.T) {
			copy := c
			mutate(&copy)
			if copy.Validate() == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}
func TestWorkerDatabaseRoleBoundary(t *testing.T) {
	m := databaseTarget{Database: "db", Schema: "worker", Role: "worker_migrator", Login: "worker_migrator", Owner: true, Create: true}
	r := databaseTarget{Database: "db", Schema: "worker", Role: "worker_runtime", Login: "worker_runtime"}
	if checkDatabaseTargets(m, r) != nil {
		t.Fatal("valid target rejected")
	}
	for name, mutate := range map[string]func(*databaseTarget, *databaseTarget){"admin": func(m, r *databaseTarget) { m.Admin = true }, "assumed runtime": func(m, r *databaseTarget) { r.Login = "platform_admin" }, "foreign schema": func(m, r *databaseTarget) { m.Schema = "gateway"; r.Schema = "gateway" }, "create runtime": func(m, r *databaseTarget) { r.Create = true }, "membership": func(m, r *databaseTarget) { r.Membership = true }, "cross schema": func(m, r *databaseTarget) { r.CrossSchema = true }, "temporary": func(m, r *databaseTarget) { r.Temporary = true }, "migration cross schema": func(m, r *databaseTarget) { m.CrossSchema = true }, "migration database create": func(m, r *databaseTarget) { m.DatabaseCreate = true }, "migration temporary": func(m, r *databaseTarget) { m.Temporary = true }, "wrong database": func(m, r *databaseTarget) { r.Database = "other" }} {
		t.Run(name, func(t *testing.T) {
			mc, rc := m, r
			mutate(&mc, &rc)
			if checkDatabaseTargets(mc, rc) == nil {
				t.Fatal("invalid role target accepted")
			}
		})
	}
}
func TestWorkerReadinessRequiresRecoveryAndOperationalState(t *testing.T) {
	a := &App{}
	request := httptest.NewRequest("GET", "/readyz", nil)
	check := func(want int) {
		w := httptest.NewRecorder()
		a.healthHandler().ServeHTTP(w, request)
		if w.Code != want {
			t.Fatalf("readiness %d", w.Code)
		}
	}
	check(503)
	a.initialized.Store(true)
	check(503)
	a.storageHealthy.Store(true)
	a.runHealthy.Store(true)
	a.manifestHealthy.Store(true)
	a.replyHealthy.Store(true)
	check(204)
	a.draining.Store(true)
	check(503)
}

type oneRun struct{ run domain.Run }

func (r oneRun) Scheduled(context.Context, int) ([]application.ScheduledRun, error) {
	return []application.ScheduledRun{{Run: r.run}}, nil
}

type heldProcessor struct {
	calls   atomic.Int64
	entered chan context.Context
	release chan struct{}
}

func (p *heldProcessor) Advance(ctx context.Context, _ domain.Run) error {
	p.calls.Add(1)
	p.entered <- ctx
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestSchedulerBoundsAttemptsAndPreservesActiveDrainContext(t *testing.T) {
	c := testConfig(t)
	c.Limits.MaxActiveAttempts = 1
	c.Timing.PollInterval = Duration(time.Millisecond)
	p := &heldProcessor{entered: make(chan context.Context, 2), release: make(chan struct{})}
	a := &App{config: c, executor: p}
	a.storageHealthy.Store(true)
	work, stopWork := context.WithCancel(context.Background())
	active, stopActive := context.WithCancel(context.Background())
	defer stopActive()
	done := make(chan struct{})
	go func() {
		a.schedule(work, active, oneRun{domain.Run{Request: domain.Requested{RunID: "run", Route: domain.Route{TenantID: "tenant"}}}})
		close(done)
	}()
	var attempt context.Context
	select {
	case attempt = <-p.entered:
	case <-time.After(time.Second):
		t.Fatal("attempt not started")
	}
	time.Sleep(10 * time.Millisecond)
	if p.calls.Load() != 1 {
		t.Fatal("duplicate concurrent advancement")
	}
	stopWork()
	<-done
	if attempt.Err() != nil {
		t.Fatal("intake stop canceled active Attempt")
	}
	close(p.release)
	a.attempts.Wait()
	if a.active.Load() != 0 {
		t.Fatal("active slot leaked")
	}
}

func TestOptionalTelemetryConfigurationIsClosed(t *testing.T) {
	raw, err := os.ReadFile("example.json")
	if err != nil {
		t.Fatal(err)
	}
	for name, extra := range map[string]string{
		"unknown": "{\"metrics_endpoint\":\"http://127.0.0.1:4318/v1/metrics\",\"export_interval\":\"1s\",\"export_timeout\":\"1s\",\"headers\":{}}",
		"missing": "{\"metrics_endpoint\":\"http://127.0.0.1:4318/v1/metrics\",\"export_interval\":\"1s\"}",
		"null":    "null",
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.TrimSpace(string(raw))
			body = body[:len(body)-1] + ",\"telemetry\":" + extra + "}"
			path := filepath.Join(t.TempDir(), "config.json")
			os.WriteFile(path, []byte(body), 0600)
			var c Config
			if readJSONFile(path, false, &c) == nil {
				t.Fatal("invalid telemetry accepted")
			}
		})
	}
	c := testConfig(t)
	c.Telemetry = &TelemetryConfig{MetricsEndpoint: "http://127.0.0.1:4318/v1/metrics", ExportInterval: Duration(time.Second), ExportTimeout: Duration(time.Second)}
	if err = c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Telemetry.MetricsEndpoint = "http://collector:4318/v1/metrics"
	if c.Validate() == nil {
		t.Fatal("remote cleartext exporter accepted")
	}
}

func TestRetainedRunCapacityConfigurationIsRequired(t *testing.T) {
	c := testConfig(t)
	if c.Limits.MaxRetainedRuns != 100000 {
		t.Fatalf("example retained capacity=%d", c.Limits.MaxRetainedRuns)
	}
	raw, err := os.ReadFile("example.json")
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(string(raw), `"max_retained_runs": 100000,`, "", 1)
	path := filepath.Join(t.TempDir(), "missing-retained.json")
	if err = os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	var missing Config
	if err = readJSONFile(path, false, &missing); err == nil {
		t.Fatal("missing retained capacity accepted")
	}
	for _, value := range []int{0, -1} {
		invalid := c
		invalid.Limits.MaxRetainedRuns = value
		if invalid.Validate() == nil {
			t.Fatalf("retained capacity %d accepted", value)
		}
	}
	// Lowering this deployment limit does not invalidate already retained facts.
	c.Limits.MaxRetainedRuns = 1
	if err = c.Validate(); err != nil {
		t.Fatal(err)
	}
}
