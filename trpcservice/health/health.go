// Package health contains the dependency registry shared by liveness,
// readiness, and the protected operational dependency endpoint.  Probe
// results are deliberately reduced to stable categories: a readiness
// response must never become an accidental DSN or provider-error oracle.
package health

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
)

type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusDegraded  Status = "degraded"
	StatusUnhealthy Status = "unhealthy"
	StatusUnknown   Status = "unknown"
)

// Probe is a low-cardinality dependency check. Required dependencies block
// readiness; telemetry backends may be registered as optional probes.
type Probe struct {
	Name     string
	Backend  string
	Required bool
	Check    func(context.Context) error
}

type Dependency struct {
	Name          string    `json:"component"`
	Backend       string    `json:"backend"`
	Status        Status    `json:"status"`
	Required      bool      `json:"required"`
	Epoch         string    `json:"epoch,omitempty"`
	LatencyMS     int64     `json:"latency_ms"`
	LastCheckedAt time.Time `json:"last_checked_at"`
	ErrorCode     string    `json:"error_code,omitempty"`
}

type Registry struct {
	mu       sync.RWMutex
	probes   map[string]Probe
	results  map[string]Dependency
	metrics  *metrics.Metrics
	interval time.Duration
	timeout  time.Duration
	stop     chan struct{}
	done     chan struct{}
	started  bool
	gates    map[string]bool
}

func NewRegistry(interval, timeout time.Duration, exporter *metrics.Metrics) *Registry {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if exporter == nil {
		exporter = metrics.NewMetrics()
	}
	return &Registry{probes: make(map[string]Probe), results: make(map[string]Dependency), metrics: exporter,
		interval: interval, timeout: timeout, gates: make(map[string]bool)}
}

func (r *Registry) Add(probe Probe) error {
	if r == nil || probe.Name == "" || probe.Backend == "" || probe.Check == nil {
		return errors.New("health probe requires name, backend, and check")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.probes[probe.Name]; exists {
		return errors.New("health probe already registered")
	}
	r.probes[probe.Name] = probe
	r.results[probe.Name] = Dependency{Name: probe.Name, Backend: probe.Backend, Required: probe.Required, Status: StatusUnknown}
	return nil
}

// SetGate adds a readiness gate for state which is not naturally represented
// by a network probe, such as control-plane convergence or audit persistence.
func (r *Registry) SetGate(name string, ready bool) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	r.gates[name] = ready
	r.mu.Unlock()
}

func (r *Registry) Check(ctx context.Context) []Dependency {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	probes := make([]Probe, 0, len(r.probes))
	for _, probe := range r.probes {
		probes = append(probes, probe)
	}
	timeout := r.timeout
	r.mu.RUnlock()
	for _, probe := range probes {
		started := time.Now()
		probeCtx, cancel := context.WithTimeout(ctxOrBackground(ctx), timeout)
		err := probe.Check(probeCtx)
		cancel()
		result := Dependency{Name: probe.Name, Backend: probe.Backend, Required: probe.Required,
			Status: StatusHealthy, LatencyMS: time.Since(started).Milliseconds(), LastCheckedAt: time.Now().UTC()}
		if err != nil {
			result.Status = StatusUnhealthy
			result.ErrorCode = classify(err)
		}
		r.mu.Lock()
		r.results[probe.Name] = result
		r.mu.Unlock()
		r.metrics.Observe("dependency_probe_duration_seconds", "Dependency probe duration.", time.Since(started).Seconds(),
			map[string]string{"component": probe.Name, "backend": probe.Backend})
		if result.Status == StatusHealthy {
			r.metrics.Set("dependency_health", "Dependency health state (1 healthy, 0 unavailable).", 1,
				map[string]string{"component": probe.Name, "backend": probe.Backend})
		} else {
			r.metrics.Set("dependency_health", "Dependency health state (1 healthy, 0 unavailable).", 0,
				map[string]string{"component": probe.Name, "backend": probe.Backend})
			r.metrics.Add("dependency_probe_failures_total", "Dependency probe failures.", 1,
				map[string]string{"component": probe.Name, "backend": probe.Backend})
		}
	}
	return r.Snapshot()
}

func (r *Registry) Snapshot() []Dependency {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Dependency, 0, len(r.results))
	for _, result := range r.results {
		out = append(out, result)
	}
	// Deterministic ordering also keeps evidence and tests stable.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Name < out[j-1].Name; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func (r *Registry) Ready() bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, result := range r.results {
		if result.Required && result.Status != StatusHealthy {
			return false
		}
	}
	for _, ready := range r.gates {
		if !ready {
			return false
		}
	}
	return true
}

func (r *Registry) Start(ctx context.Context) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.stop = make(chan struct{})
	r.done = make(chan struct{})
	stop, done, interval := r.stop, r.done, r.interval
	r.mu.Unlock()
	go func() {
		defer close(done)
		r.Check(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				r.Check(ctx)
			}
		}
	}()
}

func (r *Registry) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return
	}
	stop, done := r.stop, r.done
	r.started = false
	r.mu.Unlock()
	close(stop)
	<-done
}

func classify(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "migration"):
		return "migration_not_ready"
	case strings.Contains(text, "drain"):
		return "draining"
	case strings.Contains(text, "auth"), strings.Contains(text, "permission"):
		return "permission_denied"
	default:
		return "unavailable"
	}
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
