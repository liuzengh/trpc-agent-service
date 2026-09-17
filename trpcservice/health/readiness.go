package health

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Dependency struct {
	Name  string
	Probe func(context.Context) error
}

type DependencyStatus struct {
	Ready     bool
	CheckedAt time.Time
}

// Monitor keeps dependency probes off the HTTP request path. Readiness is
// true only when the process lifecycle is ready and every dependency passed
// the most recent probe cycle; an unprobed dependency is never ready.
type Monitor struct {
	process      ReadinessChecker
	dependencies []Dependency
	timeout      time.Duration
	interval     time.Duration

	probeMu  sync.Mutex
	mu       sync.RWMutex
	statuses map[string]DependencyStatus
}

func NewMonitor(process ReadinessChecker, dependencies []Dependency, timeout, interval time.Duration) (*Monitor, error) {
	if process == nil || len(dependencies) == 0 || timeout <= 0 || interval <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	seen := make(map[string]struct{}, len(dependencies))
	cloned := make([]Dependency, len(dependencies))
	for index, dependency := range dependencies {
		if dependency.Name == "" || dependency.Probe == nil {
			return nil, runtime.ErrInvariantViolation
		}
		if _, exists := seen[dependency.Name]; exists {
			return nil, runtime.ErrIdempotencyCollision
		}
		seen[dependency.Name] = struct{}{}
		cloned[index] = dependency
	}
	return &Monitor{process: process, dependencies: cloned, timeout: timeout, interval: interval,
		statuses: make(map[string]DependencyStatus, len(dependencies))}, nil
}

func (m *Monitor) Run(ctx context.Context) error {
	if m == nil {
		return runtime.ErrInvariantViolation
	}
	if err := m.ProbeOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.ProbeOnce(ctx); err != nil {
				return err
			}
		}
	}
}

// ProbeOnce records provider failures in the snapshot rather than returning
// them. Its error is reserved for monitor cancellation/invariant failures.
func (m *Monitor) ProbeOnce(ctx context.Context) error {
	if m == nil || len(m.dependencies) == 0 {
		return runtime.ErrInvariantViolation
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.probeMu.Lock()
	defer m.probeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	type result struct {
		name    string
		ready   bool
		checked time.Time
	}
	results := make(chan result, len(m.dependencies))
	var wait sync.WaitGroup
	for _, dependency := range m.dependencies {
		dependency := dependency
		wait.Add(1)
		go func() {
			defer wait.Done()
			probeCtx, cancel := context.WithTimeout(ctx, m.timeout)
			defer cancel()
			err := dependency.Probe(probeCtx)
			results <- result{name: dependency.Name, ready: err == nil, checked: time.Now().UTC()}
		}()
	}
	wait.Wait()
	close(results)
	m.mu.Lock()
	for value := range results {
		m.statuses[value.name] = DependencyStatus{Ready: value.ready, CheckedAt: value.checked}
	}
	m.mu.Unlock()
	return nil
}

func (m *Monitor) Ready() bool {
	if m == nil || m.process == nil || !m.process.Ready() {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.statuses) != len(m.dependencies) {
		return false
	}
	for _, dependency := range m.dependencies {
		if !m.statuses[dependency.Name].Ready {
			return false
		}
	}
	return true
}

func (m *Monitor) Snapshot() map[string]DependencyStatus {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]DependencyStatus, len(m.statuses))
	for name, status := range m.statuses {
		result[name] = status
	}
	return result
}
