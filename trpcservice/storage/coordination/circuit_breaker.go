package coordination

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// BreakerState is the local health gate for the backend being protected.
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half-open"
	BreakerBlocked  BreakerState = "blocked"
)

// FailureClass is deliberately small and backend-independent.
type FailureClass string

const (
	FailureNone    FailureClass = "none"
	FailureBackend FailureClass = "backend-unavailable"
)

// BreakerConfig contains testable policy, not backend connection settings.
type BreakerConfig struct {
	FailureThreshold int
	ProtectionWindow time.Duration
	SuccessThreshold int
	Now              func() time.Time
}

// BreakerSnapshot is a race-safe observation of breaker state.
type BreakerSnapshot struct {
	State         BreakerState
	Failures      int
	Successes     int
	OpenUntil     time.Time
	ProbeInFlight bool
}

// CircuitBreaker has no goroutines. Callers explicitly open, probe and reset
// it, which keeps ownership and lifecycle deterministic.
type CircuitBreaker struct {
	mu            sync.Mutex
	cfg           BreakerConfig
	state         BreakerState
	failures      int
	successes     int
	openUntil     time.Time
	probeInFlight bool
}

func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 1
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 1
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &CircuitBreaker{cfg: cfg, state: BreakerClosed}
}

func ClassifyFailure(err error) FailureClass {
	if err == nil {
		return FailureNone
	}
	if errors.Is(err, storage.ErrBackendUnavailable) ||
		errors.Is(err, storage.ErrOperationAmbiguous) ||
		errors.Is(err, context.DeadlineExceeded) {
		return FailureBackend
	}
	return FailureNone
}

func (b *CircuitBreaker) Snapshot() BreakerSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BreakerSnapshot{State: b.state, Failures: b.failures, Successes: b.successes, OpenUntil: b.openUntil, ProbeInFlight: b.probeInFlight}
}

func (b *CircuitBreaker) State() BreakerState { return b.Snapshot().State }

// AllowBusiness reports whether a normal Claim/lease operation may reach the
// protected backend. Open and half-open are fail-closed.
func (b *CircuitBreaker) AllowBusiness() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerClosed {
		return nil
	}
	return storage.ErrBackendUnavailable
}

func (b *CircuitBreaker) RecordFailure(err error) FailureClass {
	class := ClassifyFailure(err)
	if class != FailureBackend {
		return class
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerBlocked {
		return class
	}
	if b.state == BreakerHalfOpen {
		b.probeInFlight = false
	}
	b.failures++
	b.successes = 0
	if b.failures >= b.cfg.FailureThreshold {
		b.state = BreakerOpen
		b.openUntil = b.cfg.Now().Add(b.cfg.ProtectionWindow)
	}
	return class
}

// BeginProbe atomically opens the single half-open probe slot after the
// protection window. It never grants business access.
func (b *CircuitBreaker) BeginProbe() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	switch b.state {
	case BreakerClosed:
		return storage.ErrConflict
	case BreakerOpen:
		if now.Before(b.openUntil) {
			return storage.ErrBackendUnavailable
		}
		b.state = BreakerHalfOpen
	case BreakerHalfOpen:
		if b.probeInFlight {
			return storage.ErrConflict
		}
	case BreakerBlocked:
		return storage.ErrBackendUnavailable
	}
	b.probeInFlight = true
	return nil
}

func (b *CircuitBreaker) RecordProbeSuccess() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BreakerHalfOpen || !b.probeInFlight {
		return storage.ErrConflict
	}
	b.probeInFlight = false
	b.successes++
	if b.successes >= b.cfg.SuccessThreshold {
		b.state = BreakerClosed
		b.failures = 0
		b.openUntil = time.Time{}
	}
	return nil
}

func (b *CircuitBreaker) RecordProbeFailure(err error) FailureClass {
	class := ClassifyFailure(err)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeInFlight = false
	if class == FailureBackend {
		b.failures++
		b.successes = 0
		b.state = BreakerOpen
		b.openUntil = b.cfg.Now().Add(b.cfg.ProtectionWindow)
	}
	return class
}

func (b *CircuitBreaker) Block() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = BreakerBlocked
	b.probeInFlight = false
}

func (b *CircuitBreaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = BreakerClosed
	b.failures = 0
	b.successes = 0
	b.openUntil = time.Time{}
	b.probeInFlight = false
}
