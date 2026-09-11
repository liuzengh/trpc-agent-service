package application

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type MaintenanceOptions struct {
	PollInterval, OperationTimeout time.Duration
	BatchSize, MaxPagesPerSweep    int
}
type MaintenanceResult struct {
	RecoveredClaims, RecoveredCalling, ExpiredPending int
	ScannedObservations, ResolvedObservations         int
}
type MaintenanceSnapshot struct {
	Running     bool
	Sweeps      uint64
	LastResult  MaintenanceResult
	LastFailure string // empty, invalid, canceled, or unavailable; never a raw Port error
}
type Maintainer struct {
	store    MaintenanceStore
	options  MaintenanceOptions
	mu       sync.Mutex
	cursor   string
	started  atomic.Bool
	statusMu sync.Mutex
	status   MaintenanceSnapshot
}

func NewMaintainer(store MaintenanceStore, o MaintenanceOptions) (*Maintainer, error) {
	if absent(store) {
		return nil, domain.ErrUnavailable
	}
	if o.PollInterval == 0 {
		o.PollInterval = time.Second
	}
	if o.OperationTimeout == 0 {
		o.OperationTimeout = 2 * time.Second
	}
	if o.BatchSize == 0 {
		o.BatchSize = 100
	}
	if o.MaxPagesPerSweep == 0 {
		o.MaxPagesPerSweep = 4
	}
	if o.PollInterval < time.Millisecond || o.PollInterval > time.Hour || o.OperationTimeout < time.Millisecond || o.OperationTimeout > time.Minute || o.BatchSize < 1 || o.BatchSize > 1000 || o.MaxPagesPerSweep < 1 || o.MaxPagesPerSweep > 100 {
		return nil, domain.ErrInvalid
	}
	return &Maintainer{store: store, options: o}, nil
}

// Sweep performs delivery-only maintenance without credentials, active accounts,
// senders or an execution verifier. Each Port must honor its context deadline.
// Independent steps still run after a recoverable failure. Observation cursors
// advance even past failed/conflicting evidence; the next full pass revisits it.
func (m *Maintainer) Sweep(ctx context.Context) (result MaintenanceResult, returned error) {
	if ctx == nil {
		return result, domain.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !m.mu.TryLock() {
		return result, domain.ErrUnavailable
	}
	defer m.mu.Unlock()
	defer func() {
		failure := ""
		if returned != nil {
			failure = "unavailable"
			if returned == domain.ErrInvalid {
				failure = "invalid"
			}
			if ctx.Err() != nil {
				failure = "canceled"
			}
		}
		m.statusMu.Lock()
		m.status.Sweeps++
		m.status.LastResult = result
		m.status.LastFailure = failure
		m.statusMu.Unlock()
	}()
	for _, step := range []struct {
		run   func(context.Context, int) (int, error)
		count *int
	}{{m.store.RecoverExpiredClaims, &result.RecoveredClaims}, {m.store.RecoverStaleCalling, &result.RecoveredCalling}, {m.store.ExpirePending, &result.ExpiredPending}} {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		op, cancel := context.WithTimeout(ctx, m.options.OperationTimeout)
		n, err := step.run(op, m.options.BatchSize)
		timedOut := op.Err() != nil
		cancel()
		if err != nil || timedOut {
			returned = domain.ErrUnavailable
			continue
		}
		if n < 0 || n > m.options.BatchSize {
			returned = domain.ErrInvalid
			continue
		}
		*step.count = n
	}
	for pageNumber := 0; pageNumber < m.options.MaxPagesPerSweep; pageNumber++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		op, cancel := context.WithTimeout(ctx, m.options.OperationTimeout)
		page, err := m.store.ListResolvableObserved(op, ObservedAttemptQuery{AfterAttemptID: m.cursor, Limit: m.options.BatchSize})
		timedOut := op.Err() != nil
		cancel()
		if err != nil || timedOut {
			return result, domain.ErrUnavailable
		}
		if !validRuntimePage(page.AttemptIDs, m.cursor, page.NextAttemptID, page.Exhausted, m.options.BatchSize) {
			return result, domain.ErrInvalid
		}
		for _, id := range page.AttemptIDs {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			op, cancel := context.WithTimeout(ctx, m.options.OperationTimeout)
			resolved, err := m.store.ResolveObserved(op, id)
			timedOut := op.Err() != nil
			cancel()
			result.ScannedObservations++
			if err != nil || timedOut {
				returned = domain.ErrUnavailable
				continue
			}
			if resolved {
				result.ResolvedObservations++
			}
		}
		m.cursor = page.NextAttemptID
		if page.Exhausted {
			m.cursor = ""
			break
		}
	}
	return result, returned
}
func (m *Maintainer) Snapshot() MaintenanceSnapshot {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	return m.status
}

// Run retries bounded sweeps at a fixed cadence; it owns one lifecycle. It never
// starts detached per-operation goroutines or swallows failures into success.
func (m *Maintainer) Run(ctx context.Context) error {
	if ctx == nil {
		return domain.ErrInvalid
	}
	if !m.started.CompareAndSwap(false, true) {
		return ErrRuntimeStarted
	}
	return m.run(ctx)
}
func (m *Maintainer) run(ctx context.Context) error {
	m.statusMu.Lock()
	m.status.Running = true
	m.statusMu.Unlock()
	defer func() { m.statusMu.Lock(); m.status.Running = false; m.statusMu.Unlock() }()
	ticker := time.NewTicker(m.options.PollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		_, _ = m.Sweep(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
