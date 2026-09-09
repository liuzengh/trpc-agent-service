package coordination

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// State is the externally observable failover state.
type State string

const (
	ActivePrimary   State = "active-primary"
	Quarantined     State = "quarantined"
	Switching       State = "switching"
	ActiveSecondary State = "active-secondary"
	Probing         State = "probing"
	Recovering      State = "recovering"
	Failed          State = "failed"
	Blocked         State = "blocked"

	// Deprecated aliases retained for the initial Claim-only adapter.
	RedisActive    = ActivePrimary
	Quarantine     = Quarantined
	PostgresActive = ActiveSecondary
)

// Backend is the legacy Claim-only adapter contract.
type Backend interface {
	Claim(context.Context, tenant.TenantContext, storage.DedupKey, time.Duration, string) (storage.Claim, error)
}

// Endpoint contains the fencing-aware contracts for one coordination backend.
type Endpoint struct {
	Name     storage.CoordinationBackend
	Legacy   Backend
	Claims   storage.ClaimStore
	Leases   storage.LeaseStore
	Probe    func(context.Context) error
	Activate func(context.Context, storage.Epoch) error
}

// Config controls only the deterministic state-machine guards. It does not
// configure backend clients or production resources.
type Config struct {
	QuarantineWindow time.Duration
	FailureThreshold int
	Breaker          BreakerConfig
	Now              func() time.Time
}

// Snapshot is a race-safe state observation.
type Snapshot struct {
	State         State
	ActiveBackend storage.CoordinationBackend
	Epoch         storage.Epoch
	FailureCount  int
	QuarantineEnd time.Time
}

// FailoverStore is the fencing-aware dispatch boundary. It never retries an
// uncertain operation and never acquires a replacement lease automatically.
type FailoverStore struct {
	mu        sync.Mutex
	primary   Endpoint
	secondary Endpoint
	authority storage.EpochAuthority
	cfg       Config
	state     State
	active    Endpoint
	epoch     storage.Epoch
	resource  string
	tenantID  string
	failures  int
	until     time.Time
	breaker   *CircuitBreaker
}

// NewFailoverStore preserves the original Claim-only constructor. It is
// useful for legacy callers but cannot switch without an authority.
func NewFailoverStore(primary, secondary Backend) *FailoverStore {
	return NewFailoverStoreWithAuthority(
		Endpoint{Name: storage.BackendRedis, Legacy: primary},
		Endpoint{Name: storage.BackendPostgres, Legacy: secondary},
		nil, "", "", Config{},
	)
}

func NewFailoverStoreWithAuthority(primary, secondary Endpoint, authority storage.EpochAuthority, tenantID, resource string, cfg Config) *FailoverStore {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 1
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Breaker.FailureThreshold <= 0 {
		cfg.Breaker.FailureThreshold = cfg.FailureThreshold
	}
	if cfg.Breaker.ProtectionWindow == 0 {
		cfg.Breaker.ProtectionWindow = cfg.QuarantineWindow
	}
	cfg.Breaker.Now = cfg.Now
	if primary.Name == "" {
		primary.Name = storage.BackendRedis
	}
	if secondary.Name == "" {
		secondary.Name = storage.BackendPostgres
	}
	return &FailoverStore{primary: primary, secondary: secondary, authority: authority, cfg: cfg, state: ActivePrimary, active: primary, tenantID: tenantID, resource: resource, breaker: NewCircuitBreaker(cfg.Breaker)}
}

// Initialize reads the current epoch from the database authority. It is
// required before switching when an authority is configured.
func (f *FailoverStore) Initialize(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.authority == nil {
		f.epoch = 1
		return nil
	}
	e, err := f.authority.GetEpoch(ctx, f.tenantID, f.resource)
	if err != nil {
		return err
	}
	f.epoch = e
	return nil
}

func (f *FailoverStore) Snapshot() Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Snapshot{State: f.state, ActiveBackend: f.active.Name, Epoch: f.epoch, FailureCount: f.failures, QuarantineEnd: f.until}
}

func (f *FailoverStore) State() State         { return f.Snapshot().State }
func (f *FailoverStore) Epoch() storage.Epoch { return f.Snapshot().Epoch }

func (f *FailoverStore) activeEndpoint() (Endpoint, storage.Epoch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state != ActivePrimary && f.state != ActiveSecondary {
		return Endpoint{}, 0, storage.ErrBackendUnavailable
	}
	return f.active, f.epoch, nil
}

func (f *FailoverStore) endpointClaim(ctx context.Context, e Endpoint, tc tenant.TenantContext, key storage.DedupKey, ttl time.Duration, owner string) (storage.Claim, error) {
	if e.Claims != nil {
		return e.Claims.Claim(ctx, tc, key, ttl, owner)
	}
	if e.Legacy != nil {
		return e.Legacy.Claim(ctx, tc, key, ttl, owner)
	}
	return storage.Claim{}, storage.ErrInvalidArgument
}

func unavailable(err error) bool {
	if errors.Is(err, storage.ErrBackendUnavailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, storage.ErrOperationAmbiguous) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func (f *FailoverStore) recordFailure(err error) error {
	if !unavailable(err) {
		return err
	}
	f.mu.Lock()
	isPrimary := f.active.Name == f.primary.Name
	f.mu.Unlock()
	if isPrimary {
		f.breaker.RecordFailure(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == ActivePrimary || f.state == ActiveSecondary {
		f.failures++
		if f.failures >= f.cfg.FailureThreshold {
			f.state = Quarantined
			f.until = f.cfg.Now().Add(f.cfg.QuarantineWindow)
		}
	}
	return errors.Join(err, storage.ErrOperationAmbiguous)
}

func (f *FailoverStore) Claim(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, ttl time.Duration, owner string) (storage.Claim, error) {
	e, epoch, err := f.activeEndpoint()
	if err != nil {
		return storage.Claim{}, err
	}
	claim, err := f.endpointClaim(ctx, e, tc, key, ttl, owner)
	if err != nil {
		return storage.Claim{}, f.recordFailure(err)
	}
	if claim.Backend != e.Name || (epoch != 0 && claim.Epoch != epoch) {
		return storage.Claim{}, storage.ErrEpochRejected
	}
	return claim, nil
}

func (f *FailoverStore) guardOK(ctx context.Context, backend storage.CoordinationBackend, epoch storage.Epoch) (Endpoint, error) {
	e, current, err := f.activeEndpoint()
	if err != nil {
		return Endpoint{}, err
	}
	if backend != e.Name || epoch == 0 || current == 0 || epoch != current {
		return Endpoint{}, storage.ErrEpochRejected
	}
	if f.authority != nil {
		if err := f.authority.ValidateEpoch(ctx, f.tenantID, f.resource, epoch); err != nil {
			return Endpoint{}, err
		}
	}
	return e, nil
}

func (f *FailoverStore) Complete(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner, response string, guard storage.OperationGuard) error {
	e, err := f.guardOK(ctx, guard.Backend, guard.Epoch)
	if err != nil {
		return err
	}
	if e.Claims == nil {
		return storage.ErrInvalidArgument
	}
	return e.Claims.Complete(ctx, tc, key, owner, response, guard)
}

func (f *FailoverStore) Fail(ctx context.Context, tc tenant.TenantContext, key storage.DedupKey, owner string, guard storage.OperationGuard, retryable bool) error {
	e, err := f.guardOK(ctx, guard.Backend, guard.Epoch)
	if err != nil {
		return err
	}
	if e.Claims == nil {
		return storage.ErrInvalidArgument
	}
	return e.Claims.Fail(ctx, tc, key, owner, guard, retryable)
}

func (f *FailoverStore) Acquire(ctx context.Context, tc tenant.TenantContext, resource, owner string, ttl time.Duration) (storage.Lease, error) {
	e, epoch, err := f.activeEndpoint()
	if err != nil {
		return storage.Lease{}, err
	}
	if e.Leases == nil {
		return storage.Lease{}, storage.ErrInvalidArgument
	}
	lease, err := e.Leases.Acquire(ctx, tc, resource, owner, ttl)
	if err != nil {
		return storage.Lease{}, f.recordFailure(err)
	}
	if lease.Backend != e.Name || lease.Epoch != epoch {
		return storage.Lease{}, storage.ErrEpochRejected
	}
	return lease, nil
}

func (f *FailoverStore) Renew(ctx context.Context, tc tenant.TenantContext, lease storage.Lease, ttl time.Duration) (storage.Lease, error) {
	e, err := f.guardOK(ctx, lease.Backend, lease.Epoch)
	if err != nil {
		return lease, err
	}
	if e.Leases == nil {
		return lease, storage.ErrInvalidArgument
	}
	updated, err := e.Leases.Renew(ctx, tc, lease, ttl)
	if err != nil {
		return lease, f.recordFailure(err)
	}
	if updated.Backend != e.Name || updated.Epoch != lease.Epoch {
		return lease, storage.ErrEpochRejected
	}
	return updated, nil
}

func (f *FailoverStore) Release(ctx context.Context, tc tenant.TenantContext, lease storage.Lease) error {
	e, err := f.guardOK(ctx, lease.Backend, lease.Epoch)
	if err != nil {
		return err
	}
	if e.Leases == nil {
		return storage.ErrInvalidArgument
	}
	return e.Leases.Release(ctx, tc, lease)
}

func (f *FailoverStore) Validate(ctx context.Context, tc tenant.TenantContext, lease storage.Lease) error {
	e, err := f.guardOK(ctx, lease.Backend, lease.Epoch)
	if err != nil {
		return err
	}
	if e.Leases == nil {
		return storage.ErrInvalidArgument
	}
	return e.Leases.Validate(ctx, tc, lease)
}

// AutomaticSwitch performs one policy-gated switch attempt. It is caller
// driven, so it has no background retry or hidden goroutine.
func (f *FailoverStore) AutomaticSwitch(ctx context.Context) (storage.Epoch, error) {
	f.mu.Lock()
	state := f.state
	f.mu.Unlock()
	if state != Quarantined || f.breaker.State() != BreakerOpen {
		return 0, storage.ErrConflict
	}
	return f.Switch(ctx)
}

// AutoSwitch is a short alias for callers that use the policy name.
func (f *FailoverStore) AutoSwitch(ctx context.Context) (storage.Epoch, error) {
	return f.AutomaticSwitch(ctx)
}

// Switch advances the authority exactly once and activates the secondary.
func (f *FailoverStore) Switch(ctx context.Context) (storage.Epoch, error) {
	f.mu.Lock()
	if f.state != Quarantined {
		f.mu.Unlock()
		return 0, storage.ErrConflict
	}
	if f.cfg.Now().Before(f.until) {
		f.mu.Unlock()
		return 0, storage.ErrBackendUnavailable
	}
	f.state = Switching
	authority := f.authority
	f.mu.Unlock()
	if authority == nil {
		f.mu.Lock()
		f.state = Blocked
		f.mu.Unlock()
		return 0, storage.ErrBackendUnavailable
	}
	epoch, err := authority.BumpEpoch(ctx, f.tenantID, f.resource)
	if err != nil {
		f.breaker.Block()
		f.mu.Lock()
		f.state = Blocked
		f.mu.Unlock()
		return 0, err
	}
	if f.secondary.Activate != nil {
		if err := f.secondary.Activate(ctx, epoch); err != nil {
			f.breaker.Block()
			f.mu.Lock()
			f.state = Blocked
			f.mu.Unlock()
			return 0, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active, f.state, f.epoch, f.failures = f.secondary, ActiveSecondary, epoch, 0
	f.until = time.Time{}
	return epoch, nil
}

// Probe checks the recovered primary without activating it. A successful
// probe enters Recovering; callers must explicitly Recover to bump epoch.
func (f *FailoverStore) Probe(ctx context.Context) error {
	f.mu.Lock()
	if f.state != ActiveSecondary && f.state != Quarantined {
		f.mu.Unlock()
		return storage.ErrConflict
	}
	f.mu.Unlock()
	if err := f.breaker.BeginProbe(); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = Probing
	probe := f.primary.Probe
	f.mu.Unlock()
	if probe == nil {
		_ = f.breaker.RecordProbeSuccess()
		f.mu.Lock()
		f.state = Recovering
		f.mu.Unlock()
		return nil
	}
	if err := probe(ctx); err != nil {
		f.breaker.RecordProbeFailure(err)
		f.mu.Lock()
		f.state = Quarantined
		f.until = f.cfg.Now().Add(f.cfg.QuarantineWindow)
		f.mu.Unlock()
		return err
	}
	if err := f.breaker.RecordProbeSuccess(); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = Recovering
	f.mu.Unlock()
	return nil
}

// Recover completes a successful probe by atomically bumping authority before
// making the primary active.
func (f *FailoverStore) Recover(ctx context.Context) (storage.Epoch, error) {
	f.mu.Lock()
	if f.state != Recovering {
		f.mu.Unlock()
		return 0, storage.ErrConflict
	}
	// Claim the recovery transition before releasing the mutex. Without this
	// reservation, concurrent Recover callers can all observe Recovering and
	// each bump the authority before any caller publishes ActivePrimary.
	f.state = Switching
	f.mu.Unlock()
	if f.authority == nil {
		return 0, storage.ErrBackendUnavailable
	}
	epoch, err := f.authority.BumpEpoch(ctx, f.tenantID, f.resource)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err != nil {
		f.breaker.Block()
		f.state = Blocked
		return 0, err
	}
	f.breaker.Reset()
	f.active, f.state, f.epoch = f.primary, ActivePrimary, epoch
	return epoch, nil
}

var _ storage.ClaimStore = (*FailoverStore)(nil)
var _ storage.LeaseStore = (*FailoverStore)(nil)
