package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/capacity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	MaxDispatcherBatchSize     = 100
	MaxDispatcherConcurrency   = 32
	MaxDispatcherClaimInterval = time.Hour
	MaxDispatcherShutdown      = 5 * time.Minute
	MaxDispatcherLockGuard     = time.Hour
)

var (
	ErrInvalidConfig    = errors.New("outbox: invalid dispatcher configuration")
	ErrAlreadyStarted   = errors.New("outbox: dispatcher is already started")
	ErrNotStarted       = errors.New("outbox: dispatcher is not started")
	ErrShutdownTimeout  = errors.New("outbox: shutdown deadline exceeded")
	ErrClaimFailed      = errors.New("outbox: claim failed")
	ErrMutationFailed   = errors.New("outbox: durable mutation failed")
	ErrLockGuardElapsed = errors.New("outbox: lock guard elapsed before send")
	ErrInvalidClaim     = errors.New("outbox: repository returned an invalid claim")
)

// Sender is deliberately outcome based. Implementations must convert provider
// errors to one of the four explicit classes before returning to this package.
type Sender interface {
	Send(context.Context, storage.OutboxMessage) SenderOutcome
}

type SenderFunc func(context.Context, storage.OutboxMessage) SenderOutcome

func (f SenderFunc) Send(ctx context.Context, message storage.OutboxMessage) SenderOutcome {
	if f == nil {
		return SenderOutcome{Class: OutcomeUnknown, Code: DeliveryOutcomeUnknownCode}
	}
	return f(ctx, message)
}

type Config struct {
	OwnerID         string
	Tenants         []tenant.TenantContext
	Tenant          tenant.TenantContext
	ClaimBatchSize  int
	Concurrency     int
	ClaimInterval   time.Duration
	ShutdownTimeout time.Duration
	LockGuard       time.Duration
	RetryPolicy     RetryPolicy
	Now             func() time.Time
	// Budget is the optional durable sender budget (capacity.ScopeGuard
	// satisfies the interface; scope "sender"). When wired, a send holds one
	// sender-scope slot per tenant for the send duration; exhaustion skips
	// the item without a Send call or attempt increment — the claim lock
	// expires and the message is redelivered (bounded backpressure).
	// Tenants without a sender budget row are not enforced (fail-open).
	// Nil keeps historical behavior.
	Budget BudgetGuard
}

// senderBudgetScope is the capacity scope key for outbound dispatch; it
// mirrors capacity.ScopeSender without importing the capacity package.
const senderBudgetScope = "sender"

// BudgetGuard is the durable sender-budget seam implemented by
// capacity.ScopeGuard. It exists so the dispatcher stays testable without a
// database and so the capacity package can evolve independently.
type BudgetGuard interface {
	AcquireScope(ctx context.Context, tenantID, scope, ownerID string, ttl time.Duration) (func(), error)
}

func (c Config) withDefaults() (Config, error) {
	if c.OwnerID == "" && len(c.Tenants) == 0 && c.Tenant.TenantID == "" {
		return Config{}, fmt.Errorf("%w: owner and tenant contexts are required", ErrInvalidConfig)
	}
	if c.OwnerID == "" || !validOwnerID(c.OwnerID) {
		return Config{}, fmt.Errorf("%w: owner ID is invalid", ErrInvalidConfig)
	}
	if len(c.Tenants) == 0 && c.Tenant.TenantID != "" {
		c.Tenants = []tenant.TenantContext{c.Tenant}
	}
	if len(c.Tenants) == 0 {
		return Config{}, fmt.Errorf("%w: at least one tenant context is required", ErrInvalidConfig)
	}
	seen := make(map[string]struct{}, len(c.Tenants))
	for _, tc := range c.Tenants {
		if err := tc.Validate(); err != nil {
			return Config{}, fmt.Errorf("%w: tenant context: %v", ErrInvalidConfig, err)
		}
		if _, ok := seen[tc.TenantID]; ok {
			return Config{}, fmt.Errorf("%w: duplicate tenant context", ErrInvalidConfig)
		}
		seen[tc.TenantID] = struct{}{}
	}
	if c.ClaimBatchSize == 0 {
		c.ClaimBatchSize = storage.DefaultOutboxBatchSize
	}
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	if c.ClaimInterval == 0 {
		c.ClaimInterval = time.Second
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 5 * time.Second
	}
	if c.LockGuard == 0 {
		c.LockGuard = 100 * time.Millisecond
	}
	if c.ClaimBatchSize < 1 || c.ClaimBatchSize > MaxDispatcherBatchSize ||
		c.Concurrency < 1 || c.Concurrency > MaxDispatcherConcurrency ||
		c.ClaimInterval <= 0 || c.ClaimInterval > MaxDispatcherClaimInterval ||
		c.ShutdownTimeout <= 0 || c.ShutdownTimeout > MaxDispatcherShutdown ||
		c.LockGuard < 0 || c.LockGuard > MaxDispatcherLockGuard {
		return Config{}, fmt.Errorf("%w: bounded dispatcher settings are invalid", ErrInvalidConfig)
	}
	c.RetryPolicy = c.RetryPolicy.withDefaults()
	if err := c.RetryPolicy.Validate(); err != nil {
		return Config{}, err
	}
	if c.Now == nil {
		c.Now = func() time.Time { return time.Now().UTC() }
	}
	c.Tenants = append([]tenant.TenantContext(nil), c.Tenants...)
	return c, nil
}

type Stats struct {
	ClaimCalls        uint64
	Claimed           uint64
	ClaimErrors       uint64
	SendCalls         uint64
	DeliveredOutcomes uint64
	RetryableOutcomes uint64
	PermanentOutcomes uint64
	UnknownOutcomes   uint64
	Completed         uint64
	Retried           uint64
	DeadLettered      uint64
	MutationErrors    uint64
	LockLost          uint64
	ShutdownAbandoned uint64
	// SkippedBudgetExhausted counts items skipped before Send because the
	// tenant's durable sender budget was exhausted (or the budget backend
	// failed). The message stays claimed until its lock expires and is then
	// redelivered, so no attempt counter moves.
	SkippedBudgetExhausted uint64
	LastOutcome            OutcomeClass
	LastErrorCategory      string
}

type workItem struct {
	message storage.OutboxMessage
	tenant  tenant.TenantContext
}

// Dispatcher owns no storage truth in memory. The repository claim and the
// owner/lock conditions are the only authority for an active Outbox message.
type Dispatcher struct {
	repository storage.OutboxRepository
	sender     Sender
	config     Config

	mu             sync.Mutex
	started        bool
	stopping       bool
	stopped        bool
	runCtx         context.Context
	runCancel      context.CancelFunc
	runDone        chan struct{}
	stopDone       chan struct{}
	stopErr        error
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	activeCount    int
	activeDone     chan struct{}
	lastErr        error

	statsMu sync.Mutex
	stats   Stats
}

func NewDispatcher(repository storage.OutboxRepository, sender Sender, config Config) (*Dispatcher, error) {
	if repository == nil || sender == nil {
		return nil, ErrInvalidConfig
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	d := &Dispatcher{repository: repository, sender: sender, config: config, activeDone: make(chan struct{})}
	close(d.activeDone)
	return d, nil
}

var _ Sender = (SenderFunc)(nil)

// Run claims until its context is canceled or Stop is called. Claim failures
// are recorded and retried at the configured interval; they never turn into a
// successful delivery or an in-memory substitute for durable state.
func (d *Dispatcher) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return ErrAlreadyStarted
	}
	d.runCtx, d.runCancel = context.WithCancel(ctx)
	d.runDone = make(chan struct{})
	d.started = true
	runCtx := d.runCtx
	runDone := d.runDone
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		if !d.stopping {
			d.stopping = true
		}
		d.mu.Unlock()
		close(runDone)
	}()

	for {
		if err := runCtx.Err(); err != nil {
			return err
		}
		if d.isStopping() {
			return context.Canceled
		}
		d.claimCycle(runCtx)
		if err := waitInterval(runCtx, d.config.ClaimInterval); err != nil {
			return err
		}
	}
}

func (d *Dispatcher) claimCycle(ctx context.Context) {
	for _, tc := range d.config.Tenants {
		if d.isStopping() || ctx.Err() != nil {
			return
		}
		capacity := d.availableCapacity()
		if capacity == 0 {
			return
		}
		limit := d.config.ClaimBatchSize
		if limit > capacity {
			limit = capacity
		}
		d.addStat(func(s *Stats) { s.ClaimCalls++ })
		messages, err := d.repository.ClaimBatch(ctx, tc, d.config.OwnerID, limit)
		if err != nil {
			if ctx.Err() == nil && !d.isStopping() {
				d.addStat(func(s *Stats) { s.ClaimErrors++ })
				d.recordError(errors.Join(ErrClaimFailed, err), "claim failed")
			}
			continue
		}
		for _, message := range messages {
			if !validClaim(message, tc.TenantID, d.config.OwnerID) {
				d.recordError(ErrInvalidClaim, "invalid claim")
				continue
			}
			d.addStat(func(s *Stats) { s.Claimed++ })
			d.start(workItem{message: message, tenant: tc}, ctx)
		}
	}
}

func (d *Dispatcher) start(item workItem, ctx context.Context) {
	d.mu.Lock()
	if d.activeCount == 0 {
		d.activeDone = make(chan struct{})
	}
	d.activeCount++
	d.mu.Unlock()
	go func() {
		defer d.finishWork()
		d.process(item, ctx)
	}()
}

func (d *Dispatcher) process(item workItem, ctx context.Context) {
	message := item.message
	if !d.sendAllowed(ctx, message) {
		d.addStat(func(s *Stats) { s.ShutdownAbandoned++ })
		return
	}
	// WS-8 durable sender budget: hold one sender-scope slot across the
	// send. Exhaustion skips the item (no Send, no attempt increment); the
	// claim lock expires and the message is redelivered — bounded
	// backpressure without distorting retry accounting. A budget backend
	// failure also skips the cycle and is recorded as a mutation error
	// (fail closed: never send unaccounted).
	if d.config.Budget != nil {
		releaseBudget, budgetErr := d.config.Budget.AcquireScope(ctx, item.tenant.TenantID, senderBudgetScope, d.config.OwnerID, time.Until(message.LockedUntil.Add(d.config.LockGuard)))
		if budgetErr != nil {
			if errors.Is(budgetErr, capacity.ErrCapacityFull) {
				d.addStat(func(s *Stats) { s.SkippedBudgetExhausted++ })
				return
			}
			d.recordError(budgetErr, "sender budget acquire")
			d.addStat(func(s *Stats) { s.SkippedBudgetExhausted++ })
			return
		}
		if releaseBudget != nil {
			defer releaseBudget()
		}
	}
	deadline := message.LockedUntil.Add(-d.config.LockGuard)
	sendCtx, cancel := context.WithDeadline(ctx, deadline)
	d.addStat(func(s *Stats) { s.SendCalls++ })
	outcome := d.sender.Send(sendCtx, message)
	cancel()
	outcome = ClassifyOutcome(outcome)
	d.addStat(func(s *Stats) {
		s.LastOutcome = outcome.Class
		switch outcome.Class {
		case OutcomeDelivered:
			s.DeliveredOutcomes++
		case OutcomeRetryableFailure:
			s.RetryableOutcomes++
		case OutcomePermanentFailure:
			s.PermanentOutcomes++
		case OutcomeUnknown:
			s.UnknownOutcomes++
		}
	})
	if d.mutationExpired(message) {
		d.addStat(func(s *Stats) { s.ShutdownAbandoned++ })
		return
	}
	decision, err := d.config.RetryPolicy.Decide(message.Attempt, outcome, d.config.Now())
	if err != nil {
		d.recordError(err, "retry policy failed")
		return
	}
	mutationCtx, mutationCancel := d.mutationContext(ctx, message)
	defer mutationCancel()
	switch decision.Action {
	case Complete:
		err = d.repository.MarkCompleted(mutationCtx, item.tenant, d.config.OwnerID, message.ID)
		if err == nil {
			d.addStat(func(s *Stats) { s.Completed++ })
		}
	case Retry:
		err = d.repository.MarkRetry(mutationCtx, item.tenant, d.config.OwnerID, message.ID, decision.NextAttempt, decision.Code)
		if err == nil {
			d.addStat(func(s *Stats) { s.Retried++ })
		}
	case DeadLetter:
		err = d.repository.MoveToDLQ(mutationCtx, item.tenant, d.config.OwnerID, message.ID, decision.Code)
		if err == nil {
			d.addStat(func(s *Stats) { s.DeadLettered++ })
		}
	default:
		err = ErrInvalidPolicy
	}
	if err != nil {
		d.addStat(func(s *Stats) { s.MutationErrors++ })
		if errors.Is(err, storage.ErrOutboxLockLost) || errors.Is(err, storage.ErrOutboxLockExpired) || errors.Is(err, storage.ErrFenceRejected) || errors.Is(err, storage.ErrLeaseLost) {
			d.addStat(func(s *Stats) { s.LockLost++ })
		}
		category := "durable mutation failed"
		if errors.Is(err, storage.ErrBackendUnavailable) || errors.Is(err, storage.ErrTransactionOutcomeUnknown) {
			category = "repository unavailable or ambiguous"
		}
		d.recordError(errors.Join(ErrMutationFailed, err), category)
	}
}

func (d *Dispatcher) sendAllowed(ctx context.Context, message storage.OutboxMessage) bool {
	if message.LockedUntil.IsZero() || message.LockedBy != d.config.OwnerID {
		d.recordError(ErrInvalidClaim, "invalid claim")
		return false
	}
	if !d.isStopping() && ctx.Err() == nil && d.config.Now().Before(message.LockedUntil.Add(-d.config.LockGuard)) {
		return true
	}
	if !d.isStopping() && ctx.Err() == nil {
		d.recordError(ErrLockGuardElapsed, "lock guard elapsed")
	}
	return false
}

func (d *Dispatcher) mutationContext(ctx context.Context, message storage.OutboxMessage) (context.Context, context.CancelFunc) {
	d.mu.Lock()
	base := ctx
	if d.shutdownCtx != nil {
		base = d.shutdownCtx
	}
	d.mu.Unlock()
	return context.WithDeadline(context.WithoutCancel(base), message.LockedUntil)
}

func (d *Dispatcher) mutationExpired(message storage.OutboxMessage) bool {
	d.mu.Lock()
	shutdownCtx := d.shutdownCtx
	d.mu.Unlock()
	if shutdownCtx != nil && shutdownCtx.Err() != nil {
		return true
	}
	return !d.config.Now().Before(message.LockedUntil)
}

func (d *Dispatcher) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return ErrNotStarted
	}
	if d.stopDone != nil {
		done := d.stopDone
		d.mu.Unlock()
		select {
		case <-done:
			return d.stopResult()
		case <-ctx.Done():
			return errors.Join(ErrShutdownTimeout, ctx.Err())
		}
	}
	d.stopDone = make(chan struct{})
	d.stopping = true
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, d.config.ShutdownTimeout)
	d.shutdownCtx, d.shutdownCancel = shutdownCtx, shutdownCancel
	runCancel, runDone, activeDone := d.runCancel, d.runDone, d.activeDone
	d.mu.Unlock()

	if runCancel != nil {
		runCancel()
	}
	if !waitChannel(shutdownCtx, runDone) {
		return d.finishStop(errors.Join(ErrShutdownTimeout, shutdownCtx.Err()))
	}
	if !waitChannel(shutdownCtx, activeDone) {
		return d.finishStop(errors.Join(ErrShutdownTimeout, shutdownCtx.Err(), d.outstandingError()))
	}
	shutdownCancel()
	return d.finishStop(d.lastErrorValue())
}

func (d *Dispatcher) finishWork() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.activeCount--
	if d.activeCount == 0 {
		close(d.activeDone)
	}
}

func (d *Dispatcher) finishStop(err error) error {
	d.mu.Lock()
	if d.shutdownCancel != nil {
		d.shutdownCancel()
	}
	d.stopErr = err
	d.stopped = true
	close(d.stopDone)
	d.mu.Unlock()
	return err
}

func (d *Dispatcher) stopResult() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stopErr
}

func (d *Dispatcher) outstandingError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.activeCount == 0 {
		return nil
	}
	return fmt.Errorf("%w: active=%d", ErrShutdownTimeout, d.activeCount)
}

func (d *Dispatcher) availableCapacity() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopping || d.activeCount >= d.config.Concurrency {
		return 0
	}
	return d.config.Concurrency - d.activeCount
}

func (d *Dispatcher) isStopping() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stopping
}

func (d *Dispatcher) recordError(err error, category string) {
	d.mu.Lock()
	if d.lastErr == nil {
		d.lastErr = err
	}
	d.mu.Unlock()
	d.addStat(func(s *Stats) { s.LastErrorCategory = category })
}

func (d *Dispatcher) lastErrorValue() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastErr
}

func (d *Dispatcher) addStat(update func(*Stats)) {
	d.statsMu.Lock()
	update(&d.stats)
	d.statsMu.Unlock()
}

func (d *Dispatcher) Stats() Stats {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return d.stats
}

func (d *Dispatcher) LastError() error {
	return d.lastErrorValue()
}

func validClaim(message storage.OutboxMessage, tenantID, ownerID string) bool {
	return message.TenantID == tenantID && message.ID != "" && message.LockedBy == ownerID && message.Status == storage.OutboxProcessing && !message.LockedUntil.IsZero() && message.Attempt >= 1
}

func validOwnerID(value string) bool {
	if len(value) == 0 || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func waitInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitChannel(ctx context.Context, channel <-chan struct{}) bool {
	if channel == nil {
		return true
	}
	select {
	case <-channel:
		return true
	case <-ctx.Done():
		return false
	}
}
