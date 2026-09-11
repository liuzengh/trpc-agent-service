package application

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type RunnerOptions struct {
	InstanceID                                               string
	Providers                                                []string
	PollInterval, OperationTimeout, ClaimLease, DrainTimeout time.Duration
	MaxWorkers, PageSize, MaxPagesPerTick                    int
}

// DispatchFailures counts failed/over-budget use cases, never Provider certainty.
type RunnerSnapshot struct {
	Running, Quiesced            bool
	InFlight                     int
	Dispatched, DispatchFailures uint64
	ScanFailed                   bool
}
type Runner struct {
	reader                     DueAccountReader
	eligibility                AccountEligibility
	dispatcher                 AccountDispatcher
	maintainer                 *Maintainer
	options                    RunnerOptions
	mu                         sync.Mutex
	started, quiesced, running bool
	stop, drained              chan struct{}
	jobs                       chan domain.ClaimRequest
	active                     map[AccountKey]context.CancelFunc
	cursor                     map[string]string
	provider                   int
	dispatched, failures       uint64
	scanFailed                 bool
	cancelScan                 context.CancelFunc
}

func NewRunner(reader DueAccountReader, eligibility AccountEligibility, dispatcher AccountDispatcher, maintainer *Maintainer, o RunnerOptions) (*Runner, error) {
	if absent(reader) || absent(eligibility) || absent(dispatcher) || maintainer == nil {
		return nil, domain.ErrUnavailable
	}
	if o.Providers == nil {
		o.Providers = []string{"telegram", "wecom"}
	} else {
		o.Providers = append([]string(nil), o.Providers...)
	}
	seen := map[string]bool{}
	for _, p := range o.Providers {
		if (p != "telegram" && p != "wecom") || seen[p] {
			return nil, domain.ErrInvalid
		}
		seen[p] = true
	}
	if o.PollInterval == 0 {
		o.PollInterval = time.Second
	}
	if o.OperationTimeout == 0 {
		o.OperationTimeout = 10 * time.Second
	}
	if o.ClaimLease == 0 {
		o.ClaimLease = 15 * time.Second
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 15 * time.Second
	}
	if o.MaxWorkers == 0 {
		o.MaxWorkers = 4
	}
	if o.PageSize == 0 {
		o.PageSize = 32
	}
	if o.MaxPagesPerTick == 0 {
		o.MaxPagesPerTick = 4
	}
	if !runtimeIdentifier.MatchString(o.InstanceID) || len(o.Providers) == 0 || o.PollInterval < time.Millisecond || o.PollInterval > time.Hour || o.OperationTimeout < time.Millisecond || o.OperationTimeout > time.Minute || o.ClaimLease < 100*time.Millisecond || o.ClaimLease > 10*time.Minute || o.DrainTimeout < time.Millisecond || o.DrainTimeout > 2*time.Minute || o.MaxWorkers < 1 || o.MaxWorkers > 128 || o.PageSize < 1 || o.PageSize > 1000 || o.MaxPagesPerTick < 1 || o.MaxPagesPerTick > 100 {
		return nil, domain.ErrInvalid
	}
	return &Runner{reader: reader, eligibility: eligibility, dispatcher: dispatcher, maintainer: maintainer, options: o, stop: make(chan struct{}), drained: make(chan struct{}), jobs: make(chan domain.ClaimRequest), active: make(map[AccountKey]context.CancelFunc), cursor: make(map[string]string)}, nil
}

// Run owns a single lifecycle and a fixed worker set. A started dispatch gets an
// independent bounded context, so quiescence does not discard its result evidence.
func (r *Runner) Run(ctx context.Context) error {
	if ctx == nil {
		return domain.ErrInvalid
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return ErrRuntimeStarted
	}
	r.started = true
	if r.quiesced {
		r.mu.Unlock()
		return ErrRuntimeStopped
	}
	// Claim maintenance before admitting any account work. Two Runners cannot
	// report success while only one of them actually owns the maintenance loop.
	if !r.maintainer.started.CompareAndSwap(false, true) {
		r.quiesced = true
		close(r.stop)
		close(r.drained)
		r.mu.Unlock()
		return ErrRuntimeStarted
	}
	r.running = true
	r.mu.Unlock()
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.mu.Lock()
	r.cancelScan = cancel
	if r.quiesced {
		cancel()
	}
	r.mu.Unlock()
	var workers sync.WaitGroup
	workers.Add(r.options.MaxWorkers + 1)
	for n := 0; n < r.options.MaxWorkers; n++ {
		go func() { defer workers.Done(); r.worker(context.WithoutCancel(ctx)) }()
	}
	go func() { defer workers.Done(); _ = r.maintainer.run(scanCtx) }()
	ticker := time.NewTicker(r.options.PollInterval)
	defer ticker.Stop()
loop:
	for {
		if scanCtx.Err() != nil {
			break
		}
		select {
		case <-r.stop:
			break loop
		default:
		}
		r.scan(scanCtx)
		select {
		case <-r.stop:
			break loop
		case <-scanCtx.Done():
			break loop
		case <-ticker.C:
		}
	}
	r.Quiesce()
	cancel()
	close(r.jobs)
	go func() { workers.Wait(); r.mu.Lock(); r.running = false; r.mu.Unlock(); close(r.drained) }()
	drain, stop := context.WithTimeout(context.Background(), r.options.DrainTimeout)
	defer stop()
	if err := r.Drain(drain); err != nil {
		r.mu.Lock()
		for _, cancel := range r.active {
			if cancel != nil {
				cancel()
			}
		}
		r.mu.Unlock()
		return err
	}
	return nil
}

// Quiesce linearizes with worker admission under mu. Work admitted before that
// point is in flight; this does not promise to revoke an external side effect.
func (r *Runner) Quiesce() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.quiesced {
		r.quiesced = true
		close(r.stop)
		if r.cancelScan != nil {
			r.cancelScan()
		}
		if !r.started {
			close(r.drained)
		}
	}
}

// Drain waits for actual worker/maintenance completion, not merely Run's return.
// A timeout is retryable by the caller and never reports uncooperative work done.
func (r *Runner) Drain(ctx context.Context) error {
	if ctx == nil {
		return domain.ErrInvalid
	}
	r.mu.Lock()
	quiesced := r.quiesced
	r.mu.Unlock()
	if !quiesced {
		return domain.ErrInvalid
	}
	select {
	case <-r.drained:
		return nil
	default:
	}
	select {
	case <-r.drained:
		return nil
	case <-ctx.Done():
		return ErrRuntimeDrainTimeout
	}
}
func (r *Runner) Snapshot() RunnerSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RunnerSnapshot{Running: r.running, Quiesced: r.quiesced, InFlight: len(r.active), Dispatched: r.dispatched, DispatchFailures: r.failures, ScanFailed: r.scanFailed}
}
func (r *Runner) worker(base context.Context) {
	for request := range r.jobs {
		key := AccountKey{Provider: request.Provider, AccountID: request.AccountID}
		r.mu.Lock()
		if r.quiesced {
			delete(r.active, key)
			r.mu.Unlock()
			continue
		}
		ctx, cancel := context.WithTimeout(base, r.options.OperationTimeout)
		r.active[key] = cancel
		r.dispatched++
		r.mu.Unlock()
		_, err := r.dispatcher.DispatchAccount(ctx, request)
		timedOut := ctx.Err() != nil
		cancel()
		r.mu.Lock()
		delete(r.active, key)
		if err != nil || timedOut {
			r.failures++
		}
		r.mu.Unlock()
	}
}
func (r *Runner) scan(ctx context.Context) {
	failed := false
	defer func() { r.mu.Lock(); r.scanFailed = failed; r.mu.Unlock() }()
	// Rotate the first opportunity independently of the page budget. Otherwise a
	// budget divisible by the provider count always starts with the same provider,
	// which can repeatedly fill every worker before another provider is visited.
	firstProvider := r.provider
	r.provider = (r.provider + 1) % len(r.options.Providers)
	for n := 0; n < r.options.MaxPagesPerTick; n++ {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-r.stop:
			return
		default:
		}
		provider := r.options.Providers[(firstProvider+n)%len(r.options.Providers)]
		after := r.cursor[provider]
		op, cancel := context.WithTimeout(ctx, r.options.OperationTimeout)
		page, err := r.reader.ListDueAccounts(op, DueAccountQuery{Provider: provider, AfterAccountID: after, Limit: r.options.PageSize})
		timedOut := op.Err() != nil
		cancel()
		if err != nil || timedOut {
			failed = true
			continue
		}
		if len(page.Accounts) > r.options.PageSize {
			failed = true
			continue
		}
		ids := make([]string, 0, len(page.Accounts))
		valid := true
		for _, key := range page.Accounts {
			if key.Provider != provider {
				valid = false
			}
			ids = append(ids, key.AccountID)
		}
		if !valid || !validRuntimePage(ids, after, page.NextAccountID, page.Exhausted, r.options.PageSize) {
			failed = true
			continue
		}
		complete := true
		for _, key := range page.Accounts {
			if key.Provider != provider || !runtimeIdentifier.MatchString(key.AccountID) || key.AccountID <= after {
				failed = true
				complete = false
				break
			}
			r.mu.Lock()
			_, busy := r.active[key]
			full := len(r.active) >= r.options.MaxWorkers
			stopping := r.quiesced
			r.mu.Unlock()
			if stopping {
				return
			}
			if full && !busy {
				complete = false
				break
			}
			if busy {
				after = key.AccountID
				r.cursor[provider] = after
				continue
			}
			op, cancel := context.WithTimeout(ctx, r.options.OperationTimeout)
			eligible, err := r.eligibility.InspectAccount(op, key)
			timedOut := op.Err() != nil
			cancel()
			if err != nil || timedOut {
				failed = true
			}
			if err == nil && !timedOut && eligible.Eligible {
				var owner *domain.OwnerFence
				if eligible.Owner != nil {
					copy := *eligible.Owner
					owner = &copy
				}
				request := domain.ClaimRequest{Provider: provider, AccountID: key.AccountID, InstanceID: r.options.InstanceID, Owner: owner, Limit: 1, Lease: r.options.ClaimLease}
				if request.Validate() != nil {
					failed = true
				} else {
					r.mu.Lock()
					if r.quiesced {
						r.mu.Unlock()
						return
					}
					r.active[key] = nil
					select {
					case r.jobs <- request:
					default:
						delete(r.active, key)
						complete = false
					}
					r.mu.Unlock()
					if !complete {
						break
					}
				}
			}
			after = key.AccountID
			r.cursor[provider] = after
		}
		if complete {
			if page.Exhausted {
				r.cursor[provider] = ""
			} else {
				r.cursor[provider] = page.NextAccountID
			}
		}
	}
}
