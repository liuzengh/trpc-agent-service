package summary

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const failurePersistenceTimeout = 5 * time.Second

// Processor executes one bounded summary job. It is safe to run one instance
// per worker replica; the Store and Sink provide the cross-replica fences.
type Processor struct {
	Store          Store
	Sink           Sink
	Generator      Generator
	TargetResolver TargetResolver
	WorkerID       string
	LeaseTTL       time.Duration
	RetryBackoff   func(attempt int) time.Duration
	Clock          func() time.Time
}

func NewProcessor(store Store, sink Sink, generator Generator, workerID string, leaseTTL time.Duration) *Processor {
	return &Processor{
		Store: store, Sink: sink, Generator: generator,
		WorkerID: workerID, LeaseTTL: leaseTTL, Clock: time.Now,
	}
}

func (p *Processor) validate() error {
	// Interfaces can hold typed nil pointers. Treat those as unavailable at
	// the composition boundary instead of allowing RunOnce to panic when it
	// invokes a method through the nil interface value.
	if p == nil || nilInterface(p.Store) || nilInterface(p.Sink) || nilInterface(p.Generator) {
		return fmt.Errorf("%w: store, sink and generator are required", ErrStoreUnavailable)
	}
	if err := validateLeaseRequest(p.WorkerID, p.LeaseTTL); err != nil {
		return err
	}
	return nil
}

// RunOnce claims and processes at most one job. ErrNoWork is returned when no
// job is currently claimable; callers can use it to drive a bounded poller.
func (p *Processor) RunOnce(ctx context.Context) (Job, error) {
	if err := p.validate(); err != nil {
		return Job{}, err
	}
	ctx = nonNilContext(ctx)
	job, err := p.Store.Claim(ctx, p.WorkerID, p.LeaseTTL)
	if err != nil {
		return Job{}, err
	}

	// Keep the lease alive while a model or Session backend is slow. The
	// generator receives a child context so a lost lease stops cooperative
	// implementations before they publish a result.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var leaseMu sync.Mutex
	claimed := job
	heartbeatErr := make(chan error, 1)
	heartbeatDone := make(chan struct{})
	go p.heartbeat(runCtx, &leaseMu, &claimed, cancel, heartbeatErr, heartbeatDone)

	if job.TargetEventSequence == 0 || job.TargetResolutionLeaseVersion > 0 {
		if nilInterface(p.TargetResolver) {
			cancel()
			<-heartbeatDone
			if heartbeatFailure := readHeartbeatError(heartbeatErr); heartbeatFailure != nil {
				return job, heartbeatFailure
			}
			return p.fail(ctx, &leaseMu, &claimed, ErrTargetResolverUnavailable)
		}
		sequence, resolveErr := safeResolveTarget(runCtx, p.TargetResolver, job)
		if resolveErr == nil && sequence <= 0 {
			resolveErr = fmt.Errorf("%w: resolved target event sequence", ErrInvalidJob)
		}
		if resolveErr == nil {
			leaseMu.Lock()
			current := claimed
			leaseMu.Unlock()
			var bound Job
			bound, resolveErr = p.Store.ResolveTarget(runCtx, current, sequence)
			if resolveErr == nil {
				leaseMu.Lock()
				// Target sequences only move forward. A concurrent enqueue observed
				// by the heartbeat must not be overwritten by a slower bind result.
				if claimed.TargetEventSequence > bound.TargetEventSequence {
					bound = claimed
				}
				claimed = bound
				job = bound
				leaseMu.Unlock()
			}
		}
		if resolveErr != nil {
			cancel()
			<-heartbeatDone
			if heartbeatFailure := readHeartbeatError(heartbeatErr); heartbeatFailure != nil {
				if ctx.Err() != nil {
					return job, ctx.Err()
				}
				return job, heartbeatFailure
			}
			return p.fail(ctx, &leaseMu, &claimed, resolveErr)
		}
	}

	candidate, generationErr := safeGenerate(runCtx, p.Generator, job)
	cancel()
	<-heartbeatDone
	if heartbeatFailure := readHeartbeatError(heartbeatErr); heartbeatFailure != nil {
		if ctx.Err() != nil {
			return job, ctx.Err()
		}
		return job, heartbeatFailure
	}
	if errors.Is(generationErr, ErrSummaryNotDue) {
		leaseMu.Lock()
		latest := claimed
		leaseMu.Unlock()
		completed, err := p.Store.Complete(ctx, latest, job.TargetEventSequence)
		if err != nil {
			return Job{}, err
		}
		return completed, nil
	}
	if generationErr != nil {
		return p.fail(ctx, &leaseMu, &claimed, generationErr)
	}
	if err := candidate.Validate(); err != nil {
		return p.fail(ctx, &leaseMu, &claimed, err)
	}
	if candidate.Key != job.Key {
		return p.fail(ctx, &leaseMu, &claimed, ErrGeneratorMisScoped)
	}

	leaseMu.Lock()
	latest := claimed
	leaseMu.Unlock()
	var publish PublishResult
	var publishErr error
	if fencedSink, ok := p.Sink.(FencedSink); ok {
		publish, publishErr = fencedSink.PublishFenced(ctx, candidate, latest)
	} else {
		publish, publishErr = p.Sink.Publish(ctx, candidate)
	}
	if publishErr != nil && !errors.Is(publishErr, ErrSummaryStale) {
		return p.fail(ctx, &leaseMu, &claimed, publishErr)
	}
	observed := publish.Checkpoint.EventSequence
	if errors.Is(publishErr, ErrSummaryStale) {
		checkpoint, ok, err := p.Sink.Get(ctx, job.Key)
		if err != nil {
			return p.fail(ctx, &leaseMu, &claimed, err)
		}
		if !ok {
			return p.fail(ctx, &leaseMu, &claimed, ErrSummaryStale)
		}
		observed = checkpoint.EventSequence
	}
	leaseMu.Lock()
	latest = claimed
	leaseMu.Unlock()
	completed, err := p.Store.Complete(ctx, latest, observed)
	if err != nil {
		return Job{}, err
	}
	return completed, nil
}

func (p *Processor) heartbeat(
	ctx context.Context,
	leaseMu *sync.Mutex,
	claimed *Job,
	cancel context.CancelFunc,
	errCh chan<- error,
	done chan<- struct{},
) {
	defer close(done)
	interval := p.LeaseTTL / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			leaseMu.Lock()
			current := *claimed
			leaseMu.Unlock()
			renewed, err := p.Store.Renew(ctx, current, p.LeaseTTL)
			if err != nil {
				// RunOnce cancels the child context after generation so the
				// heartbeat cannot race with publication. A renewal already in
				// flight may observe that normal cancellation; it is not a lost
				// lease and must not turn a successful run into a retry.
				if ctx.Err() != nil {
					return
				}
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			leaseMu.Lock()
			if claimed.TargetEventSequence > renewed.TargetEventSequence {
				renewed.TargetEventSequence = claimed.TargetEventSequence
				renewed.AgentVersionID = claimed.AgentVersionID
			}
			*claimed = renewed
			leaseMu.Unlock()
		}
	}
}

func (p *Processor) fail(ctx context.Context, mu *sync.Mutex, claimed *Job, cause error) (Job, error) {
	mu.Lock()
	current := *claimed
	mu.Unlock()
	clock := p.Clock
	if clock == nil {
		clock = time.Now
	}
	next := clock().UTC().Add(p.retryDelay(current.Attempts))
	// The generation context is commonly already cancelled or expired at this
	// point. Reusing it would strand the row in PROCESSING until lease expiry.
	// Persist the terminal/retry decision through a short detached context while
	// retaining trace values; the Store's lease fence still rejects a replaced
	// worker.
	persistCtx, cancel := context.WithTimeout(
		context.WithoutCancel(nonNilContext(ctx)), failurePersistenceTimeout,
	)
	defer cancel()
	failed, err := p.Store.Fail(persistCtx, current, cause, next)
	if err != nil {
		return current, errors.Join(cause, err)
	}
	return failed, cause
}

func (p *Processor) retryDelay(attempt int) time.Duration {
	if p.RetryBackoff != nil {
		delay := p.RetryBackoff(attempt)
		if delay > 0 && delay <= 24*time.Hour {
			return delay
		}
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Second
	for i := 1; i < attempt && delay < 5*time.Minute; i++ {
		delay *= 2
	}
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func safeGenerate(ctx context.Context, generator Generator, job Job) (candidate Candidate, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			// Panic values can contain model prompts or credentials. Keep the
			// durable error a stable class; restricted diagnostics belong in a
			// process-level panic counter, not the tenant job row.
			err = errors.New("summary generator panicked")
		}
	}()
	return generator.Generate(ctx, job)
}

func safeResolveTarget(ctx context.Context, resolver TargetResolver, job Job) (sequence int64, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("summary target resolver panicked")
		}
	}()
	return resolver.ResolveTarget(ctx, job)
}

func readHeartbeatError(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	default:
		return nil
	}
}
