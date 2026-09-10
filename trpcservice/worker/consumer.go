package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

const (
	defaultConsumerLeaseDuration = 30 * time.Second
	defaultConsumerPollInterval  = time.Second
	defaultConsumerConcurrency   = 4
	consumerCompletionTimeout    = 5 * time.Second
	consumerRetryInitial         = 250 * time.Millisecond
	consumerRetryMax             = 30 * time.Second
	consumerAckMaxAttempts       = 5
)

// JobExecutor executes one durable execution claimed by a Consumer.
type JobExecutor interface {
	Run(context.Context, execution.Job) (RunResult, error)
}

// ExecutionStore owns PostgreSQL execution transitions. Redis delivery is not
// authoritative: every transition is conditioned on the run token.
type ExecutionStore interface {
	Claim(context.Context, queue.Dispatch, queue.ClaimRequest) (queue.Claim, bool, error)
	Renew(context.Context, queue.Claim, time.Duration) (queue.Lease, error)
	Complete(context.Context, queue.Claim, queue.CompletionStatus) error
	CompleteUncertain(context.Context, queue.Claim, error) error
	WaitForApproval(context.Context, queue.Claim, string) error
	Retry(context.Context, queue.Claim, error) error
}

// ExecutionUsageRecorder durably accounts model usage before a claimed
// execution is completed. A terminal event may already have accounted and
// released the reservation in its own transaction; implementations must make
// repeated calls idempotent.
type ExecutionUsageRecorder interface {
	RecordExecutionUsage(context.Context, queue.Claim, RunResult, bool) error
}

// Consumer reads Redis Stream entries, claims their PostgreSQL execution, and
// acknowledges Redis only after the persistent transition is complete.
type Consumer struct {
	executor      JobExecutor
	stream        queue.Stream
	jobs          ExecutionStore
	owner         string
	leaseDuration time.Duration
	pollInterval  time.Duration
	concurrency   int
	retryDelay    func(int) time.Duration
	afterClaim    func(context.Context, queue.Claim)
	mu            sync.Mutex
	stopClaims    chan struct{}
	claimCancel   context.CancelFunc
}

// ConsumerOptions controls the bounded worker loop without introducing a
// second concurrency manager. Zero values use production defaults.
type ConsumerOptions struct {
	LeaseDuration time.Duration
	PollInterval  time.Duration
	Concurrency   int
	RetryDelay    func(int) time.Duration
	// AfterClaim is a test/fault-injection hook. Production callers should
	// leave it nil; it runs after the durable claim and before user code.
	AfterClaim func(context.Context, queue.Claim)
}

// NewConsumer creates a consumer for one stable worker identity.
func NewConsumer(executor JobExecutor, stream queue.Stream, jobs ExecutionStore, owner string) (*Consumer, error) {
	return NewConsumerWithOptions(executor, stream, jobs, owner, ConsumerOptions{})
}

// NewConsumerWithOptions creates a consumer with explicit lease, polling, and
// concurrency settings. Concurrency is process-local; PostgreSQL/Redis remain
// authoritative for cross-node ownership.
func NewConsumerWithOptions(
	executor JobExecutor,
	stream queue.Stream,
	jobs ExecutionStore,
	owner string,
	options ConsumerOptions,
) (*Consumer, error) {
	if executor == nil {
		return nil, errors.New("job executor is required")
	}
	if stream == nil {
		return nil, errors.New("dispatch stream is required")
	}
	if jobs == nil {
		return nil, errors.New("execution store is required")
	}
	if owner == "" {
		return nil, errors.New("worker owner is required")
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = defaultConsumerLeaseDuration
	}
	if options.PollInterval <= 0 {
		options.PollInterval = defaultConsumerPollInterval
	}
	if options.Concurrency <= 0 {
		options.Concurrency = defaultConsumerConcurrency
	}
	return &Consumer{
		executor:      executor,
		stream:        stream,
		jobs:          jobs,
		owner:         owner,
		leaseDuration: options.LeaseDuration,
		pollInterval:  options.PollInterval,
		concurrency:   options.Concurrency,
		retryDelay:    options.RetryDelay,
		afterClaim:    options.AfterClaim,
		stopClaims:    make(chan struct{}),
	}, nil
}

// StopClaiming stops reads while allowing a previously claimed run to drain.
func (c *Consumer) StopClaiming() {
	if c == nil {
		return
	}
	c.mu.Lock()
	select {
	case <-c.stopClaims:
		c.mu.Unlock()
		return
	default:
		close(c.stopClaims)
		cancel := c.claimCancel
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
}

// Run consumes deliveries until cancellation or StopClaiming. Redis and
// PostgreSQL outages stay local to this worker and use bounded backoff instead
// of terminating the consumer. Claimed jobs execute with bounded concurrency;
// the execution store preserves session-lane order across concurrent runs.
// Pending Redis deliveries are reclaimed after an owner dies.
func (c *Consumer) Run(ctx context.Context) error {
	if c == nil || c.executor == nil || c.stream == nil || c.jobs == nil {
		return errors.New("consumer is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	claimCtx, cancel, stopped := c.claimContext(ctx)
	if stopped {
		return nil
	}
	defer c.clearClaimContext(cancel)

	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup
	// Wait for in-flight executions before returning so their lease renewal
	// and PostgreSQL transitions are not abandoned mid-flight.
	defer wg.Wait()

	for {
		if c.claimingStopped() {
			return nil
		}
		delivery, err := c.receive(claimCtx)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			if delivery.ID != "" {
				// A delivery id with a decode/validation error identifies a
				// permanently malformed stream entry. Dead-letter it once so
				// one poison message cannot block the consumer forever.
				if deadErr := c.deadDelivery(claimCtx, delivery, err); deadErr != nil {
					if c.claimingStopped() && errors.Is(deadErr, context.Canceled) {
						return nil
					}
					return fmt.Errorf("dead-letter invalid dispatch: %w", deadErr)
				}
				continue
			}
			if c.claimingStopped() && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := delivery.Validate(); err != nil {
			if deadErr := c.deadDelivery(claimCtx, delivery, err); deadErr != nil {
				if c.claimingStopped() && errors.Is(deadErr, context.Canceled) {
					return nil
				}
				return fmt.Errorf("dead-letter invalid dispatch: %w", deadErr)
			}
			continue
		}
		// Reserve a slot before claiming the PostgreSQL execution. The receive
		// loop itself is not an execution and must not reduce the configured
		// worker concurrency by one.
		select {
		case sem <- struct{}{}:
		case <-claimCtx.Done():
			if c.claimingStopped() {
				return nil
			}
			return claimCtx.Err()
		}
		claimCtxWithTrace := platformtelemetry.Extract(claimCtx, map[string]string{
			"traceparent": delivery.Dispatch.TraceParent,
			"tracestate":  delivery.Dispatch.TraceState,
		})
		claim, found, err := c.claimExecution(claimCtxWithTrace, delivery.Dispatch)
		if err != nil {
			<-sem
			if c.claimingStopped() && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if !found {
			if err := c.ackDelivery(claimCtxWithTrace, delivery); err != nil {
				<-sem
				if c.claimingStopped() && errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("ack unavailable execution: %w", err)
			}
			<-sem
			continue
		}
		if err := claim.Validate(); err != nil {
			<-sem
			return fmt.Errorf("claimed execution: %w", err)
		}
		if c.afterClaim != nil {
			c.afterClaim(claimCtxWithTrace, claim)
		}
		deliveryCtx := platformtelemetry.Extract(ctx, map[string]string{
			"traceparent": delivery.Dispatch.TraceParent,
			"tracestate":  delivery.Dispatch.TraceState,
		})
		wg.Add(1)
		go func(runContext context.Context, claim queue.Claim, delivery queue.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			ack, err := c.executeClaim(runContext, claim)
			if err != nil {
				log.Printf("worker execution %s failed: %s", claim.Job.RequestID(), platformlog.SafeError(err))
			}
			if ack {
				if ackErr := c.ackDelivery(runContext, delivery); ackErr != nil && runContext.Err() == nil {
					log.Printf("ack execution %s failed: %s", claim.Job.RequestID(), platformlog.SafeError(ackErr))
				}
			} else {
				// Keep the Redis delivery pending when its durable transition was
				// not completed, but allow this process to reclaim it after the
				// local execution attempt has ended.
				c.stream.Release(delivery)
			}
		}(deliveryCtx, claim, delivery)
	}
}

func (c *Consumer) receive(ctx context.Context) (queue.Delivery, error) {
	for attempt := 0; ; attempt++ {
		delivery, err := c.stream.Receive(ctx, c.owner, c.pollInterval)
		if err == nil || errors.Is(err, context.DeadlineExceeded) || delivery.ID != "" {
			return delivery, err
		}
		if ctx.Err() != nil {
			return queue.Delivery{}, ctx.Err()
		}
		log.Printf("receive dispatch failed: %s", platformlog.SafeError(err))
		if err := waitForConsumerRetry(ctx, c.backoff(attempt)); err != nil {
			return queue.Delivery{}, err
		}
	}
}

func (c *Consumer) claimExecution(ctx context.Context, dispatch queue.Dispatch) (queue.Claim, bool, error) {
	for attempt := 0; ; attempt++ {
		claim, found, err := c.jobs.Claim(ctx, dispatch, queue.ClaimRequest{Owner: c.owner, LeaseDuration: c.leaseDuration})
		if err == nil {
			return claim, found, nil
		}
		if ctx.Err() != nil {
			return queue.Claim{}, false, ctx.Err()
		}
		log.Printf("claim execution %s failed: %s", dispatch.RequestID, platformlog.SafeError(err))
		if err := waitForConsumerRetry(ctx, c.backoff(attempt)); err != nil {
			return queue.Claim{}, false, err
		}
	}
}

func (c *Consumer) ackDelivery(ctx context.Context, delivery queue.Delivery) error {
	for attempt := 0; attempt < consumerAckMaxAttempts; attempt++ {
		if err := c.stream.Ack(ctx, delivery); err == nil {
			return nil
		} else if ctx.Err() != nil {
			c.releaseDelivery(delivery)
			return ctx.Err()
		} else {
			c.releaseDelivery(delivery)
			log.Printf("ack dispatch %s failed: %s", delivery.ID, platformlog.SafeError(err))
			if attempt == consumerAckMaxAttempts-1 {
				return fmt.Errorf("ack dispatch %s failed after %d attempts: %w", delivery.ID, consumerAckMaxAttempts, err)
			}
		}
		if err := waitForConsumerRetry(ctx, c.backoff(attempt)); err != nil {
			return err
		}
	}
	return errors.New("ack dispatch retry limit reached")
}

func (c *Consumer) releaseDelivery(delivery queue.Delivery) {
	// XACK failure leaves the Redis entry pending. Drop only this process's
	// ownership marker so XAUTOCLAIM can hand it to a live consumer instead
	// of permanently skipping it until process restart. Releasing before the
	// bounded-backoff retry is safe because the durable transition is idempotent.
	c.stream.Release(delivery)
}

func (c *Consumer) deadDelivery(ctx context.Context, delivery queue.Delivery, cause error) error {
	for attempt := 0; ; attempt++ {
		if err := c.stream.Dead(ctx, delivery, cause); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else {
			log.Printf("dead-letter dispatch %s failed: %s", delivery.ID, platformlog.SafeError(err))
		}
		if err := waitForConsumerRetry(ctx, c.backoff(attempt)); err != nil {
			return err
		}
	}
}

func (c *Consumer) backoff(attempt int) time.Duration {
	if c != nil && c.retryDelay != nil {
		if delay := c.retryDelay(attempt); delay > 0 {
			return delay
		}
	}
	return consumerRetryDelay(attempt)
}

func consumerRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := consumerRetryInitial
	for attempt > 0 && delay < consumerRetryMax {
		delay *= 2
		attempt--
	}
	if delay > consumerRetryMax {
		delay = consumerRetryMax
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func waitForConsumerRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = consumerRetryInitial
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (c *Consumer) claimContext(parent context.Context) (context.Context, context.CancelFunc, bool) {
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stopClaims:
		cancel()
		return ctx, cancel, true
	default:
		c.claimCancel = cancel
		return ctx, cancel, false
	}
}
func (c *Consumer) clearClaimContext(cancel context.CancelFunc) {
	c.mu.Lock()
	c.claimCancel = nil
	c.mu.Unlock()
	cancel()
}
func (c *Consumer) claimingStopped() bool {
	select {
	case <-c.stopClaims:
		return true
	default:
		return false
	}
}
func (c *Consumer) executeClaim(ctx context.Context, claim queue.Claim) (bool, error) {
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var err error
	runCtx, err = ContextWithJobLease(runCtx, claim.Lease)
	if err != nil {
		return false, fmt.Errorf("attach execution lease: %w", err)
	}
	runCtx = ContextWithFinalAttempt(runCtx, claim.FinalAttempt)
	done := make(chan error, 1)
	go c.renewLease(runCtx, cancelRun, claim, done)
	result, runErr := c.executor.Run(runCtx, claim.Job)
	cancelRun()
	if result.CleanupError != nil {
		log.Printf("execution cleanup failed request_id=%s: %s", result.Execution.RequestID, platformlog.SafeError(result.CleanupError))
	}
	leaseErr := <-done
	if errors.Is(leaseErr, queue.ErrLeaseLost) {
		return true, nil
	}
	if leaseErr != nil {
		return false, fmt.Errorf("renew execution lease: %w", leaseErr)
	}
	completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consumerCompletionTimeout)
	defer cancel()
	if ctx.Err() != nil {
		// The execution has not reached a durable terminal transition. Leave the
		// Redis delivery pending so lease recovery can finish it after shutdown.
		return false, ctx.Err()
	}
	if recorder, ok := c.jobs.(ExecutionUsageRecorder); ok {
		// Only the transaction that durably closes the execution may release the
		// reservation. For normal runs that is Complete/Retry below; a terminal
		// event sink can do both accounting and release before this point.
		releaseQuota := result.TerminalEventPersisted && !result.ApprovalPending
		if err := recorder.RecordExecutionUsage(completeCtx, claim, result, releaseQuota); err != nil {
			return false, fmt.Errorf("record execution usage: %w", err)
		}
	}
	if result.ApprovalPending {
		if result.ApprovalID == "" {
			return false, errors.New("approval continuation is not durable")
		}
		if err := c.jobs.WaitForApproval(completeCtx, claim, result.ApprovalID); errors.Is(err, queue.ErrLeaseLost) {
			return true, nil
		} else if err != nil {
			return false, fmt.Errorf("park execution for approval: %w", err)
		}
		return true, nil
	}
	if runErr != nil && errors.Is(runErr, queue.ErrLeaseLost) {
		// A stale worker must not convert its failed side-effect boundary into a
		// terminal FAILED state. The current owner will finish the execution.
		return true, nil
	}
	if runErr != nil || !result.RunnerCompleted {
		failure := runErr
		if failure == nil {
			failure = errors.New("runner completed without completion event")
		}
		// A Runner may already have called a Tool with an external side effect.
		// Do not automatically re-run it merely because the terminal event was
		// an error. Crash recovery remains at-least-once via lease expiry.
		if IsSideEffectUncertainError(failure) {
			if err := c.jobs.CompleteUncertain(completeCtx, claim, failure); errors.Is(err, queue.ErrLeaseLost) {
				return true, nil
			} else if err != nil {
				return false, fmt.Errorf("mark uncertain execution: %w", err)
			}
			return true, nil
		}
		if IsPermanentExecutionError(failure) ||
			(!IsRetryableExecutionError(failure) && result.RunnerStarted) {
			if err := c.jobs.Complete(completeCtx, claim, queue.CompletionFailed); errors.Is(err, queue.ErrLeaseLost) {
				return true, nil
			} else if err != nil {
				return false, fmt.Errorf("fail execution: %w", err)
			}
			return true, nil
		}
		if err := c.jobs.Retry(completeCtx, claim, failure); errors.Is(err, queue.ErrLeaseLost) {
			return true, nil
		} else if err != nil {
			return false, fmt.Errorf("retry execution: %w", err)
		}
		return true, nil
	}
	if err := c.jobs.Complete(completeCtx, claim, queue.CompletionSucceeded); errors.Is(err, queue.ErrLeaseLost) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("complete execution: %w", err)
	}
	return true, nil
}
func (c *Consumer) renewLease(ctx context.Context, cancel context.CancelFunc, claim queue.Claim, done chan<- error) {
	defer close(done)
	interval := c.leaseDuration / 2
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewTimeout := c.leaseDuration / 3
			if renewTimeout <= 0 {
				renewTimeout = time.Nanosecond
			}
			renewCtx, cancelRenew := context.WithTimeout(ctx, renewTimeout)
			lease, err := c.jobs.Renew(renewCtx, claim, c.leaseDuration)
			cancelRenew()
			if err != nil {
				if ctx.Err() == nil {
					cancel()
					done <- err
				}
				return
			}
			claim.Lease = lease
		}
	}
}

var _ JobExecutor = (*Worker)(nil)
