// Package worker connects queue deliveries to the framework-independent execution core.
package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrInvalidConfig         = errors.New("worker: invalid configuration")
	ErrMissingAgent          = errors.New("worker: agent resolver is required")
	ErrDeliveryAction        = errors.New("worker: delivery action failed")
	ErrDrainTimeout          = errors.New("worker: drain timeout")
	ErrShutdownDeadline      = ErrDrainTimeout
	ErrNotStarted            = errors.New("worker: worker is not started")
	ErrAlreadyStarted        = errors.New("worker: worker is already started")
	ErrAgentResolution       = errors.New("worker: agent resolution failed")
	ErrDeliveryUnresolved    = errors.New("worker: delivery outcome unresolved")
	ErrVisibilityFailure     = errors.New("worker: delivery visibility failed")
	errActionAlreadyComplete = errors.New("worker: delivery action already completed")
)

// AgentResolver restores the immutable AgentSpec referenced by a queued job.
// It must not return mutable tenant or provider state shared across requests.
type AgentResolver interface {
	Resolve(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error)
}

type AgentResolverFunc func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error)

func (f AgentResolverFunc) Resolve(ctx context.Context, tc tenant.TenantContext, ref queue.AgentRefDTO) (agent.AgentSpec, error) {
	if f == nil {
		return agent.AgentSpec{}, ErrMissingAgent
	}
	return f(ctx, tc, ref)
}

// Config controls the bounded worker pool and one delivery's execution.
type Config struct {
	WorkerID                string
	Concurrency             int
	VisibilityTimeout       time.Duration
	LeaseTTL                time.Duration
	LeaseRenewInterval      time.Duration
	ReleaseTimeout          time.Duration
	RetryDelay              time.Duration
	ActionRetryAttempts     int
	ActionRetryBackoff      time.Duration
	VisibilityRetryAttempts int
	VisibilityRetryBackoff  time.Duration
	ReceiveBackoffInitial   time.Duration
	ReceiveBackoffMax       time.Duration
	ShutdownTimeout         time.Duration
	CleanupTimeout          time.Duration
	ResolveAgent            AgentResolver
}

func (c Config) withDefaults() (Config, error) {
	if c.WorkerID == "" || c.Concurrency < 1 || c.VisibilityTimeout <= 0 || c.LeaseTTL <= 0 || c.ResolveAgent == nil {
		return Config{}, ErrInvalidConfig
	}
	if c.LeaseRenewInterval == 0 {
		c.LeaseRenewInterval = c.LeaseTTL / 3
	}
	if c.LeaseRenewInterval <= 0 || c.LeaseRenewInterval >= c.LeaseTTL {
		return Config{}, fmt.Errorf("%w: lease renewal interval must be shorter than lease ttl", ErrInvalidConfig)
	}
	if c.RetryDelay < 0 || c.ActionRetryAttempts < 0 || c.ActionRetryBackoff < 0 || c.VisibilityRetryAttempts < 0 || c.VisibilityRetryBackoff < 0 || c.ReceiveBackoffInitial < 0 || c.ReceiveBackoffMax < 0 {
		return Config{}, fmt.Errorf("%w: retry and backoff settings must not be negative", ErrInvalidConfig)
	}
	if c.ActionRetryAttempts == 0 {
		c.ActionRetryAttempts = 3
	}
	if c.ActionRetryBackoff == 0 {
		c.ActionRetryBackoff = 5 * time.Millisecond
	}
	if c.VisibilityRetryAttempts == 0 {
		c.VisibilityRetryAttempts = 3
	}
	if c.VisibilityRetryBackoff == 0 {
		c.VisibilityRetryBackoff = 5 * time.Millisecond
	}
	if c.ReceiveBackoffInitial == 0 {
		c.ReceiveBackoffInitial = 5 * time.Millisecond
	}
	if c.ReceiveBackoffMax == 0 {
		c.ReceiveBackoffMax = time.Second
	}
	if c.ReceiveBackoffMax < c.ReceiveBackoffInitial {
		return Config{}, fmt.Errorf("%w: receive backoff max must be at least initial", ErrInvalidConfig)
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 5 * time.Second
	}
	if c.CleanupTimeout <= 0 {
		c.CleanupTimeout = 500 * time.Millisecond
	}
	return c, nil
}

type deliveryActionState string

const (
	deliveryReceived    deliveryActionState = "received"
	deliveryProcessing  deliveryActionState = "processing"
	deliveryAckPending  deliveryActionState = "ack_pending"
	deliveryAcked       deliveryActionState = "acked"
	deliveryNackPending deliveryActionState = "nack_pending"
	deliveryNacked      deliveryActionState = "nacked"
	deliveryUnresolved  deliveryActionState = "unresolved"
)

type deliveryRecord struct {
	delivery            queue.Delivery
	ctx                 context.Context
	retryCtx            context.Context
	actionCtx           context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	state               deliveryActionState
	lastAction          deliveryActionState
	actionErr           error
	visibilityErr       error
	completionPending   bool
	completionSucceeded bool
}

func (r *deliveryRecord) setProcessing() {
	r.mu.Lock()
	r.state = deliveryProcessing
	r.mu.Unlock()
}

func (r *deliveryRecord) setActionContext(ctx context.Context) {
	r.mu.Lock()
	r.actionCtx = ctx
	r.mu.Unlock()
}

func (r *deliveryRecord) actionContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.actionCtx != nil {
		return r.actionCtx
	}
	return r.retryCtx
}

func (r *deliveryRecord) actionSnapshot() (deliveryActionState, deliveryActionState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.lastAction, r.actionErr
}

func (r *deliveryRecord) beginRecoveryNack() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != deliveryUnresolved || r.lastAction == deliveryAckPending {
		return false
	}
	r.state = deliveryNackPending
	r.lastAction = deliveryNackPending
	return true
}

func (r *deliveryRecord) beginAction(pending deliveryActionState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
	case deliveryAcked:
		if pending == deliveryAckPending {
			return errActionAlreadyComplete
		}
		return ErrDeliveryAction
	case deliveryNacked:
		if pending == deliveryNackPending {
			return errActionAlreadyComplete
		}
		return ErrDeliveryAction
	case deliveryUnresolved:
		return errors.Join(ErrDeliveryUnresolved, r.actionErr)
	case deliveryAckPending, deliveryNackPending:
		return ErrDeliveryAction
	default:
		r.state = pending
		r.lastAction = pending
		return nil
	}
}

func (r *deliveryRecord) finishAction(success deliveryActionState, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.state = success
		r.actionErr = nil
		return
	}
	r.state = deliveryUnresolved
	r.actionErr = err
}

func (r *deliveryRecord) markUnresolved(err error) {
	r.mu.Lock()
	r.state = deliveryUnresolved
	r.actionErr = err
	r.mu.Unlock()
}

func (r *deliveryRecord) isUnresolved() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == deliveryUnresolved
}

func (r *deliveryRecord) actionState() deliveryActionState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

func (r *deliveryRecord) setVisibilityError(err error) {
	r.mu.Lock()
	if !r.completionSucceeded && r.visibilityErr == nil {
		r.visibilityErr = err
	}
	r.mu.Unlock()
}

func (r *deliveryRecord) beginCompletion() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.visibilityErr != nil {
		return r.visibilityErr
	}
	if r.completionPending || r.completionSucceeded {
		return ErrDeliveryAction
	}
	r.completionPending = true
	return nil
}

func (r *deliveryRecord) finishCompletion(err error) {
	r.mu.Lock()
	r.completionPending = false
	if err == nil {
		r.completionSucceeded = true
	}
	r.mu.Unlock()
}

func (r *deliveryRecord) visibilityError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.visibilityErr
}

// Worker consumes a fixed number of queue deliveries. Queue ownership remains
// with the Worker after Start and is released exactly once during Stop.
type Worker struct {
	queue      queue.JobQueue
	executor   *execution.Executor
	completion storage.AtomicCompletionCoordinator
	config     Config

	mu              sync.Mutex
	started         bool
	stopped         bool
	executionCtx    context.Context
	receiveCancel   context.CancelFunc
	lifecycleCancel context.CancelFunc
	shutdownCtx     context.Context
	stopDone        chan struct{}
	stopStarted     chan struct{}
	stopErr         error
	consumerDone    chan struct{}
	consumerRemain  int
	stopping        bool
	inFlight        map[string]*deliveryRecord
	closeOnce       sync.Once
	workers         sync.WaitGroup
}

func New(q queue.JobQueue, executor *execution.Executor, config Config) (*Worker, error) {
	return newWorkerInstance(q, executor, config, nil)
}

// NewWithAtomicCompletion constructs the durable completion mode. This mode
// requires an explicit coordinator and never falls back to Queue.Ack after a
// separate execution commit.
func NewWithAtomicCompletion(q queue.JobQueue, executor *execution.Executor, config Config, completion storage.AtomicCompletionCoordinator) (*Worker, error) {
	if completion == nil {
		return nil, ErrInvalidConfig
	}
	return newWorkerInstance(q, executor, config, completion)
}

func newWorkerInstance(q queue.JobQueue, executor *execution.Executor, config Config, completion storage.AtomicCompletionCoordinator) (*Worker, error) {
	if q == nil || executor == nil {
		return nil, ErrInvalidConfig
	}
	defaults, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Worker{queue: q, executor: executor, completion: completion, config: defaults, inFlight: make(map[string]*deliveryRecord), stopStarted: make(chan struct{})}, nil
}

// Start starts exactly one fixed consumer group.
func (w *Worker) Start(parent context.Context) error {
	if parent == nil {
		return ErrInvalidConfig
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		if w.stopped {
			return ErrAlreadyStarted
		}
		return ErrAlreadyStarted
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(parent)
	receiveCtx, receiveCancel := context.WithCancel(lifecycleCtx)
	w.executionCtx = lifecycleCtx
	w.lifecycleCancel = lifecycleCancel
	w.receiveCancel = receiveCancel
	w.consumerDone = make(chan struct{})
	w.consumerRemain = w.config.Concurrency
	w.started = true
	for i := 0; i < w.config.Concurrency; i++ {
		w.workers.Add(1)
		go w.consume(receiveCtx)
	}
	return nil
}

func (w *Worker) consume(root context.Context) {
	defer func() {
		w.workers.Done()
		w.mu.Lock()
		w.consumerRemain--
		if w.consumerRemain == 0 {
			close(w.consumerDone)
		}
		w.mu.Unlock()
	}()
	receiveFailures := 0
	for {
		delivery, err := w.queue.Receive(root, w.config.VisibilityTimeout)
		if err != nil {
			if root.Err() != nil || errors.Is(err, queue.ErrQueueClosed) {
				return
			}
			receiveFailures++
			if err := waitBackoff(root, exponentialBackoff(w.config.ReceiveBackoffInitial, w.config.ReceiveBackoffMax, receiveFailures)); err != nil {
				return
			}
			continue
		}
		receiveFailures = 0
		record := w.track(delivery, w.executionCtx)
		if w.isStopping() {
			continue
		}
		_ = w.process(record)
		if !record.isUnresolved() {
			w.untrack(record)
		}
	}
}

func (w *Worker) track(delivery queue.Delivery, root context.Context) *deliveryRecord {
	ctx, cancel := context.WithCancel(root)
	record := &deliveryRecord{delivery: delivery, ctx: ctx, retryCtx: root, actionCtx: root, cancel: cancel, state: deliveryReceived}
	record.setProcessing()
	w.mu.Lock()
	if w.stopping && w.shutdownCtx != nil {
		record.actionCtx = w.shutdownCtx
	}
	w.inFlight[delivery.DeliveryID] = record
	w.mu.Unlock()
	return record
}

func (w *Worker) isStopping() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopping
}

func (w *Worker) cancelInFlight() {
	w.mu.Lock()
	records := make([]*deliveryRecord, 0, len(w.inFlight))
	for _, record := range w.inFlight {
		records = append(records, record)
	}
	w.mu.Unlock()
	for _, record := range records {
		record.cancel()
	}
}

func (w *Worker) setActionContext(ctx context.Context) {
	w.mu.Lock()
	records := make([]*deliveryRecord, 0, len(w.inFlight))
	for _, record := range w.inFlight {
		records = append(records, record)
	}
	w.mu.Unlock()
	for _, record := range records {
		record.setActionContext(ctx)
	}
}

func (w *Worker) untrack(record *deliveryRecord) {
	record.cancel()
	w.mu.Lock()
	delete(w.inFlight, record.delivery.DeliveryID)
	w.mu.Unlock()
}

func (w *Worker) process(record *deliveryRecord) (processErr error) {
	visibilityDone := make(chan struct{})
	go w.renewVisibility(record, visibilityDone)
	defer func() {
		record.cancel()
		<-visibilityDone
	}()

	job, err := queue.DecodeJob(record.delivery.Envelope)
	if err == nil {
		err = record.delivery.Job.Validate()
	}
	if err != nil {
		return errors.Join(err, w.nack(record, false, err))
	}
	tc, err := job.Tenant.Restore()
	if err != nil {
		return errors.Join(err, w.nack(record, false, err))
	}
	spec, err := w.config.ResolveAgent.Resolve(record.ctx, tc, job.Agent)
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrAgentResolution, err)
		return errors.Join(err, w.nack(record, false, err))
	}
	if spec.TenantID != tc.TenantID || spec.AgentAppID != tc.AgentAppID || spec.Version != job.Agent.Version {
		err = fmt.Errorf("%w: resolved agent does not match job", agent.ErrTenantMismatch)
		return errors.Join(err, w.nack(record, false, err))
	}
	request := execution.ExecutionRequest{
		Job: job, Agent: spec, History: historyFromDTO(job.History), OwnerID: w.config.WorkerID,
		LeaseTTL: w.config.LeaseTTL, RenewInterval: w.config.LeaseRenewInterval,
		ReleaseTimeout: w.config.ReleaseTimeout,
	}
	var result execution.ExecutionResult
	var executeErr error
	if w.completion == nil {
		result, executeErr = w.executor.Execute(record.ctx, request)
	} else {
		result, executeErr = w.executor.ExecuteWithCompletion(record.ctx, request, func(ctx context.Context, commit execution.ExecutionCommit) error {
			if err := record.beginCompletion(); err != nil {
				return err
			}
			completionRequest, err := execution.AtomicCompletionRequestFor(commit, record.delivery)
			if err == nil {
				err = w.completion.CommitResultAndAck(ctx, completionRequest)
			}
			record.finishCompletion(err)
			record.cancel()
			return err
		})
	}
	if errors.Is(executeErr, storage.ErrCompletionOutcomeUnknown) {
		record.cancel()
		record.markUnresolved(executeErr)
		return executeErr
	}
	if visibilityErr := record.visibilityError(); visibilityErr != nil {
		executeErr = errors.Join(executeErr, visibilityErr)
	}
	if executeErr != nil || result.State != execution.StateSucceeded {
		record.cancel()
		return errors.Join(executeErr, w.nack(record, w.shouldRequeue(executeErr, result), executeErr))
	}
	if w.completion != nil {
		return nil
	}
	record.cancel()
	if err := w.ack(record); err != nil {
		return err
	}
	return nil
}

func (w *Worker) renewVisibility(record *deliveryRecord, done chan<- struct{}) {
	defer close(done)
	interval := w.config.VisibilityTimeout / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	failures := 0
	for {
		if err := waitBackoff(record.ctx, interval); err != nil {
			return
		}
		extendCtx, cancel := context.WithTimeout(record.ctx, w.config.CleanupTimeout)
		err := w.queue.ExtendVisibility(extendCtx, record.delivery, w.config.VisibilityTimeout)
		cancel()
		if err == nil {
			failures = 0
			continue
		}
		failures++
		if failures >= w.config.VisibilityRetryAttempts {
			record.setVisibilityError(errors.Join(ErrVisibilityFailure, err))
			record.cancel()
			return
		}
		if err := waitBackoff(record.ctx, exponentialBackoff(w.config.VisibilityRetryBackoff, w.config.VisibilityRetryBackoff, failures)); err != nil {
			return
		}
	}
}

func (w *Worker) ack(record *deliveryRecord) error {
	if err := record.beginAction(deliveryAckPending); err != nil {
		if errors.Is(err, errActionAlreadyComplete) {
			return nil
		}
		return err
	}
	actionErr := w.retryAction(record, func(ctx context.Context) error {
		if err := w.queue.Ack(ctx, record.delivery); err != nil {
			return fmt.Errorf("%w: ack: %w", ErrDeliveryAction, err)
		}
		return nil
	}, deliveryAcked)
	if actionErr == nil {
		return nil
	}
	return w.unresolvedAction(record, actionErr)
}

func (w *Worker) nack(record *deliveryRecord, requeue bool, cause error) error {
	if err := record.beginAction(deliveryNackPending); err != nil {
		if errors.Is(err, errActionAlreadyComplete) {
			return nil
		}
		return err
	}
	actionErr := w.retryAction(record, func(ctx context.Context) error {
		err := w.queue.Nack(ctx, record.delivery, queue.NackOptions{Requeue: requeue, RetryAfter: w.config.RetryDelay, Reason: safeNackReason(cause)})
		if err != nil {
			return fmt.Errorf("%w: nack: %w", ErrDeliveryAction, err)
		}
		return nil
	}, deliveryNacked)
	if actionErr == nil {
		return nil
	}
	return w.unresolvedAction(record, actionErr)
}

func (w *Worker) retryAction(record *deliveryRecord, action func(context.Context) error, success deliveryActionState) error {
	var actionErr error
	actionCtx := record.actionContext()
	for attempt := 1; attempt <= w.config.ActionRetryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(actionCtx, w.config.CleanupTimeout)
		actionErr = action(ctx)
		cancel()
		if actionErr == nil {
			record.finishAction(success, nil)
			return nil
		}
		if attempt < w.config.ActionRetryAttempts {
			if err := waitBackoff(actionCtx, exponentialBackoff(w.config.ActionRetryBackoff, w.config.ActionRetryBackoff, attempt)); err != nil {
				break
			}
		}
	}
	record.finishAction(deliveryUnresolved, actionErr)
	return actionErr
}

func (w *Worker) unresolvedAction(record *deliveryRecord, actionErr error) error {
	recoveryErr := w.recoverVisibility(record)
	unresolvedErr := errors.Join(ErrDeliveryUnresolved, actionErr, recoveryErr)
	record.markUnresolved(unresolvedErr)
	return unresolvedErr
}

func (w *Worker) recoverVisibility(record *deliveryRecord) error {
	var recoveryErr error
	actionCtx := record.actionContext()
	for attempt := 1; attempt <= w.config.VisibilityRetryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(actionCtx, w.config.CleanupTimeout)
		recoveryErr = w.queue.ExtendVisibility(ctx, record.delivery, w.config.VisibilityTimeout)
		cancel()
		if recoveryErr == nil {
			return nil
		}
		if attempt < w.config.VisibilityRetryAttempts {
			if err := waitBackoff(actionCtx, exponentialBackoff(w.config.VisibilityRetryBackoff, w.config.VisibilityRetryBackoff, attempt)); err != nil {
				break
			}
		}
	}
	return errors.Join(ErrVisibilityFailure, recoveryErr)
}

func waitBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func exponentialBackoff(initial, maximum time.Duration, failures int) time.Duration {
	if initial <= 0 {
		return 0
	}
	if maximum <= 0 || maximum < initial {
		maximum = initial
	}
	backoff := initial
	for i := 1; i < failures && backoff < maximum; i++ {
		if backoff > maximum/2 {
			return maximum
		}
		backoff *= 2
	}
	if backoff > maximum {
		return maximum
	}
	return backoff
}

func safeNackReason(cause error) string {
	const generic = "execution_failed"
	if cause == nil {
		return generic
	}
	if errors.Is(cause, queue.ErrInvalidJob) || errors.Is(cause, queue.ErrInvalidEnvelope) {
		return "tenant_validation_failed"
	}
	if errors.Is(cause, agent.ErrProducerIncomplete) {
		return "producer_incomplete"
	}
	if errors.Is(cause, agent.ErrDrainTimeout) {
		return "drain_timeout"
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return "execution_deadline"
	}
	if errors.Is(cause, context.Canceled) {
		return "execution_canceled"
	}
	if errors.Is(cause, agent.ErrFrameworkFailure) {
		return "framework_failure"
	}
	if errors.Is(cause, ErrVisibilityFailure) {
		return "delivery_visibility_failed"
	}
	var providerErr *agent.ProviderResponseError
	if errors.As(cause, &providerErr) || errors.Is(cause, agent.ErrProviderFailure) {
		return "provider_response_error"
	}
	var executionErr *execution.ExecutionError
	if errors.As(cause, &executionErr) {
		switch executionErr.Class {
		case execution.FailureLeaseLost:
			return "lease_lost"
		case execution.FailurePermanent:
			return "execution_permanent_failure"
		case execution.FailureCanceled:
			return "execution_canceled"
		case execution.FailureRetryable:
			return "execution_retryable_failure"
		}
	}
	if errors.Is(cause, agent.ErrTenantMismatch) || errors.Is(cause, agent.ErrInvalidSpec) || errors.Is(cause, ErrAgentResolution) {
		return "tenant_validation_failed"
	}
	return generic
}

func historyFromDTO(messages []queue.MessageDTO) []agent.Message {
	if len(messages) == 0 {
		return nil
	}
	history := make([]agent.Message, len(messages))
	for i, message := range messages {
		calls := make([]agent.ToolCall, len(message.ToolCalls))
		for j, call := range message.ToolCalls {
			calls[j] = agent.ToolCall{ID: call.ID, Name: call.Name, Arguments: call.Arguments}
		}
		history[i] = agent.Message{ID: message.ID, Role: message.Role, Content: message.Content, CreatedAt: message.CreatedAt, ToolID: message.ToolID, ToolName: message.ToolName, ToolCalls: calls}
	}
	return history
}

func (w *Worker) shouldRequeue(err error, result execution.ExecutionResult) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, agent.ErrProducerIncomplete) || errors.Is(err, agent.ErrDrainTimeout) {
		return true
	}
	if errors.Is(err, queue.ErrInvalidJob) || errors.Is(err, queue.ErrInvalidEnvelope) || errors.Is(err, agent.ErrInvalidSpec) || errors.Is(err, agent.ErrTenantMismatch) || errors.Is(err, execution.ErrInvalidRequest) || errors.Is(err, execution.ErrAgentVersionMismatch) || errors.Is(err, ErrAgentResolution) {
		return false
	}
	return result.Failure != execution.FailurePermanent
}

// Stop stops receiving, drains in-flight work until the bounded shutdown
// deadline, then cancels remaining execution and recovers outstanding deliveries.
func (w *Worker) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	w.mu.Lock()
	if !w.started {
		w.mu.Unlock()
		return ErrNotStarted
	}
	if w.stopDone != nil {
		done := w.stopDone
		w.mu.Unlock()
		select {
		case <-done:
			return w.stopResult()
		case <-ctx.Done():
			return errors.Join(ErrDrainTimeout, ctx.Err())
		}
	}
	stopDone := make(chan struct{})
	w.stopDone = stopDone
	w.stopping = true
	close(w.stopStarted)
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, w.config.ShutdownTimeout)
	w.shutdownCtx = shutdownCtx
	receiveCancel := w.receiveCancel
	lifecycleCancel := w.lifecycleCancel
	consumerDone := w.consumerDone
	for _, record := range w.inFlight {
		record.setActionContext(shutdownCtx)
	}
	w.mu.Unlock()

	if receiveCancel != nil {
		receiveCancel()
	}
	shutdownErr := error(nil)
	forced := false
	if !waitForDone(shutdownCtx, consumerDone) {
		forced = true
		w.cancelInFlight()
		if lifecycleCancel != nil {
			lifecycleCancel()
		}
		select {
		case <-consumerDone:
			shutdownErr = errors.Join(ErrDrainTimeout, shutdownCtx.Err())
		default:
			shutdownErr = errors.Join(ErrDrainTimeout, shutdownCtx.Err(), w.outstandingError())
			go w.finishStopAfterWorkers()
			shutdownCancel()
			return w.finishStop(shutdownErr, stopDone)
		}
	}
	if lifecycleCancel != nil {
		lifecycleCancel()
	}
	if forced {
		shutdownErr = errors.Join(ErrDrainTimeout, shutdownCtx.Err(), shutdownErr)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), w.config.CleanupTimeout)
	w.setActionContext(cleanupCtx)
	shutdownErr = errors.Join(shutdownErr, w.nackOutstanding(cleanupCtx))
	closeErr := w.closeQueue()
	shutdownErr = errors.Join(shutdownErr, closeErr, w.outstandingError())
	cleanupCancel()
	shutdownCancel()
	return w.finishStop(shutdownErr, stopDone)
}

func (w *Worker) finishStopAfterWorkers() {
	w.workers.Wait()
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), w.config.CleanupTimeout)
	w.setActionContext(cleanupCtx)
	_ = w.nackOutstanding(cleanupCtx)
	_ = w.closeQueue()
	cleanupCancel()
}

func (w *Worker) finishStop(err error, done chan struct{}) error {
	w.mu.Lock()
	w.stopErr = err
	w.stopped = true
	close(done)
	w.mu.Unlock()
	return err
}

func (w *Worker) stopResult() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopErr
}

func waitForDone(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *Worker) closeQueue() error {
	var closeErr error
	w.closeOnce.Do(func() {
		closeErr = w.queue.Close()
	})
	return closeErr
}

func (w *Worker) nackOutstanding(ctx context.Context) error {
	w.mu.Lock()
	records := make([]*deliveryRecord, 0, len(w.inFlight))
	for _, record := range w.inFlight {
		records = append(records, record)
	}
	w.mu.Unlock()
	var recoveryErr error
	for _, record := range records {
		state, lastAction, actionErr := record.actionSnapshot()
		switch state {
		case deliveryAcked, deliveryNacked:
			w.untrack(record)
		case deliveryAckPending, deliveryNackPending:
			err := errors.Join(ErrDeliveryUnresolved, ErrDeliveryAction, actionErr)
			record.markUnresolved(err)
			recoveryErr = errors.Join(recoveryErr, err)
		case deliveryUnresolved:
			if lastAction == deliveryAckPending {
				visibilityErr := w.recoverVisibility(record)
				err := errors.Join(ErrDeliveryUnresolved, actionErr, visibilityErr)
				record.markUnresolved(err)
				recoveryErr = errors.Join(recoveryErr, err)
				continue
			}
			if !record.beginRecoveryNack() {
				err := errors.Join(ErrDeliveryUnresolved, actionErr)
				recoveryErr = errors.Join(recoveryErr, err)
				continue
			}
			if err := w.retryAction(record, func(actionCtx context.Context) error {
				if err := w.queue.Nack(actionCtx, record.delivery, queue.NackOptions{Requeue: true, RetryAfter: w.config.RetryDelay, Reason: safeNackReason(context.Canceled)}); err != nil {
					return fmt.Errorf("%w: nack: %w", ErrDeliveryAction, err)
				}
				return nil
			}, deliveryNacked); err != nil {
				err = errors.Join(ErrDeliveryUnresolved, actionErr, err)
				record.markUnresolved(err)
				recoveryErr = errors.Join(recoveryErr, err)
			} else {
				w.untrack(record)
			}
		default:
			if err := w.nack(record, true, context.Canceled); err != nil {
				recoveryErr = errors.Join(recoveryErr, err)
			} else {
				w.untrack(record)
			}
		}
	}
	return errors.Join(recoveryErr, ctx.Err())
}

func (w *Worker) outstandingError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.inFlight) == 0 {
		return nil
	}
	ids := make([]string, 0, len(w.inFlight))
	for id, record := range w.inFlight {
		state := record.actionState()
		ids = append(ids, fmt.Sprintf("%s(%s)", id, state))
	}
	return fmt.Errorf("%w: deliveries=%s", ErrDeliveryUnresolved, strings.Join(ids, ","))
}
