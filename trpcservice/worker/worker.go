// Package worker consumes reliable tasks and executes them with a multi-tenant
// Runtime. Phase 3 intentionally processes one task at a time.
package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type Executor interface {
	Ready(context.Context) error
	Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error)
}

type FencedExecutor interface {
	ExecuteFenced(context.Context, message.ExecutionTask, sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error)
}

type PersistenceExecutor interface {
	PersistenceRoute(context.Context, message.ExecutionTask) (persistence.Route, error)
	Persist(context.Context, persistence.Envelope) error
}

type TaskAuthorizer interface {
	AuthorizeTask(context.Context, message.ExecutionTask) error
}

type Worker struct {
	store      *messaging.Store
	executor   Executor
	consumer   string
	controller *scheduler.Controller

	running     atomic.Bool
	mu          sync.Mutex
	cancel      context.CancelFunc
	activeDone  chan struct{}
	activeLease *messaging.Lease
}

func New(store *messaging.Store, runtime Executor, consumer string) (*Worker, error) {
	return NewWithController(store, runtime, consumer, nil)
}

func NewWithController(store *messaging.Store, runtime Executor, consumer string, controller *scheduler.Controller) (*Worker, error) {
	if store == nil || runtime == nil {
		return nil, errors.New("messaging store and executor are required")
	}
	if consumer == "" {
		return nil, errors.New("worker consumer name is required")
	}
	if store.Config().SessionFencing == "strong" {
		if _, ok := runtime.(FencedExecutor); !ok {
			return nil, errors.New("strong session fencing requires a fenced executor")
		}
	}
	return &Worker{store: store, executor: runtime, consumer: consumer, controller: controller}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	if w.cancel != nil {
		w.mu.Unlock()
		cancel()
		return errors.New("worker is already running")
	}
	w.cancel = cancel
	w.mu.Unlock()
	defer func() {
		cancel()
		w.running.Store(false)
		w.mu.Lock()
		w.cancel = nil
		w.mu.Unlock()
	}()

	if err := w.store.Ready(runCtx); err != nil {
		return err
	}
	if w.controller != nil {
		if err := w.controller.Start(runCtx); err != nil {
			return err
		}
	}
	w.running.Store(true)
	defer func() {
		if w.controller != nil {
			_ = w.controller.Close()
		}
	}()
	for {
		if runCtx.Err() != nil {
			return nil
		}
		_, _ = w.store.PromoteRetries(runCtx, 32)
		_, _ = w.store.PromotePersistenceRetries(runCtx, 32)
		_, _ = w.store.PromoteSessionWait(runCtx, 32)
		if w.controller != nil {
			_, _ = w.controller.Promote(runCtx, 32)
			if err := w.controller.Ready(runCtx); err != nil {
				if !waitContext(runCtx, 250*time.Millisecond) {
					return nil
				}
				continue
			}
		}
		stale, err := w.store.ClaimStale(runCtx, w.consumer, 16)
		if err == nil {
			for _, delivery := range stale {
				_ = w.store.Recover(runCtx, delivery, w.consumer)
			}
		}

		delivery, err := w.store.ReadTask(runCtx, w.consumer, time.Second)
		if err != nil {
			if delivery.StreamID != "" {
				_ = w.store.Reject(context.Background(), delivery, "invalid_task")
				continue
			}
			if errors.Is(err, redis.Nil) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if runCtx.Err() != nil {
				return nil
			}
			if !waitContext(runCtx, 250*time.Millisecond) {
				return nil
			}
			continue
		}
		w.process(runCtx, delivery)
	}
}

func (w *Worker) process(runCtx context.Context, delivery messaging.Delivery) {
	ctx, span := telemetry.Start(telemetry.ExtractTraceParent(runCtx, delivery.Task.TraceParent), "worker.claim")
	defer span.End()
	runCtx = ctx
	w.mu.Lock()
	activeDone := make(chan struct{})
	w.activeDone = activeDone
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		if w.activeDone == activeDone {
			w.activeDone = nil
			w.activeLease = nil
		}
		w.mu.Unlock()
		close(activeDone)
	}()
	if err := delivery.Task.Validate(); err != nil {
		_ = w.store.Reject(context.Background(), delivery, "invalid_task")
		return
	}
	snapshot, err := w.store.Snapshot(runCtx, delivery.InboxID)
	if err != nil || delivery.InboxID != delivery.Task.InboxID() || snapshot.TaskID != delivery.Task.TaskID || !message.ConstantTimeDigestEqual(snapshot.Digest, delivery.Task.PayloadDigest) {
		_ = w.store.Reject(context.Background(), delivery, "invalid_task")
		return
	}
	if w.controller != nil && w.controller.Enabled() {
		defer func() {
			current, currentErr := w.store.Snapshot(context.Background(), delivery.InboxID)
			if currentErr == nil && current.Terminal() {
				w.controller.MarkCompleted(context.Background(), delivery)
			}
		}()
	}
	if snapshot.State == messaging.StatePersisting {
		w.processPersistenceDelivery(runCtx, delivery)
		return
	}
	if w.controller != nil && w.controller.Enabled() {
		admitted, admitErr := w.controller.Admit(runCtx, delivery)
		if admitErr != nil {
			return
		}
		if !admitted {
			return
		}
		w.controller.SetInflight(1)
		defer w.controller.SetInflight(0)
	}
	if authorizer, ok := w.executor.(TaskAuthorizer); ok {
		if authErr := authorizer.AuthorizeTask(runCtx, delivery.Task); authErr != nil {
			_ = w.store.Reject(context.Background(), delivery, governanceErrorCode(authErr))
			return
		}
	}
	lease, err := w.store.Begin(runCtx, delivery, w.consumer)
	if err != nil {
		if errors.Is(err, messaging.ErrSessionBusy) || errors.Is(err, messaging.ErrSessionWait) {
			_ = w.store.DeferSession(context.Background(), delivery, time.Now(), "session_wait")
			return
		}
		if errors.Is(err, messaging.ErrSessionStale) {
			_ = w.store.Reject(context.Background(), delivery, "session_stale")
			return
		}
		if errors.Is(err, messaging.ErrLeaseLost) {
			_ = w.store.RequeueAfterBeginFailure(context.Background(), delivery)
		}
		return
	}
	w.mu.Lock()
	w.activeLease = &lease
	w.mu.Unlock()
	if w.controller != nil && w.controller.Enabled() {
		w.controller.MarkRunning(runCtx, delivery)
	}
	execCtx, cancel := context.WithCancel(runCtx)
	heartbeats := 1
	if w.store.Config().SessionFencing == "strong" {
		heartbeats = 2
	}
	heartbeatDone := make(chan error, heartbeats)
	go w.taskHeartbeat(execCtx, cancel, lease, heartbeatDone)
	if heartbeats == 2 {
		go w.sessionHeartbeat(execCtx, cancel, lease, heartbeatDone)
	}
	var reply message.OutboundMessage
	var executeErr error
	var turnCommit sessionfence.TurnCommit
	var route persistence.Route
	var envelope persistence.Envelope
	var persistenceErr error
	persistentExecutor, hasPersistence := w.executor.(PersistenceExecutor)
	if hasPersistence {
		route, executeErr = persistentExecutor.PersistenceRoute(execCtx, delivery.Task)
		if executeErr == nil {
			executeErr = w.store.EnsureBackendFingerprint(execCtx, route)
		}
	}
	if executeErr == nil {
		if fenced, ok := w.executor.(FencedExecutor); ok && w.store.Config().SessionFencing == "strong" {
			reply, turnCommit, executeErr = fenced.ExecuteFenced(execCtx, delivery.Task, sessionfence.Fence{TaskID: delivery.Task.TaskID, SessionCoord: lease.SessionCoord, SessionSeq: lease.SessionSeq, LeaseEpoch: lease.Epoch, LockToken: lease.LockToken})
		} else {
			reply, executeErr = w.executor.Execute(execCtx, delivery.Task)
		}
	}
	if executeErr == nil && route.IsSQL() {
		envelope, persistenceErr = w.store.PreparePersistence(execCtx, lease, route, reply, turnCommit)
		if persistenceErr == nil {
			persistCtx, persistCancel := context.WithTimeout(execCtx, w.store.Config().PersistenceTimeout)
			persistenceErr = persistentExecutor.Persist(persistCtx, envelope)
			persistCancel()
		}
		status := "succeeded"
		if persistenceErr != nil {
			status = "error"
		}
		telemetry.RecordPersistence(execCtx, string(route.Fingerprint.Kind), status)
	}
	cancel()
	heartbeatFailed := false
	for i := 0; i < heartbeats; i++ {
		heartbeatErr := <-heartbeatDone
		if heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
			heartbeatFailed = true
		}
	}
	if heartbeatFailed {
		// The lease may have been taken over. Do not issue a retry/fail write
		// with an unknown fencing state; recovery owns the transition.
		return
	}

	transitionTimeout := 5 * time.Second
	if runCtx.Err() != nil && w.store.Config().ShutdownTimeout > 0 {
		transitionTimeout = w.store.Config().ShutdownTimeout
	}
	transitionCtx, transitionCancel := context.WithTimeout(context.WithoutCancel(runCtx), transitionTimeout)
	defer transitionCancel()
	if executeErr == nil && w.store.Config().SessionFencing == "strong" && route.IsSQL() {
		if turnCommit.SessionCoord == "" {
			_ = w.store.Fail(transitionCtx, lease, "session_commit_invalid")
			return
		}
		if persistenceErr != nil {
			if envelope.TaskID == "" {
				if errors.Is(persistenceErr, persistence.ErrInvalidEnvelope) {
					_ = w.store.Fail(transitionCtx, lease, "persistence_envelope_invalid")
				}
				return
			}
			w.handlePersistenceFailure(transitionCtx, lease, envelope, persistenceErr)
			return
		}
		_ = w.store.FinalizePersistence(transitionCtx, lease, envelope)
		return
	}
	if runCtx.Err() != nil || errors.Is(executeErr, context.Canceled) {
		w.retryOrFail(transitionCtx, lease, "worker_shutdown", true)
		return
	}
	if executeErr == nil {
		if w.store.Config().SessionFencing == "strong" {
			if turnCommit.SessionCoord == "" {
				_ = w.store.Fail(transitionCtx, lease, "session_commit_invalid")
				return
			}
			finalizeCtx, finalizeSpan := telemetry.Start(transitionCtx, "redis.finalize")
			if err := w.store.CompleteTurn(finalizeCtx, lease, reply, turnCommit); err != nil {
				finalizeSpan.End()
				return
			}
			finalizeSpan.End()
			return
		}
		_ = w.store.Complete(transitionCtx, lease, reply)
		return
	}
	code, retryable := classify(executeErr)
	if !retryable || delivery.Task.Attempt >= w.store.Config().MaxAttempts {
		_ = w.store.Fail(transitionCtx, lease, code)
		return
	}
	w.retryOrFail(transitionCtx, lease, code, false)
}

func (w *Worker) processPersistenceDelivery(runCtx context.Context, delivery messaging.Delivery) {
	persistentExecutor, ok := w.executor.(PersistenceExecutor)
	if !ok {
		return
	}
	lease, envelope, err := w.store.BeginPersistence(runCtx, delivery, w.consumer)
	if err != nil {
		return
	}
	w.mu.Lock()
	w.activeLease = &lease
	w.mu.Unlock()
	execCtx, cancel := context.WithCancel(runCtx)
	heartbeatDone := make(chan error, 2)
	go w.taskHeartbeat(execCtx, cancel, lease, heartbeatDone)
	go w.sessionHeartbeat(execCtx, cancel, lease, heartbeatDone)
	persistCtx, persistCancel := context.WithTimeout(execCtx, w.store.Config().PersistenceTimeout)
	persistErr := persistentExecutor.Persist(persistCtx, envelope)
	persistCancel()
	persistenceStatus := "succeeded"
	if persistErr != nil {
		persistenceStatus = "error"
	}
	telemetry.RecordPersistence(execCtx, string(envelope.BackendKind), persistenceStatus)
	cancel()
	heartbeatFailed := false
	for i := 0; i < 2; i++ {
		heartbeatErr := <-heartbeatDone
		if heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
			heartbeatFailed = true
		}
	}
	if heartbeatFailed {
		return
	}
	transitionCtx, transitionCancel := context.WithTimeout(context.WithoutCancel(runCtx), w.store.Config().PersistenceTimeout)
	defer transitionCancel()
	if persistErr == nil {
		_ = w.store.FinalizePersistence(transitionCtx, lease, envelope)
		return
	}
	w.handlePersistenceFailure(transitionCtx, lease, envelope, persistErr)
}

func (w *Worker) handlePersistenceFailure(ctx context.Context, lease messaging.Lease, envelope persistence.Envelope, persistErr error) {
	code, retryable := classifyPersistence(persistErr)
	if !retryable {
		_ = w.store.FailPersistence(ctx, lease, envelope, code)
		return
	}
	if envelope.PersistAttempt >= w.store.Config().PersistenceMaxAttempts {
		_ = w.store.FailPersistence(ctx, lease, envelope, "persistence_retry_exhausted")
		return
	}
	_, _ = w.store.DeferPersistence(ctx, lease, envelope, code)
}

func classifyPersistence(err error) (string, bool) {
	switch {
	case errors.Is(err, persistence.ErrFingerprintConflict):
		return "backend_fingerprint_conflict", false
	case errors.Is(err, persistence.ErrCommitDigestConflict):
		return "sql_commit_digest_conflict", false
	case errors.Is(err, persistence.ErrSessionSequenceConflict):
		return "sql_session_sequence_conflict", false
	case errors.Is(err, persistence.ErrInvalidEnvelope):
		return "persistence_envelope_invalid", false
	case errors.Is(err, persistence.ErrSchemaIncompatible):
		return "sql_schema_incompatible", false
	default:
		return "sql_unavailable", true
	}
}

func (w *Worker) taskHeartbeat(ctx context.Context, cancel context.CancelFunc, lease messaging.Lease, done chan<- error) {
	ticker := time.NewTicker(w.store.Config().HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ticker.C:
			if err := w.store.Heartbeat(ctx, lease); err != nil {
				telemetry.RecordHeartbeat(ctx, "task", "error")
				cancel()
				done <- err
				return
			}
			telemetry.RecordHeartbeat(ctx, "task", "succeeded")
		}
	}
}

func (w *Worker) sessionHeartbeat(ctx context.Context, cancel context.CancelFunc, lease messaging.Lease, done chan<- error) {
	ticker := time.NewTicker(w.store.Config().HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ticker.C:
			if err := w.store.SessionHeartbeat(ctx, lease); err != nil {
				telemetry.RecordHeartbeat(ctx, "session", "error")
				cancel()
				done <- err
				return
			}
			telemetry.RecordHeartbeat(ctx, "session", "succeeded")
		}
	}
}

func (w *Worker) retryOrFail(ctx context.Context, lease messaging.Lease, code string, immediate bool) {
	var err error
	if lease.Delivery.Task.Attempt >= w.store.Config().MaxAttempts {
		err = w.store.Fail(ctx, lease, code)
	} else {
		err = w.store.Retry(ctx, lease, code, immediate)
	}
	if err != nil && w.store.Config().SessionFencing == "strong" && immediate {
		w.releaseAfterAgentCancellation(lease)
	}
}

// releaseAfterAgentCancellation is the first of two permitted standalone
// release_session_lock call sites. It runs only after a canceled Agent could
// not complete its fenced Retry/Fail transition.
func (w *Worker) releaseAfterAgentCancellation(lease messaging.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), w.store.Config().ShutdownTimeout)
	defer cancel()
	_ = w.store.ReleaseSessionLock(ctx, lease)
}

func classify(err error) (string, bool) {
	switch {
	case errors.Is(err, governance.ErrActorForbidden):
		return "actor_forbidden", false
	case errors.Is(err, governance.ErrPolicyMissing):
		return "tenant_policy_missing", false
	case errors.Is(err, governance.ErrPolicyUnavailable):
		return "tenant_policy_unavailable", false
	case errors.Is(err, governance.ErrToolForbidden):
		return "tool_rejected", false
	case errors.Is(err, governance.ErrDangerousConfirmation):
		return "confirmation_required", false
	case errors.Is(err, control.ErrConfirmationNotFound), errors.Is(err, control.ErrConfirmationMismatch), errors.Is(err, governance.ErrInvalidConfirmationCmd):
		return "confirmation_denied", false
	case errors.Is(err, persistence.ErrFingerprintConflict):
		return "backend_fingerprint_conflict", false
	case errors.Is(err, persistence.ErrPostgresSummaryDisabled):
		return "postgres_summary_disabled", false
	case errors.Is(err, persistence.ErrSchemaIncompatible):
		return "sql_schema_incompatible", false
	case errors.Is(err, persistence.ErrBackendUnavailable):
		return "sql_unavailable", true
	case errors.Is(err, executor.ErrTurnTooLarge):
		return "session_turn_too_large", false
	case errors.Is(err, executor.ErrUnknownBinding), errors.Is(err, executor.ErrConfigurationUnavailable):
		return "configuration_unavailable", false
	case errors.Is(err, executor.ErrAgentTimeout):
		return "model_timeout", true
	case errors.Is(err, executor.ErrDependencyUnavailable), errors.Is(err, executor.ErrRunnerDraining):
		return "dependency_unavailable", true
	case errors.Is(err, executor.ErrEmptyAgentResponse):
		return "empty_agent_response", true
	default:
		return "agent_failed", true
	}
}

func governanceErrorCode(err error) string {
	code, _ := classify(err)
	if code == "agent_failed" {
		return "tenant_policy_unavailable"
	}
	return code
}

func (w *Worker) Ready(ctx context.Context) error {
	if !w.running.Load() {
		return errors.New("worker loop is not running")
	}
	if err := w.store.Ready(ctx); err != nil {
		return err
	}
	if err := w.executor.Ready(ctx); err != nil {
		return err
	}
	if w.controller != nil {
		if err := w.controller.Ready(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) Close() error {
	w.mu.Lock()
	cancel := w.cancel
	activeDone := w.activeDone
	activeLease := w.activeLease
	timeout := w.store.Config().ShutdownTimeout
	w.mu.Unlock()
	if cancel != nil {
		if w.controller != nil {
			_ = w.controller.MarkDraining(context.Background())
		}
		cancel()
	}
	if activeDone != nil {
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-activeDone:
		case <-timer.C:
			if activeLease != nil {
				w.releaseOnShutdownTimeout(*activeLease)
			}
		}
	}
	return nil
}

// releaseOnShutdownTimeout is the second permitted standalone
// release_session_lock call site. Shutdown actively yields only the exact
// token/task/epoch captured for the still-running turn.
func (w *Worker) releaseOnShutdownTimeout(lease messaging.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), w.store.Config().ShutdownTimeout)
	defer cancel()
	_ = w.store.ReleaseSessionLock(ctx, lease)
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
