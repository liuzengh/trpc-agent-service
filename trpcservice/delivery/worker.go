package delivery

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

// WorkerConfig bounds polling, concurrency, leases, and retry backoff.
type WorkerConfig struct {
	Owner          string
	PollInterval   time.Duration
	ClaimTTL       time.Duration
	BatchSize      int
	MaxConcurrency int
	RetryBase      time.Duration
	RetryMax       time.Duration
}

// Worker claims durable Outbox rows and sends them through registered routes.
type Worker struct {
	store     Store
	router    RouteResolver
	limiter   Limiter
	telemetry *servicemetrics.Telemetry
	config    WorkerConfig

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
	work    sync.WaitGroup
	sem     chan struct{}
}

// NewWorker constructs a stopped Delivery Worker.
func NewWorker(store Store, router RouteResolver, limiter Limiter, telemetry *servicemetrics.Telemetry, config WorkerConfig) (*Worker, error) {
	if store == nil || router == nil || limiter == nil || config.Owner == "" {
		return nil, errors.New("delivery: store, router, limiter, and owner are required")
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.ClaimTTL <= 0 {
		config.ClaimTTL = 30 * time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 32
	}
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 8
	}
	if config.RetryBase <= 0 {
		config.RetryBase = time.Second
	}
	if config.RetryMax <= 0 {
		config.RetryMax = 5 * time.Minute
	}
	worker := &Worker{store: store, router: router, limiter: limiter, telemetry: telemetry, config: config, sem: make(chan struct{}, config.MaxConcurrency)}
	// Inject once while the route table is immutable. Dynamic production
	// resolvers perform the same injection when they create a versioned sender.
	for _, key := range router.Keys() {
		sender, err := router.Resolve(gateway.OutboundMessage{TenantID: key.TenantID, BindingID: key.BindingID})
		if err == nil {
			if aware, ok := sender.(channels.RateLimitAware); ok {
				aware.SetDeliveryLimiter(limiter)
			}
		}
	}
	return worker, nil
}

// Start begins polling. It implements trpcservice.Component.
func (worker *Worker) Start(parent context.Context) error {
	if worker == nil || parent == nil {
		return errors.New("delivery: worker and start context are required")
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.started {
		return errors.New("delivery: worker already started")
	}
	ctx, cancel := context.WithCancel(parent)
	worker.started, worker.cancel, worker.done = true, cancel, make(chan struct{})
	go worker.run(ctx)
	return nil
}

func (worker *Worker) run(ctx context.Context) {
	defer close(worker.done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			worker.poll(ctx)
			timer.Reset(worker.config.PollInterval)
		}
	}
}

func (worker *Worker) poll(ctx context.Context) {
	available := cap(worker.sem) - len(worker.sem)
	if available <= 0 {
		return
	}
	limit := min(worker.config.BatchSize, available)
	started := time.Now()
	bindings := worker.router.Keys()
	if len(bindings) == 0 {
		return
	}
	claims, err := worker.store.ClaimReady(ctx, bindings, worker.config.Owner, worker.config.ClaimTTL, limit)
	if err != nil {
		worker.telemetry.Request(ctx, servicemetrics.Labels{Operation: "outbox_claim", Status: "failed"}, time.Since(started), 0, 0)
		return
	}
	for _, claim := range claims {
		worker.sem <- struct{}{}
		worker.work.Add(1)
		go func(claim Claim) {
			defer worker.work.Done()
			defer func() { <-worker.sem }()
			worker.deliver(ctx, claim)
		}(claim)
	}
}

func (worker *Worker) deliver(parent context.Context, claim Claim) {
	message := claim.Message
	fields := servicemetrics.SpanFields{TenantID: message.TenantID, AppID: message.AppID, Channel: message.BindingID, RequestID: message.OutboxID, TraceID: message.TraceID}
	parent = worker.telemetry.Extract(parent, message.TraceContext)
	ctx, span := worker.telemetry.Start(parent, "outbox.deliver", fields)
	defer span.End()
	started := time.Now()
	success := false
	defer func() {
		worker.telemetry.Delivery(ctx, servicemetrics.Labels{TenantID: message.TenantID, AppID: message.AppID, Channel: message.BindingID, Operation: "outbox"}, success)
		worker.telemetry.Request(ctx, servicemetrics.Labels{TenantID: message.TenantID, AppID: message.AppID, Channel: message.BindingID, Operation: "outbox", Status: deliveryStatus(success)}, time.Since(started), 0, 0)
	}()

	sendCtx, cancelSend := context.WithCancel(ctx)
	renewCtx, cancelRenew := context.WithCancel(ctx)
	renewDone := make(chan error, 1)
	go worker.renew(renewCtx, cancelSend, claim, renewDone)
	stopRenew := func() error {
		cancelRenew()
		return <-renewDone
	}

	var sender channels.TextSender
	var err error
	if contextual, ok := worker.router.(ContextRouteResolver); ok {
		sender, err = contextual.ResolveContext(sendCtx, message)
	} else {
		sender, err = worker.router.Resolve(message)
	}
	if err == nil {
		if _, aware := sender.(channels.RateLimitAware); !aware {
			// Provider-aware senders charge once per physical API call (for
			// example, each text chunk). Generic senders are charged once for
			// the logical delivery here.
			err = worker.limiter.Wait(sendCtx, message)
		}
	}
	if err != nil {
		_ = stopRenew()
		worker.fail(claim, err, true)
		cancelSend()
		return
	}
	if err = worker.store.BeginSend(sendCtx, claim); err != nil {
		_ = stopRenew()
		cancelSend()
		return
	}
	err = sender.SendText(sendCtx, message)
	renewErr := stopRenew()
	cancelSend()
	if err == nil && renewErr != nil {
		err = renewErr
	}
	if err != nil {
		if outcomeUncertain(err) {
			updateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = worker.store.MarkUncertain(updateCtx, claim, err)
			cancel()
			return
		}
		worker.fail(claim, err, retryable(err))
		return
	}
	updateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = worker.store.MarkSent(updateCtx, claim)
	cancel()
	if err == nil {
		success = true
	}
}

func (worker *Worker) renew(ctx context.Context, cancelSend context.CancelFunc, claim Claim, done chan<- error) {
	interval := worker.config.ClaimTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			var err error
			claim, err = worker.store.Renew(ctx, claim, worker.config.ClaimTTL)
			if err != nil {
				cancelSend()
				done <- err
				return
			}
		}
	}
}

func (worker *Worker) fail(claim Claim, cause error, canRetry bool) {
	updateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, _ = worker.store.Fail(updateCtx, claim, cause, worker.retryAt(claim), canRetry)
	cancel()
}

func (worker *Worker) retryAt(claim Claim) time.Time {
	delay := worker.config.RetryBase
	for attempt := 1; attempt < claim.Attempt && delay < worker.config.RetryMax; attempt++ {
		delay *= 2
		if delay > worker.config.RetryMax {
			delay = worker.config.RetryMax
		}
	}
	digest := sha256.Sum256([]byte(claim.Message.TenantID + "\x00" + claim.Message.OutboxID))
	jitter := time.Duration(int64(delay) * int64(digest[0]) / 1280) // stable 0-19.9%
	return time.Now().UTC().Add(delay + jitter)
}

func retryable(err error) bool {
	var classified channels.RetryClassifier
	if errors.As(err, &classified) {
		return classified.DeliveryRetryable()
	}
	return true
}

func outcomeUncertain(err error) bool {
	var classified channels.OutcomeClassifier
	return errors.As(err, &classified) && classified.DeliveryOutcomeUncertain()
}

func deliveryStatus(success bool) string {
	if success {
		return "success"
	}
	return "failed"
}

// Close stops new claims and waits for all in-flight sends.
func (worker *Worker) Close(ctx context.Context) error {
	if worker == nil || ctx == nil {
		return errors.New("delivery: worker and close context are required")
	}
	worker.mu.Lock()
	if !worker.started {
		worker.mu.Unlock()
		return nil
	}
	cancel, done := worker.cancel, worker.done
	worker.mu.Unlock()
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	workDone := make(chan struct{})
	go func() {
		worker.work.Wait()
		close(workDone)
	}()
	select {
	case <-workDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
