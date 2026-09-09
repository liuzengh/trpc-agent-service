package agent

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

var tracer = otel.Tracer("trpc-agent-service/worker")

// errSessionLockLost cancels a run whose session lease was taken over. The lock
// is what keeps one message from being processed twice across replicas, so once
// it is gone the in-flight generation must stop: continuing spends tokens a peer
// is already spending and emits a second reply for one message.
var errSessionLockLost = errors.New("session lock lost")

func processAttr(msg channels.InboundMessage) otelmetric.MeasurementOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
	)
}

// Processor handles one inbound message and produces a reply.
// Runner-backed implementations must consume the framework event channel to
// completion, and drain it after context cancellation, to avoid goroutine
// leaks.
type Processor interface {
	Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error)
}

// EchoProcessor echoes the input verbatim. It exercises the pipeline end to
// end without LLM access and serves as the fallback when no model key is
// configured.
type EchoProcessor struct{}

// Process implements Processor.
func (EchoProcessor) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{
		Channel:    msg.Channel,
		MsgID:      msg.MsgID,
		SessionKey: msg.SessionKey,
		UserID:     msg.UserID,
		ChatID:     msg.ChatID,
		BindingID:  msg.BindingID,
		ReplyToken: msg.ReplyToken,
		Text:       "echo: " + msg.Text,
		TenantID:   msg.TenantID,
		TraceID:    msg.TraceID,
		ReceivedAt: msg.ReceivedAt,
	}, nil
}

// Worker consumes the inbound stream as part of consumer group "workers": on
// success the reply is enqueued to the outbound stream and the message is
// Acked; on failure the message stays pending for a surviving node to take
// over via XCLAIM (a crash loses no messages).
//
// Message-level auditing is owned by the guardrail (the Guarded processor),
// which is the only place that knows the decision behind each reply.
type Worker struct {
	Stream    *storage.Stream
	Lock      *storage.Lock // nil disables session locking (single-replica dev)
	Processor Processor
	// Processed is the execution-layer idempotency marker: redelivered
	// messages skip reprocessing once done. Nil disables it.
	Processed *storage.ProcessedMarker
	Name      string // consumer name identifying pending ownership (e.g. hostname-pid)

	InStream  string // empty means storage.StreamInbound
	OutStream string // empty means storage.StreamOutbound

	// ReapInterval is how often pending messages are scanned for takeover.
	// MaxIdle is how long a message must stay pending before it counts as
	// orphaned; it must exceed the p95 processing time, or healthy in-flight
	// work would be double-processed. MaxAttempts caps redeliveries before a
	// message is dead-lettered.
	ReapInterval time.Duration
	MaxIdle      time.Duration
	MaxAttempts  int64

	// LockTTL is the session lock lease, renewed by the watchdog every
	// TTL/3 while processing runs. LockWait is how long a message spins for
	// the lock before being re-queued.
	LockTTL  time.Duration
	LockWait time.Duration

	// DrainTimeout bounds the graceful shutdown drain: after Run's ctx is
	// canceled the in-flight message keeps processing until it finishes or
	// this timeout forces cancellation.
	DrainTimeout time.Duration
}

func (w *Worker) reapInterval() time.Duration {
	if w.ReapInterval > 0 {
		return w.ReapInterval
	}
	return 30 * time.Second
}

func (w *Worker) maxIdle() time.Duration {
	if w.MaxIdle > 0 {
		return w.MaxIdle
	}
	// Same number, two roles: the takeover delay for a left-pending message
	// (busy session) and the safety bound against claiming an in-flight run.
	// Must exceed the worst-case processing time - model timeout x retries
	// + retry backoff + the approved-tool timeout (60s x 2 + ~8s + 60s ≈
	// 190s); 4min keeps slack above that while bounding the delay.
	return 4 * time.Minute
}

func (w *Worker) maxAttempts() int64 {
	if w.MaxAttempts > 0 {
		return w.MaxAttempts
	}
	return 5
}

func (w *Worker) lockTTL() time.Duration {
	if w.LockTTL > 0 {
		return w.LockTTL
	}
	return 10 * time.Second
}

func (w *Worker) lockWait() time.Duration {
	if w.LockWait > 0 {
		return w.LockWait
	}
	return 15 * time.Second
}

func (w *Worker) drainTimeout() time.Duration {
	if w.DrainTimeout > 0 {
		return w.DrainTimeout
	}
	return 2 * time.Minute
}

func (w *Worker) inStream() string {
	if w.InStream != "" {
		return w.InStream
	}
	return storage.StreamInbound
}

func (w *Worker) outStream() string {
	if w.OutStream != "" {
		return w.OutStream
	}
	return storage.StreamOutbound
}

// Run consumes until ctx is canceled; a nil return means a clean shutdown.
// Every reapInterval it also takes over pending messages orphaned by crashed
// consumers (XCLAIM semantics via XAUTOCLAIM).
//
// Graceful drain: on shutdown the worker stops pulling new messages, and the
// in-flight message keeps its own process context — it
// finishes (or force-cancels at DrainTimeout) before Run returns, so a
// rolling update does not interrupt a session mid-run.
func (w *Worker) Run(ctx context.Context) error {
	// processCtx outlives the stop signal: handlers use it instead of ctx.
	processCtx, stopProcessing := context.WithCancel(context.Background())
	defer stopProcessing()
	go func() {
		<-ctx.Done()
		time.AfterFunc(w.drainTimeout(), stopProcessing)
	}()

	lastReap := time.Now()
	for {
		if ctx.Err() != nil {
			return nil
		}

		if time.Since(lastReap) >= w.reapInterval() {
			w.reap(processCtx)
			lastReap = time.Now()
		}

		msgs, err := w.Stream.Read(ctx, w.inStream(), "workers", w.Name, 10, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return nil // read error during shutdown — exit cleanly
			}
			// Redis briefly unavailable: back off and retry instead of
			// killing the worker.
			plog.Warnf("worker %s read inbound: %v", w.Name, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		for _, m := range msgs {
			w.handle(processCtx, m)
		}
	}
}

func (w *Worker) handle(ctx context.Context, m storage.Message) {
	var msg channels.InboundMessage
	if err := json.Unmarshal(m.Payload, &msg); err != nil {
		// Poison message (can never be unmarshaled): Ack and drop it so it
		// cannot block the queue with endless redeliveries.
		plog.Errorf("worker %s drop poison message %s: %v", w.Name, m.ID, err)
		_ = w.Stream.Ack(ctx, w.inStream(), "workers", m.ID)
		return
	}

	// Continue the trace across the async Stream boundary.
	ctx = metrics.ExtractTraceparent(ctx, propagation.MapCarrier{"traceparent": msg.TraceParent})
	ctx, span := tracer.Start(ctx, "worker.process")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("session_key", msg.SessionKey),
	)
	started := time.Now()

	// Execution-layer idempotency: a redelivered message whose reply already
	// made it outbound is acked without reprocessing — the LLM
	// must not run twice and the journal must not get duplicate events.
	if w.Processed != nil {
		done, err := w.Processed.IsDone(ctx, msg.Channel, msg.BindingID, msg.MsgID)
		if err != nil {
			plog.Warnf("worker %s done check %s: %v", w.Name, m.ID, err)
			// Fall through: better to reprocess than to wedge on a Redis hiccup.
		} else if done {
			plog.Infof("worker %s skip already-processed msg %s", w.Name, msg.MsgID)
			_ = w.Stream.Ack(ctx, w.inStream(), "workers", m.ID)
			return
		}
	}

	// Session lock: serialize concurrent processing of the same session
	// across replicas. The run is bound to the lease — the watchdog cancels it
	// if the lock is taken over, because from that moment a peer may be
	// generating a reply for the same message.
	// Deferred calls run LIFO: the watchdog stops first, then the lock is
	// released, then the run context is dropped.
	runCtx := ctx
	if w.Lock != nil {
		var cancelRun context.CancelCauseFunc
		runCtx, cancelRun = context.WithCancelCause(ctx)
		owner, stopWatchdog, ok := w.acquireSession(ctx, m, msg.AppID, msg.SessionKey, cancelRun)
		if !ok {
			cancelRun(nil)
			return // left pending for the reaper, or shutting down
		}
		defer cancelRun(nil)
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := w.Lock.Release(releaseCtx, msg.AppID, msg.SessionKey, owner); err != nil {
				plog.Warnf("worker %s release lock %s: %v", w.Name, msg.SessionKey, err)
			}
		}()
		defer stopWatchdog()
	}

	out, err := w.Processor.Process(runCtx, msg)
	metrics.ProcessDuration.Record(ctx, float64(time.Since(started).Milliseconds()), processAttr(msg))
	// A run that outlived its lease is not publishable: the peer that took the
	// session over is producing a reply for the same message, so leave the entry
	// pending instead of racing it. This also catches a run that finished a hair
	// before the cancellation reached it.
	lockLost := errors.Is(context.Cause(runCtx), errSessionLockLost)
	if err == nil && lockLost {
		err = errSessionLockLost
	}
	if err != nil {
		// No Ack: leave it pending for redelivery. Redelivery of a processed
		// message is caught by the done marker above; events journaled twice
		// inside the crash window remain bounded by the (session_id,
		// event_seq) unique constraint.
		if !lockLost {
			// A lease handover is not this message's failure: the peer that took
			// the session over will process the same entry, so counting it would
			// dead-letter a message another replica is about to deliver.
			w.countFailure(ctx, m.ID)
		}
		metrics.ProcessErrorTotal.Add(ctx, 1, processAttr(msg))
		if lockLost {
			plog.Warnf("worker %s aborts %s: session %s was taken over mid-run, leaving it pending",
				w.Name, m.ID, msg.SessionKey)
		} else {
			plog.Errorf("worker %s process %s failed: %v", w.Name, m.ID, err)
		}
		span.RecordError(err)
		return
	}

	// An empty reply means "handled, nothing to send" (recall events): mark
	// done, ack, no outbound hop.
	if out.Text == "" {
		if w.Processed != nil {
			if err := w.Processed.MarkDone(ctx, msg.Channel, msg.BindingID, msg.MsgID); err != nil {
				plog.Warnf("worker %s done mark %s: %v", w.Name, m.ID, err)
			}
		}
		if err := w.Stream.Ack(ctx, w.inStream(), "workers", m.ID); err != nil {
			plog.Warnf("worker %s ack %s: %v", w.Name, m.ID, err)
		}
		return
	}

	// Carry the worker span context into the outbound message so the send
	// span joins this trace as a child of worker.process.
	carrier := propagation.MapCarrier{}
	metrics.InjectTraceparent(ctx, carrier)
	out.TraceParent = carrier.Get("traceparent")

	//nolint:gosec // G117: SessionKey is a routing key on the internal stream, not a credential
	payload, err := json.Marshal(out)
	if err != nil {
		// Deterministic: no retry can ever marshal this reply, so it counts
		// toward the dead-letter bound instead of looping forever.
		w.countFailure(ctx, m.ID)
		plog.Errorf("worker %s marshal outbound: %v", w.Name, err)
		return
	}
	if _, err := w.Stream.Add(ctx, w.outStream(), payload); err != nil {
		// Not counted: Redis being down is not this message's failure, and
		// dead-lettering during an outage would discard deliverable replies.
		plog.Errorf("worker %s enqueue outbound: %v", w.Name, err)
		return
	}

	// Mark done before the Ack: a redelivery after a lost Ack must skip
	// reprocessing (the reply is already queued; the sent: key covers the
	// sender side).
	if w.Processed != nil {
		if err := w.Processed.MarkDone(ctx, msg.Channel, msg.BindingID, msg.MsgID); err != nil {
			plog.Warnf("worker %s done mark %s: %v", w.Name, m.ID, err)
		}
	}
	if err := w.Stream.Ack(ctx, w.inStream(), "workers", m.ID); err != nil {
		plog.Warnf("worker %s ack %s: %v", w.Name, m.ID, err)
	}
	zap.L().Debug("message processed",
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID))
}

// reap takes over pending messages idle longer than maxIdle (their consumers
// crashed) and reprocesses them. A message that keeps failing past
// maxAttempts is dead-lettered so it cannot loop forever.
func (w *Worker) reap(ctx context.Context) {
	if err := w.Stream.EnsureGroup(ctx, w.inStream(), "workers"); err != nil {
		plog.Warnf("worker %s ensure group before reap: %v", w.Name, err)
		return
	}
	msgs, err := w.Stream.AutoClaim(ctx, w.inStream(), "workers", w.Name, w.maxIdle(), 50)
	if err != nil {
		plog.Warnf("worker %s autoclaim: %v", w.Name, err)
		return
	}
	for _, m := range msgs {
		// Read-only: the counter tracks genuine processing failures (recorded
		// by handle), not takeovers — a message waiting on a busy session or
		// a Redis hiccup must not count toward the dead-letter bound.
		attempts, err := w.Stream.Attempts(ctx, w.inStream(), "workers", m.ID)
		if err != nil {
			plog.Warnf("worker %s count attempts %s: %v", w.Name, m.ID, err)
			continue
		}
		if attempts > w.maxAttempts() {
			plog.Errorf("worker %s dead-letters %s after %d attempts", w.Name, m.ID, attempts)
			if err := w.Stream.DeadLetter(ctx, w.inStream(), "workers", m); err != nil {
				plog.Errorf("worker %s deadletter %s: %v", w.Name, m.ID, err)
			}
			continue
		}
		plog.Infof("worker %s takes over %s (attempt %d)", w.Name, m.ID, attempts)
		w.handle(ctx, m)
	}
}

// countFailure records one genuine processing failure against the workers
// group's counter, which is what lets the reaper dead-letter a message that
// always fails. Waiting on a busy session, a lease handover or a Redis outage
// are not failures and must never come through here.
func (w *Worker) countFailure(ctx context.Context, id string) {
	if _, err := w.Stream.IncAttempts(ctx, w.inStream(), "workers", id); err != nil {
		plog.Warnf("worker %s count failure %s: %v", w.Name, id, err)
	}
}

// acquireSession spins for the session lock until lockWait. On timeout the
// message is left pending and the reaper takes it over after maxIdle, so a
// busy session delays the message without failing it. The returned stop ends
// the renewal watchdog; cancelRun is what the watchdog fires to abort the run
// if the lease is lost underneath it.
func (w *Worker) acquireSession(ctx context.Context, m storage.Message, appID, sessionKey string, cancelRun context.CancelCauseFunc) (owner string, stop func(), ok bool) {
	owner = w.Name + ":" + m.ID
	deadline := time.Now().Add(w.lockWait())
	for {
		acquired, err := w.Lock.TryAcquire(ctx, appID, sessionKey, owner, w.lockTTL())
		if err != nil {
			plog.Warnf("worker %s acquire lock %s: %v", w.Name, sessionKey, err)
		}
		if acquired {
			return owner, w.startLockWatchdog(ctx, appID, sessionKey, owner, cancelRun), true
		}
		if time.Now().After(deadline) {
			// Leave the entry pending for the reaper instead of re-queueing a
			// copy: a copy would carry a fresh attempts counter and could
			// re-queue forever without ever reaching the dead-letter bound.
			plog.Infof("worker %s leaves %s pending: session %s is busy, reaper takes over",
				w.Name, m.ID, sessionKey)
			return "", nil, false
		}
		select {
		case <-ctx.Done():
			return "", nil, false
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// startLockWatchdog renews the session lock every TTL/3 so long tool calls
// and slow generations cannot outlive the lease. A Redis hiccup is retried on
// the next tick (the TTL has slack for one or two misses); losing the lock
// itself (another owner) stops the renewal AND cancels the run — the lease is
// the only thing keeping a peer from processing the same message, so the
// generation in flight is no longer ours to finish.
func (w *Worker) startLockWatchdog(ctx context.Context, appID, sessionKey, owner string, cancelRun context.CancelCauseFunc) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(w.lockTTL() / 3)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := w.Lock.Extend(ctx, appID, sessionKey, owner, w.lockTTL())
				switch {
				case err != nil:
					// Transient Redis failure: keep renewing — the lease has
					// slack for a missed tick, and stopping here would drop
					// the lock at TTL expiry while processing continues.
					plog.Warnf("worker %s lock renew %s failed (retrying): %v", w.Name, sessionKey, err)
				case !ok:
					plog.Warnf("worker %s lost session lock %s, aborting the run", w.Name, sessionKey)
					cancelRun(errSessionLockLost)
					return
				}
			}
		}
	}()
	return func() { close(done) }
}
