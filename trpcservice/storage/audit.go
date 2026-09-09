package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

// AuditEvent is one row of the audit_log table.
type AuditEvent struct {
	TenantID         string // tenant UUID; zero UUID when the caller has none
	Channel          string
	UserID           string
	SessionID        string // session UUID; empty means NULL (session_key is not a UUID)
	AgentName        string
	ToolName         string
	Decision         string // allow / deny / review / review_timeout
	LatencyMs        int
	ErrorType        string
	Cost             float64
	PromptTokens     int
	CompletionTokens int
	TraceID          string
	// Detail carries the before/after payload of admin write operations
	// ({"before": ..., "after": ...}, change audit); empty means NULL.
	Detail    json.RawMessage
	CreatedAt time.Time
}

const (
	auditBatchSize      = 100             // flush the buffer at this many events
	auditFlushInterval  = time.Second     // or this often, whichever comes first
	auditSyncTimeout    = 3 * time.Second // deny/review writes get this much slack
	auditFlushTimeout   = 5 * time.Second // per flush attempt
	auditFlushTries     = 3               // a dropped batch is a lost audit trail
	auditFlushBackoff   = 200 * time.Millisecond
	auditRecoveryBudget = 5 * time.Second // whole-batch per-row recovery budget

	// auditQueueSize is the buffer depth and auditEnqueueWait how long a
	// producer waits for room before its event is dropped: a brief burst is
	// absorbed, while a sustained overload cannot stall the request path.
	auditQueueSize   = 10000
	auditEnqueueWait = 100 * time.Millisecond
)

// Auditor writes audit events to PG in two lanes: routine allow events are
// buffered and flushed asynchronously in batches (off the request path);
// critical decisions (deny / review) are written synchronously — they must
// not be lost, even at the cost of milliseconds of latency.
type Auditor struct {
	pool   *pgxpool.Pool
	ctx    context.Context // cancelled by Close; built once in NewAuditor, so it needs no lock
	cancel context.CancelFunc
	ch     chan AuditEvent
	done   chan struct{}

	dropped atomic.Uint64 // events lost to a sustained queue overload

	// lifecycle hands the flush loop to whichever of Start and Close gets
	// there first, and to nobody afterwards. Both are safe from any
	// goroutine: cancel is written once at construction and done is closed
	// by exactly one of the two.
	lifecycle sync.Once
}

// Dropped returns how many events were lost to a sustained queue overload
// (compliance: a non-zero value means the audit trail has holes).
func (a *Auditor) Dropped() uint64 { return a.dropped.Load() }

// markDropped records lost events on the in-process counter and the
// audit_dropped_total metric: operations discovers a hole in the compliance
// trail from the dashboard, not from grepping logs.
func (a *Auditor) markDropped(n uint64) {
	//nolint:gosec // G115: the counter accumulates in-memory drops, far below int64 range
	metrics.AuditDroppedTotal.Add(context.Background(), int64(n))
	a.dropped.Add(n)
}

// NewAuditor creates an Auditor on an established pool.
func NewAuditor(pool *pgxpool.Pool) *Auditor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Auditor{
		pool:   pool,
		ctx:    ctx,
		cancel: cancel,
		ch:     make(chan AuditEvent, auditQueueSize),
		done:   make(chan struct{}),
	}
}

// Start runs the background flush loop until Close. It is idempotent, and a
// Start after Close does nothing: a second loop would take events off the same
// queue from under the first, outlive the Close that was supposed to stop it,
// and after a restart cycle close a.done a second time — panicking in the one
// process that owns the audit trail.
func (a *Auditor) Start() {
	a.lifecycle.Do(func() { go a.loop() })
}

// Close stops the flush loop and waits for it to drain what is still queued.
// It is safe to call before Start (or twice).
func (a *Auditor) Close() {
	a.cancel()
	// Never started, so the loop that would close done does not exist: close
	// it here. Taking the Once is also what keeps a later Start from
	// resurrecting a loop nobody is left to stop.
	a.lifecycle.Do(func() { close(a.done) })
	<-a.done
}

// loop batches the queue into audit_log until the context is cancelled.
func (a *Auditor) loop() {
	defer close(a.done)
	ticker := time.NewTicker(auditFlushInterval)
	defer ticker.Stop()
	buf := make([]AuditEvent, 0, auditBatchSize)
	for {
		select {
		case ev := <-a.ch:
			buf = append(buf, ev)
			if len(buf) >= auditBatchSize {
				a.flush(buf)
				buf = buf[:0]
			}
		case <-ticker.C:
			if len(buf) > 0 {
				a.flush(buf)
				buf = buf[:0]
			}
		case <-a.ctx.Done():
			// Graceful shutdown: drain whatever is still queued before
			// exiting. Stopping on the buffer alone would lose the burst
			// that arrived right before Close.
			for {
				select {
				case ev := <-a.ch:
					buf = append(buf, ev)
					if len(buf) >= auditBatchSize {
						a.flush(buf)
						buf = buf[:0]
					}
				default:
					a.flush(buf)
					return
				}
			}
		}
	}
}

// LogAsync buffers a routine event.
//
// A producer waits up to auditEnqueueWait for room, so a short burst is
// absorbed instead of dropped; only a sustained overload drops the event, and
// that drop is counted and logged at error level — a silent loss would leave
// holes in the audit trail without anyone noticing.
func (a *Auditor) LogAsync(ev AuditEvent) {
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now()
	}
	timer := time.NewTimer(auditEnqueueWait)
	defer timer.Stop()
	select {
	case a.ch <- ev:
	case <-timer.C:
		a.markDropped(1)
		plog.Errorf("audit queue full, dropped %s event (trace=%s, dropped_total=%d)",
			ev.Decision, ev.TraceID, a.dropped.Load())
	}
}

// LogSync writes a critical decision (deny / review) immediately.
func (a *Auditor) LogSync(ctx context.Context, ev AuditEvent) error {
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now()
	}
	ctx, cancel := context.WithTimeout(ctx, auditSyncTimeout)
	defer cancel()
	return a.insert(ctx, []AuditEvent{ev})
}

// flush writes one batch, retrying with a short backoff: the buffer is the
// only copy of these events, so giving up on the first error would drop a
// chunk of the audit trail. After auditFlushTries the batch falls back to
// per-row recovery instead of being dropped wholesale: a batch fails
// atomically (the statements between two pipeline syncs are implicitly
// transactional), so one poisoned event — a shape the schema rejects, e.g. a
// tenant id that is not a uuid — fails deterministically on every retry and
// would otherwise bury every valid event sharing its batch.
func (a *Auditor) flush(buf []AuditEvent) {
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), auditFlushTimeout)
		err := a.insert(ctx, buf)
		cancel()
		if err == nil {
			return
		}
		if attempt >= auditFlushTries {
			plog.Errorf("audit batch flush (%d events) failed after %d attempts, recovering per row: %v",
				len(buf), attempt, err)
			a.recoverPerRow(buf)
			return
		}
		plog.Warnf("audit batch flush (%d events) attempt %d/%d: %v",
			len(buf), attempt, auditFlushTries, err)
		time.Sleep(time.Duration(attempt) * auditFlushBackoff)
	}
}

// recoverPerRow writes the batch one event at a time, so a schema-rejected
// row costs itself alone. Rows that still fail — the poison and, while the
// database is down, everything behind the recovery budget — are dropped
// loudly and counted, keyed by trace id so the hole in the trail is
// identifiable.
func (a *Auditor) recoverPerRow(events []AuditEvent) {
	// One budget for the whole recovery: a database outage must not turn a
	// 100-row batch into 100 sequential timeouts riding the flush loop.
	deadline := time.Now().Add(auditRecoveryBudget)
	for _, ev := range events {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		err := a.insert(ctx, []AuditEvent{ev})
		cancel()
		if err == nil {
			continue
		}
		a.markDropped(1)
		plog.Errorf("audit event dropped (trace=%s, decision=%s): %v", ev.TraceID, ev.Decision, err)
	}
}

// insert writes the whole slice as one pgx batch: one round trip for the
// batch instead of one Exec per row.
func (a *Auditor) insert(ctx context.Context, events []AuditEvent) error {
	if len(events) == 0 {
		return nil
	}
	const q = `INSERT INTO audit_log
		(tenant_id, channel, user_id, session_id, agent_name, tool_name,
		 decision, latency_ms, error_type, cost, prompt_tokens, completion_tokens,
		 trace_id, detail, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`
	batch := &pgx.Batch{}
	for _, ev := range events {
		var sessionID *string
		if ev.SessionID != "" {
			sessionID = &ev.SessionID
		}
		var detail []byte
		if len(ev.Detail) > 0 {
			detail = ev.Detail
		}
		batch.Queue(q,
			ev.TenantID, ev.Channel, ev.UserID, sessionID, ev.AgentName, ev.ToolName,
			ev.Decision, ev.LatencyMs, ev.ErrorType, ev.Cost, ev.PromptTokens,
			ev.CompletionTokens, ev.TraceID, detail, ev.CreatedAt,
		)
	}
	results := a.pool.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()
	for range events {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("insert audit_log: %w", err)
		}
	}
	return nil
}
